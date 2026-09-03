"""总线网关 —— 把总线延伸出进程边界(远程域:子服务 = 域的异地实现)。

设计地位:域的契约(header/schema,仍只在 src 声明一份)与实现所在进程解耦。
远程进程经 WebSocket 连上网关、声明自己是哪个域,之后:
  · 该域的订阅投递顺着连接送达远端,远端 ack 才销账;
  · 远端的发布经该域 Window 注入总线——发布权/schema 校验与本地完全一致;
  · 远端心跳就是普通的 publish(心跳公约 topic),哨兵零改动覆盖。

协议(JSON 文本帧):
  C→S  {"t":"hello","domain":d,"token":t}
  S→C  {"t":"hello_ok"} | {"t":"error","msg":…}(随后断开)
  C→S  {"t":"publish","seq":n,"topic":…,"payload":{…},"corr":…?}
  C→S  {"t":"log","record":{…}}  远程域结构化日志中继(单向遥测,无 ack)→ 主进程 reinject
  S→C  {"t":"pub_ok","seq":n,"msg_id":…} | {"t":"pub_err","seq":n,"msg":…}
  S→C  {"t":"deliver","did":n,"slot":…,"topic":…,"payload":{…},"msg_id":…,"corr":…}
  C→S  {"t":"ack","did":n} | {"t":"nack","did":n,"err":…}

投递语义(与本地完全同源):
  · ack → 销账;nack / ack 超时 → 现有重试→死信机制;
  · 断线 → 订阅暂停(消息在队列/账上等待),在途投递重连后原样重发
    ——不烧重试次数;远端可能"做完了但 ack 丢了" → at-least-once,
    处理器幂等军规覆盖(msg_id 恒定作去重键)。
安全:绑定地址与 token 来自 config.toml [gateway];每域同时至多一条连接。
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import websockets

from .. import logger
from .. import Bus, Window
from ..registry import validate_header

Handler = Any


class _Disconnected(Exception):
    """连接在等待 ack 期间断开(重连后重发,不属于处理失败)。"""


class _RemoteDomain:
    def __init__(self) -> None:
        self.conn = None
        self.connected = asyncio.Event()
        self.pending: dict[int, asyncio.Future] = {}
        self.inflight = 0                 # 在飞投递数(懒进程域闲时回收判据)
        self.last_active = 0.0            # 最近一次投递完成时刻(monotonic)


class GatewayServer:
    """网关本体:远程域绑定 + WS 服务。组装顺序:
    remote_handlers()(mount 前)→ attach(bus, windows)→ start()。"""

    def __init__(self, *, host: str = "127.0.0.1", port: int = 8790,
                 token: str = "", ack_timeout: float = 600.0) -> None:
        self._host, self._port = host, port
        self._token = token
        self._ack_timeout = ack_timeout
        self._remotes: dict[str, _RemoteDomain] = {}
        self._starters: dict[str, dict] = {}   # 懒进程域:{domain: {ensure, stop, idle_sec}}
        self._bus: Bus | None = None
        self._windows: dict[str, Window] = {}
        self._server = None
        self._seq = 0
        self._idle_task: asyncio.Task | None = None
        self.port: int | None = None      # start() 后为实际监听口(支持 port=0 随机)

    # ── 懒进程域:按需拉起 + 闲时回收(对需求方透明)────────────────────────────

    def register_lazy(self, domain: str, *, ensure, stop, idle_sec: float = 30.0) -> None:
        """把某(远程)域登记为**懒进程域**:投递到它而它未连接时,网关自动 `ensure`
        拉起子进程(再等首连);连接着且无在飞投递空闲超过 idle_sec 时,自动 `stop` 回收。
        对需求方完全透明(只管发消息)。ensure/stop 幂等(接监督器 ensure_child/stop_child)。"""
        self._remotes.setdefault(domain, _RemoteDomain())
        self._starters[domain] = {"ensure": ensure, "stop": stop, "idle_sec": float(idle_sec)}

    # ── 组装期:为远程域生成转发处理器(交给 bus.mount 绑定)────────────────────

    def remote_handlers(self, header) -> dict[str, Handler]:
        """把某域声明为远程:返回该域全部订阅槽位的转发处理器。"""
        domain, _pub, subscribes, _eph, _vol = validate_header(header)
        rd = self._remotes.setdefault(domain, _RemoteDomain())
        return {slot: self._make_handler(rd, domain, slot)
                for slot in set(subscribes.values())}

    def _make_handler(self, rd: _RemoteDomain, domain: str, slot: str) -> Handler:
        async def forward(env) -> None:
            while True:                                   # 断线重发环(不烧重试次数)
                # VOLATILE 在途断线即放弃(不重发):易失命令 at-most-once,收件进程
                # 死在投递中途=交接作废。首连等待不属放弃范围——远端刚被懒拉起、尚未
                # 连上网关时首次投递照常等连接(冷启动窗口,仍是首投)。跨窗口陈旧
                # 命令由收端执行前状态校验兜底(task 读 jobs 非 running 即弃)。
                volatile = self._bus is not None and env.topic in self._bus.volatile_topics
                # 懒进程域:未连接则按需拉起(幂等),再等首连——需求方无感
                st = self._starters.get(domain)
                if st is not None and not rd.connected.is_set():
                    try:
                        await st["ensure"]()
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("懒进程域 %s 拉起失败:%s", domain, exc,
                                       layer="CORE", component="Gateway")
                await rd.connected.wait()
                self._seq += 1
                did = self._seq
                fut: asyncio.Future = asyncio.get_running_loop().create_future()
                rd.pending[did] = fut
                rd.inflight += 1
                try:
                    await rd.conn.send(json.dumps({
                        "t": "deliver", "did": did, "slot": slot, "topic": env.topic,
                        "payload": env.payload.model_dump(), "msg_id": env.msg_id,
                        "corr": env.correlation_id}, ensure_ascii=False))
                    result = await asyncio.wait_for(fut, self._ack_timeout)
                except (_Disconnected, websockets.ConnectionClosed):
                    if volatile:
                        raise RuntimeError(
                            f"远端 {domain} 断线,易失投递放弃 topic={env.topic}")
                    continue                              # 持久投递:重连后原样重发
                except asyncio.TimeoutError:
                    raise RuntimeError(
                        f"远端 {domain} ack 超时({self._ack_timeout:.0f}s)slot={slot}")
                finally:
                    rd.pending.pop(did, None)
                    rd.inflight -= 1
                    rd.last_active = time.monotonic()
                if result is not True:                    # nack → 处理失败
                    raise RuntimeError(f"远端 {domain} nack:{result}")
                return
        return forward

    # ── 生命周期 ──────────────────────────────────────────────────────────────

    def attach(self, bus: Bus, windows: dict[str, Window]) -> None:
        """挂接总线与远程域的发布窗口(远端 publish 经窗口注入)。"""
        self._bus = bus
        self._windows = {d: windows[d] for d in self._remotes if d in windows}

    async def start(self) -> None:
        self._server = await websockets.serve(self._on_conn, self._host, self._port)
        self.port = self._server.sockets[0].getsockname()[1]
        if self._starters:
            self._idle_task = asyncio.create_task(self._idle_reaper(), name="gateway_idle_reaper")
        logger.info("总线网关监听 ws://%s:%d(远程域:%s;懒进程域:%s)", self._host, self.port,
                    ",".join(sorted(self._remotes)) or "无",
                    ",".join(sorted(self._starters)) or "无",
                    layer="CORE", component="Gateway")

    async def _idle_reaper(self) -> None:
        """闲时回收:懒进程域连接着、无在飞投递、空闲超过 idle_sec → stop_child 回收子进程。"""
        while True:
            await asyncio.sleep(5.0)
            now = time.monotonic()
            for domain, st in list(self._starters.items()):
                rd = self._remotes.get(domain)
                if rd is None or not rd.connected.is_set() or rd.inflight > 0:
                    continue
                if rd.last_active and now - rd.last_active >= st["idle_sec"]:
                    try:
                        await st["stop"]()
                        logger.info("懒进程域 %s 闲置 %.0fs,回收子进程", domain, st["idle_sec"],
                                    layer="CORE", component="Gateway")
                        rd.last_active = 0.0   # 停过就不再重复 stop,直到下次投递刷新
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("懒进程域 %s 回收失败:%s", domain, exc,
                                       layer="CORE", component="Gateway")

    async def stop(self) -> None:
        if self._idle_task is not None:
            self._idle_task.cancel()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        for rd in self._remotes.values():
            if rd.conn is not None:
                await rd.conn.close()

    # ── 连接处理 ──────────────────────────────────────────────────────────────

    async def _on_conn(self, ws) -> None:
        domain = ""
        rd: _RemoteDomain | None = None
        try:
            hello = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
            domain = str(hello.get("domain", ""))
            if hello.get("t") != "hello" or (self._token and hello.get("token") != self._token):
                await ws.send(json.dumps({"t": "error", "msg": "鉴权失败"})); return
            rd = self._remotes.get(domain)
            if rd is None:
                await ws.send(json.dumps(
                    {"t": "error", "msg": f"'{domain}' 未声明为远程域"})); return
            if rd.conn is not None:
                await ws.send(json.dumps(
                    {"t": "error", "msg": f"'{domain}' 已有活动连接"})); rd = None; return
            rd.conn = ws
            rd.connected.set()
            await ws.send(json.dumps({"t": "hello_ok"}))
            logger.info("远程域上线:%s", domain, layer="CORE", component="Gateway")

            async for raw in ws:
                m = json.loads(raw)
                t = m.get("t")
                # 活性信号 = 除 log 外的一切上行(ack/nack/publish/busy)。**日志不计活性**
                # (纯观测,liveness 不该搭日志);正在跑的进程域靠传输层 busy 帧 + 业务 publish
                # 续命,不被 idle_reaper 误杀。见 docs boot.md 形态 B。
                if t != "log":
                    rd.last_active = time.monotonic()
                if t == "busy":                        # 传输层保活帧:仅刷 last_active(上面已做)
                    continue
                if t == "ack":
                    fut = rd.pending.get(int(m["did"]))
                    if fut is not None and not fut.done():
                        fut.set_result(True)
                elif t == "nack":
                    fut = rd.pending.get(int(m["did"]))
                    if fut is not None and not fut.done():
                        fut.set_result(str(m.get("err", "远端处理失败")))
                elif t == "publish":
                    await self._remote_publish(ws, domain, m)
                elif t == "log":
                    # 远程域结构化日志中继:字段保真重注入本进程 logger(照常进
                    # 控制台/hub_logs/SSE);比 supervisor 文本泵强(保留 phase/临时进度)。
                    logger.reinject(m.get("record") or {})
        except (websockets.ConnectionClosed, asyncio.TimeoutError, json.JSONDecodeError):
            pass
        finally:
            if rd is not None:
                rd.conn = None
                rd.connected.clear()
                for fut in list(rd.pending.values()):     # 在途投递:重连后重发
                    if not fut.done():
                        fut.set_exception(_Disconnected())
                logger.warning("远程域离线:%s(订阅暂停,消息在账等待重连)", domain,
                               layer="CORE", component="Gateway")

    async def _remote_publish(self, ws, domain: str, m: dict) -> None:
        seq = m.get("seq")
        try:
            win = self._windows.get(domain)
            if win is None or self._bus is None:
                raise RuntimeError(f"'{domain}' 无发布窗口")
            topic = str(m["topic"])
            schema = self._bus.topic_schema(topic)
            if schema is None:
                raise RuntimeError(f"未声明的 topic '{topic}'")
            payload = schema.model_validate(m.get("payload") or {})
            msg_id = await win.publish(topic, payload,
                                       correlation_id=m.get("corr") or None)
            await ws.send(json.dumps({"t": "pub_ok", "seq": seq, "msg_id": msg_id}))
        except Exception as exc:  # noqa: BLE001 — 远端的错误远端自己收
            await ws.send(json.dumps(
                {"t": "pub_err", "seq": seq, "msg": str(exc)}, ensure_ascii=False))
