"""总线客户端 —— 子服务侧的总线公民资格(Python 版;协议语言无关,见 gateway.py)。

用法(在子服务进程里):
    client = BusClient("ws://127.0.0.1:8790", domain="publishers", token="…",
                       handlers={"on_publish_requested": my_handler})
    await client.start()                       # 后台连接,断线自动重连
    await client.publish("publishers/publish/finished", {"job_id": …, "ok": True})

处理器签名:``async def handler(payload: dict, meta: dict) -> None``
  meta = {"topic","msg_id","corr"};正常返回 → ack(销账);抛异常 → nack(重试→死信)。
心跳:子服务自己周期 publish 心跳公约 topic 即可,哨兵自动监护。
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Awaitable, Callable

import websockets

from ... import logger

RemoteHandler = Callable[[dict, dict], Awaitable[None]]


class GatewayError(Exception):
    """网关拒绝(鉴权失败/域未声明/发布被拒)。"""


class BusClient:
    def __init__(self, url: str, domain: str, *, token: str = "",
                 handlers: dict[str, RemoteHandler] | None = None,
                 reconnect_min: float = 0.5, reconnect_max: float = 10.0) -> None:
        self._url = url
        self._domain = domain
        self._token = token
        self._handlers = handlers or {}
        self._rc_min, self._rc_max = reconnect_min, reconnect_max
        self._ws = None
        self._ready = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._seq = 0
        self._pub_waiters: dict[int, asyncio.Future] = {}
        self._fatal: str | None = None
        self._log_relay: asyncio.Task | None = None
        self._log_stop = asyncio.Event()

    # ── 生命周期 ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """后台连接(断线指数退避重连);等首次就绪或首个致命错误。"""
        self._task = asyncio.create_task(self._run(), name=f"busclient:{self._domain}")
        # 结构化日志中继:订阅本进程全量日志 → 逐条经 ws 送主进程 reinject(断连时丢弃)。
        # 走网关即自动获得——远程域无需任何业务代码;字段保真(含 phase / 临时进度)。
        self._log_relay = asyncio.create_task(
            logger.relay(self._send_log, stop=self._log_stop),
            name=f"logrelay:{self._domain}")
        while not self._ready.is_set() and self._fatal is None:
            await asyncio.sleep(0.02)
        if self._fatal:
            raise GatewayError(self._fatal)

    async def _send_log(self, record: dict) -> None:
        """把一条结构化日志 record 作 log 帧送主进程(未连即丢弃——日志是有损遥测,
        绝不阻塞/反噬业务)。单向、无 ack。"""
        ws = self._ws
        if ws is None:
            return
        try:
            await ws.send(json.dumps({"t": "log", "record": record}, ensure_ascii=False))
        except Exception:  # noqa: BLE001 — 断线/背压:丢弃这条,不影响业务
            pass

    async def close(self) -> None:
        self._log_stop.set()
        if self._log_relay is not None:
            self._log_relay.cancel()
            await asyncio.gather(self._log_relay, return_exceptions=True)
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        if self._ws is not None:
            await self._ws.close()

    async def _run(self) -> None:
        backoff = self._rc_min
        while True:
            try:
                async with websockets.connect(self._url) as ws:
                    await ws.send(json.dumps(
                        {"t": "hello", "domain": self._domain, "token": self._token}))
                    resp = json.loads(await ws.recv())
                    if resp.get("t") != "hello_ok":
                        self._fatal = str(resp.get("msg", "网关拒绝"))
                        return                              # 致命:配置错误,不重连
                    self._ws = ws
                    self._ready.set()
                    backoff = self._rc_min
                    async for raw in ws:
                        self._dispatch(json.loads(raw))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — 网络故障:退避重连
                pass
            self._ready.clear()
            self._ws = None
            for fut in list(self._pub_waiters.values()):
                if not fut.done():
                    fut.set_exception(GatewayError("连接断开"))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self._rc_max)

    # ── 收帧分发 ──────────────────────────────────────────────────────────────

    def _dispatch(self, m: dict) -> None:
        t = m.get("t")
        if t == "deliver":
            asyncio.get_running_loop().create_task(self._handle(m))
        elif t in ("pub_ok", "pub_err"):
            fut = self._pub_waiters.pop(int(m.get("seq", 0)), None)
            if fut is not None and not fut.done():
                if t == "pub_ok":
                    fut.set_result(m.get("msg_id"))
                else:
                    fut.set_exception(GatewayError(str(m.get("msg"))))

    async def _handle(self, m: dict) -> None:
        did = m["did"]
        handler = self._handlers.get(m.get("slot", ""))
        try:
            if handler is None:
                raise RuntimeError(f"本端未绑定槽位 '{m.get('slot')}'")
            await handler(m.get("payload") or {},
                          {"topic": m.get("topic"), "msg_id": m.get("msg_id"),
                           "corr": m.get("corr")})
            frame = {"t": "ack", "did": did}
        except Exception as exc:  # noqa: BLE001 — 处理失败 → nack(服务端重试→死信)
            frame = {"t": "nack", "did": did, "err": str(exc)[:500]}
        ws = self._ws
        if ws is not None:
            try:
                await ws.send(json.dumps(frame, ensure_ascii=False))
            except Exception:  # noqa: BLE001 — 断线:服务端会重发,无需处理
                pass

    # ── 发布 ─────────────────────────────────────────────────────────────────

    async def publish(self, topic: str, payload: dict | Any, *,
                      corr: str | None = None, timeout: float = 30.0) -> str:
        """经网关发布;返回 msg_id。校验(发布权/schema)在服务端,被拒即抛错。"""
        await asyncio.wait_for(self._ready.wait(), timeout)
        if hasattr(payload, "model_dump"):
            payload = payload.model_dump()
        self._seq += 1
        seq = self._seq
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pub_waiters[seq] = fut
        await self._ws.send(json.dumps(
            {"t": "publish", "seq": seq, "topic": topic, "payload": payload,
             "corr": corr}, ensure_ascii=False))
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            self._pub_waiters.pop(seq, None)
