"""进程内消息总线内核 —— 目录即路由(地基级投递语义)。

拓扑规则:
  · topic 形如 "域/子路径/事件",首段 = 拥有域(即目录名);发布权随目录走。
  · 订阅支持通配:"+" 匹配一段,"#" 匹配剩余子树(仅限末段)。
  · 各域唯一的收发窗口 = header.py 声明 + mount 时绑定的处理器;
    未声明的 topic、越权发布、载荷类型不符 —— 一律立即抛错,不静默。

投递语义(地基级,"发出去 = 一定会被处理到或显式进死信"):
  · 持久投递(默认):publish 返回前,消息 + 每个匹配订阅的投递账已落盘(journal);
    处理成功销账;处理失败退避重试,超限进死信(可查可重放);
    进程崩溃后重启,未销账投递自动重投 —— at-least-once,处理器必须幂等
    (env.msg_id 恒定,可作去重键)。
  · EPHEMERAL(头文件显式声明的纯遥测 topic,如进度心跳):跳过落盘走快路径。
  · 背压取代丢弃:队列满时发布方等待(超时抛 BusError,绝不无声丢弃)。
    持久消息即使入队超时也已在账,重启必达。
  · 每个订阅独立队列 + 消费任务:慢/错处理器不拖垮他人;publish 为 async,
    换 RabbitMQ 载体时窗口 API 与语义(持久+ACK+死信)一一对应。
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from types import ModuleType
from typing import Awaitable, Callable, Optional

from .. import logger
from .contracts import Message
from .journal import Journal
from .registry import validate_header

Handler = Callable[["Envelope"], Awaitable[None]]


# ── 因果链自动透传 ─────────────────────────────────────────────────────────────
# handler 内续发消息须携带触发消息的 correlation_id(处理器契约)。逐个服务方法穿
# 参数侵入太广且必被遗漏(架构审计:全部续发点都没传)——收进内核:投递前把当前
# 信封的 corr 放进任务上下文,publish 未显式给 corr 时自动取用。contextvars 按
# asyncio 任务隔离;handler 内 create_task 派发的后台任务同样继承(上下文拷贝)。
# 显式传参仍最高优先;worker 线程等脱离上下文的路径取不到则回退新建(原语义)。
import contextvars as _contextvars

_current_corr: _contextvars.ContextVar[str | None] = _contextvars.ContextVar(
    "bus_correlation_id", default=None)


class BusError(Exception):
    """总线使用错误(越权发布/未声明 topic/载荷类型不符/槽位缺失/入队超时)。"""


@dataclass(frozen=True)
class Envelope:
    """一次投递的完整上下文;correlation_id 沿因果链传递,用于全链路追踪。

    at-least-once 下重投的消息 msg_id 不变 —— 幂等去重键。
    """

    topic: str
    source: str
    payload: Message
    msg_id: str
    correlation_id: str
    published_at: float
    journal_id: int | None = None     # 持久消息的账本号(EPHEMERAL 为 None)


def topic_matches(pattern: str, topic: str) -> bool:
    """目录式匹配:'+' 匹配单段,'#' 匹配剩余子树。"""
    p, t = pattern.split("/"), topic.split("/")
    for i, seg in enumerate(p):
        if seg == "#":
            return True
        if i >= len(t) or (seg != "+" and seg != t[i]):
            return False
    return len(p) == len(t)


@dataclass
class _Subscription:
    domain: str
    pattern: str
    slot: str
    handler: Handler
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    task: asyncio.Task | None = None

    @property
    def key(self) -> str:
        return f"{self.domain}:{self.slot}"


class Window:
    """域的唯一发布窗口:mount 返回,发布受该域 header 声明约束。"""

    def __init__(self, bus_: "Bus", domain: str) -> None:
        self._bus = bus_
        self._domain = domain

    @property
    def domain(self) -> str:
        return self._domain

    async def publish(self, topic: str, payload: Message, *,
                      correlation_id: str | None = None) -> str:
        """发布消息,返回 msg_id。在处理器内续发时应透传 env.correlation_id。"""
        return await self._bus._publish(self._domain, topic, payload, correlation_id)


class Bus:
    """总线本体:声明表 + 订阅表 + 持久账本 + 每订阅独立消费任务。

    journal=None(如单元测试)时无持久层:全部 topic 走快路径,
    仍然背压不丢弃,但不抗进程崩溃 —— 生产组装必须传 journal。
    """

    def __init__(self, queue_size: int = 4096, recent_size: int = 200,
                 journal: Journal | None = None, max_attempts: int = 3,
                 put_timeout: float = 30.0) -> None:
        self._queue_size = queue_size
        self._journal = journal
        self._max_attempts = max_attempts
        self._put_timeout = put_timeout
        self._domains: set[str] = set()
        self._topics: dict[str, tuple[str, type[Message]]] = {}   # topic → (拥有域, schema)
        self._ephemeral: set[str] = set()   # 纯遥测:不落账本、不计投递
        self._volatile: set[str] = set()    # 易失命令:不落账本、at-most-once,但计投递
        self._subs: list[_Subscription] = []
        self._retry_tasks: set[asyncio.Task] = set()
        self._recover_boundary = 0
        self._started = False
        self.stats: dict[str, int] = {
            "published": 0, "published_ephemeral": 0, "delivered": 0, "handler_errors": 0,
            "retried": 0, "dead_letters": 0,
        }
        # ── 内省:声明存档 + 按 topic 计数 + 最近消息环形缓冲 + 观察者钩子 ──
        self._declared: dict[str, tuple[dict[str, type[Message]], dict[str, str]]] = {}
        self._topic_counts: dict[str, int] = {}
        self._recent: deque[dict] = deque(maxlen=recent_size)
        self._observers: list[Callable[[dict], None]] = []

    @property
    def journal(self) -> Journal | None:
        return self._journal

    def check_closure(self) -> list[str]:
        """契约闭环校验:每个持久 topic 必须有 ≥1 订阅匹配(EPHEMERAL 豁免)。
        返回悬空 topic 列表——"发布即蒸发"从运行时的沉默变成组装期的红灯。"""
        dangling = [t for t in self._topics
                    if t not in self._ephemeral
                    and not any(topic_matches(s.pattern, t) for s in self._subs)]
        return sorted(dangling)

    def topic_schema(self, topic: str) -> type[Message] | None:
        """某 topic 的载荷契约(网关把远端 dict 还原成 Message 用);未声明返回 None。"""
        entry = self._topics.get(topic)
        return entry[1] if entry else None

    # ── 挂载 ──────────────────────────────────────────────────────────────

    def mount(self, header: ModuleType, handlers: dict[str, Handler] | None = None) -> Window:
        """挂载一个域:校验头文件声明,绑定订阅槽位,返回该域的发布窗口。"""
        domain, publishes, subscribes, ephemeral, volatile = validate_header(header)
        if domain in self._domains:
            raise BusError(f"域 '{domain}' 已挂载")
        handlers = handlers or {}
        slots = set(subscribes.values())
        missing, extra = slots - handlers.keys(), handlers.keys() - slots
        if missing:
            raise BusError(f"{domain}: 订阅槽位缺少处理器 {sorted(missing)}")
        if extra:
            raise BusError(f"{domain}: 传入了未声明的处理器 {sorted(extra)}")

        self._domains.add(domain)
        self._declared[domain] = (publishes, subscribes)
        self._topics.update({t: (domain, s) for t, s in publishes.items()})
        self._ephemeral |= ephemeral
        self._volatile |= volatile
        for pattern, slot in subscribes.items():
            sub = _Subscription(domain=domain, pattern=pattern, slot=slot,
                                handler=handlers[slot],
                                queue=asyncio.Queue(maxsize=self._queue_size))
            self._subs.append(sub)
            if self._started:
                sub.task = asyncio.create_task(
                    self._consume(sub), name=f"bus:{domain}:{slot}")
        logger.info("总线挂载域 %s(发布 %d topic,订阅 %d)", domain,
                    len(publishes), len(subscribes), layer="CORE", component="Bus")
        return Window(self, domain)

    # ── 生命周期 ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        """启动全部消费任务(幂等);持久层就绪时先恢复未销账投递。"""
        if self._started:
            return
        self._started = True
        # 恢复水位线:只补投上一会话遗留(≤此线);本会话新消息走正常队列,
        # 否则 start 后立即发布的消息会被恢复流程与队列双路投递(竞态)
        self._recover_boundary = (await self._journal.max_id()
                                  if self._journal is not None else 0)
        for sub in self._subs:
            if sub.task is None:
                sub.task = asyncio.create_task(
                    self._consume(sub), name=f"bus:{sub.domain}:{sub.slot}")

    async def close(self) -> None:
        """取消全部消费与重试任务。持久消息的未竟投递留在账上,下次启动重投。"""
        tasks = [s.task for s in self._subs if s.task is not None]
        tasks += list(self._retry_tasks)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._started = False
        self._retry_tasks.clear()
        for sub in self._subs:
            sub.task = None

    # ── 投递 ──────────────────────────────────────────────────────────────

    async def _publish(self, source: str, topic: str, payload: Message,
                       correlation_id: str | None) -> str:
        owner_schema = self._topics.get(topic)
        if owner_schema is None:
            raise BusError(f"{source}: 发布未声明的 topic '{topic}'")
        owner, schema = owner_schema
        if owner != source:
            raise BusError(f"{source}: 越权发布 '{topic}'(拥有域为 {owner})")
        if not isinstance(payload, schema):
            raise BusError(
                f"{source}: '{topic}' 载荷类型 {type(payload).__name__} 不符,"
                f"契约要求 {schema.__name__}")

        matched = [s for s in self._subs if topic_matches(s.pattern, topic)]
        msg_id = uuid.uuid4().hex[:12]
        corr = correlation_id or _current_corr.get() or uuid.uuid4().hex[:12]
        jid: int | None = None
        transient = topic in self._ephemeral or topic in self._volatile
        if matched and self._journal is not None and not transient:
            # 持久:先落盘(消息 + 每订阅投递账,单事务),再入队 —— 崩溃必达。
            # EPHEMERAL/VOLATILE 均跳过账本(at-most-once,jid=None → 不重试)。
            jid = await self._journal.append(
                topic, source, payload.model_dump_json(), msg_id, corr,
                [s.key for s in matched])
        env = Envelope(topic=topic, source=source, payload=payload,
                       msg_id=msg_id, correlation_id=corr,
                       published_at=time.time(), journal_id=jid)

        self.stats["published"] += 1
        if topic in self._ephemeral:
            # 遥测分账:EPHEMERAL 是纯观测量纲,单列"已发布(遥测)"、且其签收不计"已投递"
            # (见 _handle)——高频遥测不污染工作投递账。VOLATILE 虽也跳账本,但属工作
            # 量纲(计入发布与投递),故不在此列。观测台:发布=published−ephemeral(持久+易失)。
            self.stats["published_ephemeral"] += 1
        self._topic_counts[topic] = self._topic_counts.get(topic, 0) + 1
        record = {"ts": env.published_at, "source": source, "topic": topic,
                  "msg_id": msg_id, "corr": corr, "payload": payload.model_dump()}
        self._recent.append(record)
        for fn in self._observers:
            try:
                fn(record)
            except Exception as exc:  # noqa: BLE001 — 观察者故障不影响投递
                logger.error("总线观察者异常 err=%s", exc, layer="CORE", component="Bus")

        for sub in matched:
            await self._enqueue(sub, env)
        logger.debug("bus %s → %s corr=%s%s", source, topic, corr,
                     f" journal#{jid}" if jid else "",
                     layer="CORE", component="Bus")
        return msg_id

    async def _enqueue(self, sub: _Subscription, env: Envelope) -> None:
        """背压入队:队列满则等待;超时抛显式错误(绝不无声丢弃)。
        持久消息即使超时也已落盘,重启后必重投。"""
        try:
            sub.queue.put_nowait(env)
        except asyncio.QueueFull:
            try:
                await asyncio.wait_for(sub.queue.put(env), timeout=self._put_timeout)
            except asyncio.TimeoutError:
                msg = (f"入队超时 {self._put_timeout}s:订阅 {sub.key} 队列持续满载"
                       f"(topic={env.topic}"
                       + (f",已落盘 journal#{env.journal_id},重启后重投)" if env.journal_id
                          else ",EPHEMERAL 消息将丢失)"))
                logger.error("总线背压超时:%s", msg, layer="CORE", component="Bus")
                raise BusError(msg) from None

    # ── 消费(含崩溃恢复与重试)────────────────────────────────────────────

    async def _consume(self, sub: _Subscription) -> None:
        if self._journal is not None:
            await self._recover(sub)
        while True:
            env = await sub.queue.get()
            await self._handle(sub, env)

    async def _recover(self, sub: _Subscription) -> None:
        """重启恢复:处理该订阅账上全部未销账投递(按消息序,先于新消息)。"""
        try:
            rows = await self._journal.pending_for(sub.key)   # type: ignore[union-attr]
        except Exception as exc:  # noqa: BLE001
            logger.error("总线恢复读账失败 sub=%s err=%s", sub.key, exc,
                         layer="CORE", component="Bus")
            return
        rows = [r for r in rows if r["journal_id"] <= self._recover_boundary]
        if not rows:
            return
        logger.info("总线恢复:%s 重投 %d 条未销账投递", sub.key, len(rows),
                    layer="CORE", component="Bus")
        for r in rows:
            owner_schema = self._topics.get(r["topic"])
            if owner_schema is None:   # 契约已删除该 topic → 显式死信,不静默
                await self._journal.bump_attempts(   # type: ignore[union-attr]
                    r["journal_id"], sub.key, "topic 已不在契约中", 1)
                self.stats["dead_letters"] += 1
                logger.error("总线恢复:topic '%s' 已无契约,journal#%d 转死信",
                             r["topic"], r["journal_id"], layer="CORE", component="Bus")
                continue
            payload = owner_schema[1].model_validate_json(r["payload"])
            env = Envelope(topic=r["topic"], source=r["source"], payload=payload,
                           msg_id=r["msg_id"], correlation_id=r["corr"],
                           published_at=time.time(), journal_id=r["journal_id"])
            await self._handle(sub, env)

    async def replay_dead(self, delivery_id: int) -> bool:
        """死信在线重放:账上重置为 pending 后立即入队重投(无需重启)。

        与 discard 互为死信的两个终点。返回是否命中(非死信/不存在 → False);
        topic 契约已删除或订阅已不在 → 回滚为死信,显式失败不静默。"""
        if self._journal is None:
            return False
        r = await self._journal.replay(delivery_id)
        if r is None:
            return False
        sub = next((s for s in self._subs if s.key == r["subscriber"]), None)
        owner_schema = self._topics.get(r["topic"])
        if sub is None or owner_schema is None:
            await self._journal.bump_attempts(   # max=1:一击即回死信(attempts 已被 replay 清零)
                r["journal_id"], r["subscriber"],
                "重放失败:订阅或 topic 契约已不在", 1)
            logger.error("死信重放失败:%s/%s 已无契约或订阅,回滚死信",
                         r["topic"], r["subscriber"], layer="CORE", component="Bus")
            return False
        payload = owner_schema[1].model_validate_json(r["payload"])
        env = Envelope(topic=r["topic"], source=r["source"], payload=payload,
                       msg_id=r["msg_id"], correlation_id=r["corr"],
                       published_at=time.time(), journal_id=r["journal_id"])
        await sub.queue.put(env)
        logger.info("死信重放入队 journal#%d → %s(%s)",
                    r["journal_id"], r["subscriber"], r["topic"],
                    layer="CORE", component="Bus")
        return True

    async def _handle(self, sub: _Subscription, env: Envelope) -> None:
        token = _current_corr.set(env.correlation_id)   # 因果链:handler 内续发自动携带
        try:
            await sub.handler(env)
            if env.topic not in self._ephemeral:
                # 投递账只记工作量纲(持久 + VOLATILE);纯遥测的签收不计,免高频淹没
                self.stats["delivered"] += 1
            if env.journal_id is not None and self._journal is not None:
                await self._journal.mark_done(env.journal_id, sub.key)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — 处理器故障隔离,总线不倒
            self.stats["handler_errors"] += 1
            if env.journal_id is None or self._journal is None:
                logger.error("总线处理器异常(EPHEMERAL,不重试)domain=%s slot=%s topic=%s err=%s",
                             sub.domain, sub.slot, env.topic, exc,
                             layer="CORE", component="Bus")
                return
            attempts, dead = await self._journal.bump_attempts(
                env.journal_id, sub.key, str(exc), self._max_attempts)
            if dead:
                self.stats["dead_letters"] += 1
                logger.error("总线死信:journal#%d → %s 重试 %d 次仍失败 topic=%s err=%s"
                             "(可经 journal.replay 重放)",
                             env.journal_id, sub.key, attempts, env.topic, exc,
                             layer="CORE", component="Bus")
                return
            self.stats["retried"] += 1
            delay = 0.5 * (2 ** (attempts - 1))     # 0.5s, 1s, 2s…
            logger.warning("总线重试:journal#%d → %s 第 %d 次失败,%.1fs 后重投 err=%s",
                           env.journal_id, sub.key, attempts, delay, exc,
                           layer="CORE", component="Bus")
            t = asyncio.create_task(self._requeue_later(sub, env, delay))
            self._retry_tasks.add(t)
            t.add_done_callback(self._retry_tasks.discard)
        finally:
            _current_corr.reset(token)

    async def _requeue_later(self, sub: _Subscription, env: Envelope, delay: float) -> None:
        await asyncio.sleep(delay)
        try:
            await self._enqueue(sub, env)
        except BusError:
            pass   # 入队超时已大声记录;投递账仍 pending,重启后重投

    # ── 内省 ──────────────────────────────────────────────────────────────

    def add_observer(self, fn: Callable[[dict], None]) -> None:
        """注册发布观察者(同步回调,收到每条消息的摘要 record)。"""
        self._observers.append(fn)

    def remove_observer(self, fn: Callable[[dict], None]) -> None:
        self._observers.remove(fn)

    def snapshot(self) -> dict:
        """全量内省:拓扑(声明即数据)+ 计数 + 队列水位 + 最近消息。"""
        domains = {
            d: {"publishes": sorted(pub), "subscribes": dict(sub)}
            for d, (pub, sub) in sorted(self._declared.items())
        }
        edges = []   # 发布域 →(topic 经某订阅)→ 订阅域
        for topic, (owner, _schema) in sorted(self._topics.items()):
            for s in self._subs:
                if topic_matches(s.pattern, topic):
                    edges.append({"from": owner, "to": s.domain, "topic": topic,
                                  "pattern": s.pattern, "slot": s.slot})
        return {
            "domains": domains,
            "edges": edges,
            "topics": {t: {"owner": o, "schema": sc.__name__,
                           "count": self._topic_counts.get(t, 0),
                           "ephemeral": t in self._ephemeral}
                       for t, (o, sc) in sorted(self._topics.items())},
            "subscriptions": [
                {"domain": s.domain, "pattern": s.pattern, "slot": s.slot,
                 "queue_depth": s.queue.qsize()} for s in self._subs],
            "stats": dict(self.stats),
            "recent": list(self._recent),
        }
