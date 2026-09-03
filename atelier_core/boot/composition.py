"""域装配基础设施 —— 按 KIND 通用组装(local / process / remote)。

第三根支柱的"执行侧":header(总线契约)、schema(存储)声明**是什么**,
build.py 声明**怎么构造**,本模块与 main.py 是**唯一执行组装的基础设施**——
域绝不接管 bus.mount / 网关 / 子进程机制。是基础设施层(非域、非内核),
import 白名单豁免(与 main/supervisor 同列)。

三类型:
  · local   —— 主进程内:build(主进程 ctx) → handlers 挂本地总线 + 起循环;
  · process —— 按需子进程:主进程搭网关转发 + lazy 子进程 + scheduler 起停;
               子进程(`python -m atelier_core.boot.remote <域> [域根]`,本模块 run_domain_subprocess)
               调**同一** build(BusClient ctx)真执行;
  · remote  —— 异地/外部实现:无 build.py,网关转发,公民资格外部自取。

BuildContext / DomainRuntime 是域与基础设施之间的装配契约(域只碰这两个类型)。
"""

from __future__ import annotations

import asyncio
import importlib
import signal
from dataclasses import dataclass, field
from types import ModuleType, SimpleNamespace
from typing import Any, Awaitable, Callable

from ..core import config, logger
from ..core.registry import _ROOT as _DEFAULT_ROOT, discover_headers, discover_schemas

_BUSY_TICK_SEC = 10.0    # 进程域忙帧间隔(须 < 网关 register_lazy 的 idle_sec,活跃时持续刷 last_active)
from ..core.storage import StorageHub, StoreWindow

Handler = Callable[[Any], Awaitable[None]]


@dataclass
class BuildContext:
    """基础设施交给域 build() 的一切(域只用这些构造,不碰组装机制)。

    publish 是本域发布句柄:主进程内是 Window,子进程内是 BusClient——二者
    `async publish(topic, payload)` 鸭子同型,域无感。bus 是完整总线(web/sentinel
    的观察者/HTTP 应用需要;子进程内为 None——那些域只 local,不出进程)。
    infra 是基础设施横切件(如 process 域的 'task_process' 控制器)。
    """

    hub: StorageHub
    stores: dict[str, StoreWindow]
    publish: Any
    loop: asyncio.AbstractEventLoop
    cfg: Any
    bus: Any = None
    infra: dict[str, Any] = field(default_factory=dict)


@dataclass
class DomainRuntime:
    """域 build() 的产物:基础设施据此挂 handler、起循环、跑落种/停机。"""

    handlers: dict[str, Handler] = field(default_factory=dict)
    loops: list[Awaitable] = field(default_factory=list)   # 背景协程(create_task)
    on_seed: Callable[[], Awaitable[None]] | None = None    # 启动落种(seed/get)
    on_shutdown: Callable[[], Awaitable[None]] | None = None
    # 进程域"是否在忙"钩子(有活跃后台工作时返回 True)。仅子进程内被宿主轮询:忙则周期给网关
    # 发传输层 busy 帧(供网关闲时回收决策——handler fire-and-forget 后 inflight 归零,靠此续命)。
    # 这是**网关↔子进程的传输信号**,不是日志、不是业务 topic、不进哨兵(见 docs boot.md 形态 B)。
    busy: Callable[[], bool] | None = None


def load_build(domain: str, root: str | None = None) -> ModuleType | None:
    """加载域的 build.py;无此文件=非本域根实现域(remote,外部实现)。root=域根包。"""
    try:
        return importlib.import_module(f"{root or _DEFAULT_ROOT}.{domain}.build")
    except ModuleNotFoundError:
        return None


def domain_kind(domain: str, root: str | None = None) -> str:
    """域类型:有 build.py 取其 KIND(local | process);无 build.py 默认 local
    (未迁移域仍在 main 手工接线)。**remote 不由此判**——那是部署事实,由
    config.gateway.remote_domains 显式声明(外部实现,无 src build.py),与本函数正交。"""
    b = load_build(domain, root)
    return getattr(b, "KIND", "local") if b is not None else "local"


def topic_schema_map(root: str | None = None) -> dict[str, Any]:
    """topic → 载荷契约(全域 PUBLISHES 汇总):子进程把 BusClient 收到的 dict
    还原成 Message 用。"""
    out: dict[str, Any] = {}
    for hd in discover_headers(root).values():
        out.update(hd.PUBLISHES)
    return out


def _adapt(handler: Handler, schema: Any) -> Callable[[dict, dict], Awaitable[None]]:
    """BusClient 处理器签名 (payload:dict, meta:dict) → 域 handler(env)。"""
    async def h(payload: dict, meta: dict) -> None:
        await handler(SimpleNamespace(
            topic=meta.get("topic"), msg_id=meta.get("msg_id"),
            correlation_id=meta.get("corr"),
            payload=schema.model_validate(payload or {})))
    return h


async def run_domain_subprocess(domain: str, root: str | None = None) -> None:
    """process 域子进程宿主(`python -m src.boot.remote <域>`)——通用,不为任何域特化。

    组装 = 开存储枢纽(挂全 schema,DDL 幂等)→ BusClient 接网关 → 调域 build()
    (与主进程同一份)→ 绑 handler → 起循环/落种 → 常驻直至信号。日志走结构化
    中继(BusClient 自动),detach 控制台防与监督器文本泵重复。
    """
    root = root or _DEFAULT_ROOT
    from ..core.gateway.client import BusClient

    cfg = config.load()
    logger.set_level(cfg.logger.level)
    logger.detach_console()
    import os
    os.environ.setdefault("GCP_MODELS_DIR", cfg.paths.models_dir)

    build_mod = load_build(domain, root)
    if build_mod is None or getattr(build_mod, "KIND", None) != "process":
        raise RuntimeError(f"'{domain}' 不是 process 域(无 build.py 或 KIND≠process)")
    if not cfg.gateway.enabled:
        raise RuntimeError("process 域子进程需要网关(config [gateway] enabled=true)")

    hub = StorageHub(cfg.paths.store_db)
    stores = {d: hub.mount(s) for d, s in discover_schemas(root).items()}
    await hub.start()

    loop = asyncio.get_running_loop()
    client = BusClient(f"ws://{cfg.gateway.host}:{cfg.gateway.port}",
                       domain=domain, token=cfg.gateway.token)
    ctx = BuildContext(hub=hub, stores=stores, publish=client, loop=loop, cfg=cfg)
    rt = build_mod.build(ctx)

    header = importlib.import_module(f"{root}.{domain}.header")
    schemas = topic_schema_map(root)
    for topic, slot in header.SUBSCRIBES.items():
        h = rt.handlers.get(slot)
        if h is None:
            raise RuntimeError(f"{domain} build 缺槽位实现:{slot}")
        client.bind(slot, _adapt(h, schemas[topic]))

    if rt.on_seed is not None:
        await rt.on_seed()
    tasks = [asyncio.create_task(c) for c in rt.loops]
    await client.start()

    # 忙帧循环:域有活跃后台工作时,周期给网关发传输层 busy 帧,让网关别在活干到一半时
    # (fire-and-forget handler 已 ack、inflight 归零)误回收本子进程。空闲即停发 → 网关闲时回收。
    if rt.busy is not None:
        async def _busy_ticker() -> None:
            while True:
                try:
                    if rt.busy():
                        await client.send_busy()
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001
                    pass
                await asyncio.sleep(_BUSY_TICK_SEC)
        tasks.append(asyncio.create_task(_busy_ticker(), name=f"{domain}_busy_ticker"))
    logger.info("%s 执行进程就绪 pid=%d(网关 ws://%s:%d)", domain, os.getpid(),
                cfg.gateway.host, cfg.gateway.port, layer=domain.upper(),
                component=f"{domain}Worker")

    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    logger.info("%s 执行进程收到停止信号,退出", domain,
                layer=domain.upper(), component=f"{domain}Worker")
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    if rt.on_shutdown is not None:
        await rt.on_shutdown()
    await client.close()
