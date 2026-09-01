"""域装配原语 —— 域无关的组装底座(与内核库同源:随 gcp-core 走)。

这里只提供"怎么把域装起来"的通用原语:
    · build_hub   —— 目录扫描发现全部域 → 挂载 → 闭环校验 → 启动总线
    · build_store —— 发现全部 schema → 挂载 → 无感自增量对齐数据库
    · housekeeping/单实例锁/声明自检

**"装配哪些域、各域 handler 接什么"不在此**——那是各产品仓 `main.py::serve()`
的职责(见铁律:组装点唯一,产品接线随产品变体仓走)。本文件对任何产品的域集合
一无所知,只认注册表发现到的东西;无域即挂 0 个域(空总线)。
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable

from ..core import logger
from ..core import Bus, Envelope, Window, discover_headers
from ..core.journal import Journal
from ..core.registry import discover_schemas
from ..core.storage import StorageHub, StoreWindow

Handlers = dict[str, Callable[[Envelope], Awaitable[None]]]


def _acquire_single_instance_lock(store_db: str):
    """单实例守卫:对数据目录抢独占文件锁,抢不到=已有 hub 在跑,拒绝启动。

    架构是单进程(单 worker 串行、进程内 _pending/队列)。两个 hub 跑同一数据目录会
    跨进程双领同一任务(_run_one 领活是读-改写、非原子 CAS)、抢端口/抢 supervisor,
    腐化 data/jobs 产物。此锁从源头堵死。返回持锁 fd(须存活至进程退出;OS 在进程死时
    自动释放,故崩溃/硬退也不残留死锁)。抢不到返回 None。"""
    import fcntl
    import os
    from pathlib import Path
    lock_path = Path(store_db).resolve().parent / "hub.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = open(lock_path, "w")
    try:
        fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fd.close()
        return None
    fd.write(f"{os.getpid()}\n")
    fd.flush()
    return fd


async def build_hub(
    handlers_by_domain: dict[str, Handlers] | None = None,
    *,
    fill_noop: bool = False,
    journal_db: str | None = None,
    queue_size: int = 4096,
    max_attempts: int = 3,
    put_timeout: float = 30.0,
    domain_root: str | None = None,
) -> tuple[Bus, dict[str, Window]]:
    """组装枢纽:挂载 src/ 下全部域并启动总线。

    Args:
        handlers_by_domain: 各域的槽位处理器;缺槽位即抛错(显式失败)。
        fill_noop: 自检模式——未提供处理器的槽位以空转处理器占位。

    Returns:
        (总线, {域名: 发布窗口})
    """
    provided = handlers_by_domain or {}
    journal: Journal | None = None
    if journal_db:                      # 生产组装必须传:持久投递(崩溃重投/重试/死信)的基座
        journal = Journal(journal_db)
        await journal.start()
    bus = Bus(journal=journal, queue_size=queue_size,
              max_attempts=max_attempts, put_timeout=put_timeout)
    windows: dict[str, Window] = {}
    for domain, header in discover_headers(domain_root).items():
        handlers = dict(provided.get(domain, {}))
        if fill_noop:
            async def _noop(env: Envelope) -> None:
                logger.debug("noop 消费 %s(域未接入)", env.topic,
                             layer="CORE", component="HubSelfTest")
            for slot in set(header.SUBSCRIBES.values()) - handlers.keys():
                handlers[slot] = _noop
        windows[domain] = bus.mount(header, handlers)
    dangling = bus.check_closure()
    if dangling:                      # 组装期红灯:持久消息不许"发布即蒸发"
        from ..core.registry import BusDeclarationError
        raise BusDeclarationError(
            f"持久 topic 无任何订阅者:{dangling} —— 要么加订阅,要么声明 EPHEMERAL")
    await bus.start()
    return bus, windows


async def build_store(db_path: str | None = None, *, domain_root: str | None = None) -> tuple[StorageHub, dict[str, StoreWindow]]:
    """组装存储枢纽:发现全部域的 schema.py,挂载并自增量对齐数据库。

    Returns:
        (存储枢纽, {域名: 写窗口});读经 hub.read(自由),写只能走本域窗口。
    """
    from ..core.log_store import LOGS_COLUMNS, LOGS_INDEXES, LOGS_TABLE
    hub = StorageHub(db_path or "tmp/src_store.db")
    windows = {domain: hub.mount(schema)
               for domain, schema in discover_schemas(domain_root).items()}
    hub.attach_kernel_table(LOGS_TABLE, LOGS_COLUMNS, LOGS_INDEXES)   # 内核日志表
    await hub.start()
    return hub, windows


async def housekeeping_sweep(bus: Bus, store, retention) -> tuple[int, int]:
    """一轮内务:清全销账过期消息 + 过保留期日志。返回 (账本清理数, 日志清理数)。"""
    purged_journal = 0
    if bus.journal is not None:
        purged_journal = await bus.journal.purge_done(retention.journal_days * 86400)
    purged_logs = await store.kernel_purge(
        "hub_logs", "ts < ?", (time.time() - retention.logs_days * 86400,))
    if purged_journal or purged_logs:
        logger.info("内务:账本清理 %d 条消息,日志清理 %d 行",
                    purged_journal, purged_logs, layer="CORE", component="Housekeeping")
    return purged_journal, purged_logs


async def _housekeeping_loop(bus: Bus, store, retention) -> None:
    while True:
        try:
            await housekeeping_sweep(bus, store, retention)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error("内务循环异常 err=%s", exc, layer="CORE", component="Housekeeping")
        await asyncio.sleep(retention.sweep_interval_h * 3600)


async def self_test(domain_root: str | None = None) -> None:
    """声明自检:全域挂载 + 统计输出。契约有任何不合法此处即崩。

    域无关——发现到几个域就自检几个(空总线=0 域,亦通过)。"""
    bus, windows = await build_hub(fill_noop=True, journal_db="tmp/atelier_journal_selftest.db",
                                   domain_root=domain_root)
    topics = sum(len(h.PUBLISHES) for h in discover_headers(domain_root).values())
    logger.info("枢纽自检通过:%d 个域挂载,%d 个 topic,订阅消费任务已启动",
                len(windows), topics, layer="CORE", component="HubSelfTest")
    store, store_windows = await build_store(domain_root=domain_root)
    logger.info("存储枢纽自检通过:%s", await store.snapshot(),
                layer="CORE", component="HubSelfTest")
    await store.close()
    await bus.close()
    if bus.journal is not None:
        await bus.journal.close()
