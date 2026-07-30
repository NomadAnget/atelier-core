"""src 组装入口:发现全部域 → 挂载 → 启动。

真实模块挂载协议(迁移期逐域接入):
    每个域提供一个 handlers 字典(槽位名 → async 处理器),经 build_hub()
    传入;域实现通过返回的 Window 发布消息。除此之外域与域之间零接触。

直接运行本文件 = 声明自检:所有头文件必须通过 registry 校验并成功挂载
(未接入的域用空转处理器占位),用于 CI/开发期快速验证契约完整性。
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import time
from typing import Awaitable, Callable

from . import logger
from .core import Bus, Envelope, Window, discover_headers
from .core.journal import Journal
from .core.registry import discover_schemas
from .core.storage import StorageHub, StoreWindow

Handlers = dict[str, Callable[[Envelope], Awaitable[None]]]

# 优雅停机硬时限(秒):超时=等价崩溃退出(账本兜底),防 GPU 推理线程拖死停机链
_SHUTDOWN_DEADLINE = 20.0


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
    for domain, header in discover_headers().items():
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
        from .core.registry import BusDeclarationError
        raise BusDeclarationError(
            f"持久 topic 无任何订阅者:{dangling} —— 要么加订阅,要么声明 EPHEMERAL")
    await bus.start()
    return bus, windows


async def build_store(db_path: str | None = None) -> tuple[StorageHub, dict[str, StoreWindow]]:
    """组装存储枢纽:发现全部域的 schema.py,挂载并自增量对齐数据库。

    Returns:
        (存储枢纽, {域名: 写窗口});读经 hub.read(自由),写只能走本域窗口。
    """
    from .core.log_store import LOGS_COLUMNS, LOGS_INDEXES, LOGS_TABLE
    hub = StorageHub(db_path or "tmp/src_store.db")
    windows = {domain: hub.mount(schema)
               for domain, schema in discover_schemas().items()}
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


async def serve(config_path: str | None = None) -> None:
    """常驻运行(新系统正式入口形态):读基建配置 → 组装 → 常驻服务 → 等停止信号。

    移植期说明:域实现尚未全部挂载,未接入的订阅槽位以空转处理器占位
    (消息照常投递销账);已上岗的常驻服务=哨兵。随各域移植完成,
    此处逐个接入真实 handlers 与域循环——组装点唯一,改动只发生在这里。
    """
    from . import config
    cfg = config.load(config_path)
    logger.set_level(cfg.logger.level)
    logger.info("基建配置来源:%s", cfg.source, layer="CORE", component="Main")

    # 单实例守卫:必须在组装/绑端口/起 worker 之前。抢不到=已有 hub 跑同一数据目录,
    # 拒绝启动(否则跨进程双领任务腐化 data/jobs、抢端口)。fd 存活至进程退出。
    _lock_fd = _acquire_single_instance_lock(cfg.paths.store_db)
    if _lock_fd is None:
        logger.error("已有 hub 进程在运行(数据目录 %s 被独占),本进程拒绝启动——"
                     "单实例架构不允许两个 hub 跑同一数据目录", cfg.paths.store_db,
                     layer="CORE", component="Main")
        return

    import os
    os.environ.setdefault("GCP_MODELS_DIR", cfg.paths.models_dir)   # 引擎库权重根唯一源(启动级)

    gateway = None
    svc_holder: dict = {}
    scheduler_handlers: Handlers = {
        "on_job_requested": lambda env: svc_holder["scheduler"].on_job_requested(env),
        "on_job_cancel": lambda env: svc_holder["scheduler"].on_job_cancel(env),
        "on_job_delete": lambda env: svc_holder["scheduler"].on_job_delete(env),
        "on_job_republish": lambda env: svc_holder["scheduler"].on_job_republish(env),
        "on_publish_discard": lambda env: svc_holder["scheduler"].on_publish_discard(env),
        "on_schedule_due": lambda env: svc_holder["scheduler"].on_schedule_due(env),
        "on_monitor_discovered": lambda env: svc_holder["scheduler"].on_monitor_discovered(env),
        "on_job_finished": lambda env: svc_holder["scheduler"].on_job_finished(env),
        "on_job_progress": lambda env: svc_holder["scheduler"].on_job_progress(env),
        "on_publish_finished": lambda env: svc_holder["scheduler"].on_publish_finished(env),
        "on_channel_targets_edit": lambda env: svc_holder["scheduler"].on_channel_targets_edit(env),
    }
    task_handlers: Handlers = {
        "on_run_requested": lambda env: svc_holder["task_runner"].on_run_requested(env),
        "on_cancel": lambda env: svc_holder["task_runner"].on_cancel(env),
        "on_settings_edit": lambda env: svc_holder["task_ops"].on_settings_edit(env),
        "on_job_deleted": lambda env: svc_holder["task_ops"].on_job_deleted(env),
    }
    daemons_handlers: Handlers = {
        "on_job_status": lambda env: svc_holder["monitor"].on_job_status(env),
        "on_job_deleted": lambda env: svc_holder["monitor"].on_job_deleted(env),
        "on_auth_expired": lambda env: svc_holder["monitor"].on_auth_expired(env),
        "on_channel_add": lambda env: svc_holder["monitor"].on_channel_add(env),
        "on_channel_edit": lambda env: svc_holder["monitor"].on_channel_edit(env),
        "on_channel_remove": lambda env: svc_holder["monitor"].on_channel_remove(env),
        "on_poll_now": lambda env: svc_holder["monitor"].on_poll_now(env),
        "on_poll_channel": lambda env: svc_holder["monitor"].on_poll_channel(env),
        "on_digest_source_add": lambda env: svc_holder["monitor"].on_digest_source_add(env),
        "on_digest_source_edit": lambda env: svc_holder["monitor"].on_digest_source_edit(env),
        "on_digest_source_remove": lambda env: svc_holder["monitor"].on_digest_source_remove(env),
        "on_digest_collect": lambda env: svc_holder["digest_collector"].on_digest_collect(env),
        "on_digest_collect_source": lambda env: svc_holder["digest_collector"].on_digest_collect_source(env),
        "on_pool_edit": lambda env: svc_holder["digest_collector"].on_pool_edit(env),
        "on_items_used": lambda env: svc_holder["digest_collector"].on_items_used(env),
        "on_settings_edit": lambda env: svc_holder["monitor"].on_settings_edit(env),
    }
    platform_adapters_handlers: Handlers = {
        "on_accounts_snapshot": lambda env: svc_holder["connections"].on_accounts_snapshot(env),
        "on_settings_edit": lambda env: svc_holder["ingest"].on_settings_edit(env),
    }
    engines_handlers: Handlers = {   # 纯声明域:owner=core.settings 底座(见下方接线)
        "on_settings_edit": lambda env: svc_holder["engines_settings_edit"](env),
    }
    remote_handlers: dict[str, Handlers] = {"scheduler": scheduler_handlers,
                                            "task": task_handlers,
                                            "daemons": daemons_handlers,
                                            "platform_adapters": platform_adapters_handlers,
                                            "engines": engines_handlers}
    if cfg.gateway.enabled and cfg.gateway.remote_domains:
        from .core.gateway.server import GatewayServer
        from .core.registry import load_header
        gateway = GatewayServer(host=cfg.gateway.host, port=cfg.gateway.port,
                                token=cfg.gateway.token,
                                ack_timeout=cfg.gateway.ack_timeout)
        # 合并而非覆盖:远程域只接管自己声明的槽位,本地域(scheduler/task/daemons)照旧
        remote_handlers.update({d: gateway.remote_handlers(load_header(d))
                                for d in cfg.gateway.remote_domains})

    bus, windows = await build_hub(
        remote_handlers, fill_noop=True, journal_db=cfg.paths.journal_db,
        queue_size=cfg.bus.queue_size, max_attempts=cfg.bus.max_attempts,
        put_timeout=cfg.bus.put_timeout)
    if gateway is not None:
        gateway.attach(bus, windows)
        await gateway.start()
    store, store_windows = await build_store(cfg.paths.store_db)
    from .core.log_store import LogStoreHandler
    log_sink = LogStoreHandler(cfg.paths.store_db, batch=cfg.logstore.batch,
                               flush_interval=cfg.logstore.flush_interval,
                               queue_max=cfg.logstore.queue_max)
    logging.getLogger(logger.ROOT).addHandler(log_sink)
    from .core.settings import SettingsStore, edit_handler
    from .engines.schema import EngineSettings
    engines_settings = SettingsStore(store, store_windows["engines"],
                                     "engines_settings", EngineSettings, layer="ENGINES")
    svc_holder["engines_settings_edit"] = edit_handler(engines_settings, "engines")
    await engines_settings.get()        # 启动落种:前端设置页立现 engines 节

    from .task.impl.ops_service import TaskOpsService
    svc_holder["task_ops"] = TaskOpsService(store, store_windows["task"])

    # task 执行器:领 scheduler 的 run_requested、跑管线、回 finished(执行交接经总线)
    from .task.impl.runner_service import TaskRunnerService
    svc_holder["task_runner"] = TaskRunnerService(
        windows["task"], asyncio.get_running_loop())

    # 管线注册(可插拔:每管线一行;加新任务类型只增不改)
    from .task.impl.digest_pipeline import register_news_digest
    register_news_digest(store, store_windows["task"], asyncio.get_running_loop(),
                         bus_win=windows["task"])   # items_used 事件发布(池归 daemons)

    from .scheduler.impl.publish_service import PublishService
    from .scheduler.impl.service import JobSchedulerService
    job_scheduler_svc = JobSchedulerService(
        windows["scheduler"], store, store_windows["scheduler"],
        publish_service=PublishService(windows["scheduler"], store))
    svc_holder["scheduler"] = job_scheduler_svc

    from .platform_adapters.impl.service import ConnectionService
    svc_holder["connections"] = ConnectionService(
        windows["platform_adapters"], store, store_windows["platform_adapters"])
    from .platform_adapters import ingest as pa_ingest
    # 采集门面(域根白名单件);实例接总线槽位 on_settings_edit(采集设置编辑落库)
    svc_holder["ingest"] = pa_ingest.init(store, store_windows["platform_adapters"])
    from .platform_adapters import youtube_token
    youtube_token.init(store, store_windows["platform_adapters"])   # OAuth token 入库(本域写方)

    # video_translate 注册(#7):依赖 ingest(下载/cookie),故在其后接线
    from .task.impl.pipelines.video_translate.runner import register_video_translate
    register_video_translate(store, store_windows["task"], asyncio.get_running_loop())
    yt_mirror_task = asyncio.create_task(
        svc_holder["connections"].run_youtube_mirror_loop(), name="yt_mirror")
    scheduler_worker = asyncio.create_task(job_scheduler_svc.run_worker(),
                                           name="scheduler_worker")

    # daemons 域:频道监控轮询 + 闲时排产(执行区间接真实 scheduler_jobs)
    from .daemons.impl.monitor import MonitorService
    from .daemons.impl.service import SchedulerService, make_jobs_intervals_provider
    monitor_svc = MonitorService(windows["daemons"], store, store_windows["daemons"])
    svc_holder["monitor"] = monitor_svc
    from .daemons import sources as daemons_sources
    daemons_sources.init(store, store_windows["daemons"])   # 采集源门面(域根白名单件)
    await daemons_sources.seed()        # 启动落种:前端监控源页首屏可见默认源
    scheduler_svc = SchedulerService(
        windows["daemons"], store, store_windows["daemons"],
        make_jobs_intervals_provider(store))
    from .daemons.impl.digest_collector import DigestCollector
    svc_holder["digest_collector"] = DigestCollector(store, store_windows["daemons"])
    monitor_loop_task = asyncio.create_task(monitor_svc.run_loop(), name="monitor_loop")
    digest_sched_task = asyncio.create_task(scheduler_svc.run_digest_loop(),
                                            name="digest_scheduler")
    digest_collect_task = asyncio.create_task(
        svc_holder["digest_collector"].run_loop(), name="digest_collector")

    web_server = None
    web_task = None
    if cfg.web.enabled and "web" in windows:
        import uvicorn

        from .web.impl.service import build_web_app
        web_app = build_web_app(bus, store, windows["web"])
        web_server = uvicorn.Server(uvicorn.Config(
            web_app, host=cfg.web.host, port=cfg.web.port,
            log_config=None, access_log=False))
        web_task = asyncio.create_task(web_server.serve(), name="web")

    from .supervisor import Supervisor
    supervisor = Supervisor(cfg.supervisor.children)
    await supervisor.start()

    housekeeping_task = asyncio.create_task(
        _housekeeping_loop(bus, store, cfg.retention), name="housekeeping")

    sentinel_task = None
    if "sentinel" in windows:
        from .sentinel.impl.service import SentinelService
        svc = SentinelService(
            bus, store, windows["sentinel"], store_windows["sentinel"],
            patrol_interval=cfg.sentinel.patrol_interval,
            queue_backlog_threshold=cfg.sentinel.queue_backlog_threshold,
            pending_stale_sec=cfg.sentinel.pending_stale_sec,
            job_stall_sec=cfg.sentinel.job_stall_sec,
            heartbeat_grace=cfg.sentinel.heartbeat_grace,
            error_burst_threshold=cfg.sentinel.error_burst_threshold)
        sentinel_task = asyncio.create_task(svc.run_loop(), name="sentinel")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    logger.info("src 常驻运行中(%d 域,journal=%s,store=%s)。Ctrl-C 优雅停机",
                len(windows), cfg.paths.journal_db, cfg.paths.store_db,
                layer="CORE", component="Main")
    await stop.wait()

    logger.info("收到停止信号,优雅停机…", layer="CORE", component="Main")
    supervisor.begin_shutdown()   # 第一步禁子进程重启:Ctrl+C 打死子进程后不再被重拉(见 supervisor)

    # ── 绝杀计时器(旧系统 os._exit 语义的守门员半边)────────────────────────
    # GPU 推理线程不可中断:它能拖死优雅链的任一环。超时硬退 = 等价一次崩溃,
    # 而崩溃语义账本本来就兜底(未竟投递在账,重启自动重投)——所以这里无损。
    # 优雅链按时走完则撤销,正常路径零影响。退出码 70(EX_SOFTWARE)标记超时退出。
    import os as _os
    import threading as _threading

    def _deadline_kill() -> None:
        logger.error("优雅停机超时(%.0fs),硬退——账本兜底:未竟投递重启自动重投",
                     _SHUTDOWN_DEADLINE, layer="CORE", component="Main")
        log_sink.flush(timeout=1.0)
        _os._exit(70)
    watchdog = _threading.Timer(_SHUTDOWN_DEADLINE, _deadline_kill)
    watchdog.daemon = True
    watchdog.start()

    if web_server is not None:
        # 先掐 SSE 长连接(它们永不自行排空;客户端按设计自动重连),
        # uvicorn 的优雅退出随即零等待通过
        web_app.state.close_streams()
        web_server.should_exit = True
        await asyncio.gather(web_task, return_exceptions=True)
    if gateway is not None:
        await gateway.stop()
    scheduler_worker.cancel()
    monitor_loop_task.cancel()
    digest_sched_task.cancel()
    digest_collect_task.cancel()
    await asyncio.gather(scheduler_worker, monitor_loop_task, digest_sched_task,
                         digest_collect_task, return_exceptions=True)
    await supervisor.stop()
    yt_mirror_task.cancel()
    await asyncio.gather(yt_mirror_task, return_exceptions=True)
    housekeeping_task.cancel()
    await asyncio.gather(housekeeping_task, return_exceptions=True)
    if sentinel_task is not None:
        sentinel_task.cancel()
        await asyncio.gather(sentinel_task, return_exceptions=True)
    await store.close()
    await bus.close()
    if bus.journal is not None:
        await bus.journal.close()
    logger.info("已停机:未竟投递在账,下次启动自动重投", layer="CORE", component="Main")
    log_sink.flush(timeout=3.0)
    logging.getLogger(logger.ROOT).removeHandler(log_sink)
    log_sink.shutdown()
    watchdog.cancel()
    # ── 终点硬退(旧系统 lifespan 结尾的 os._exit 语义)───────────────────────
    # 至此一切该落盘的都已落盘;解释器自然退出会去 join 线程池里可能仍在跑的
    # GPU 推理线程(不可 join),硬退只是跳过这次注定挂起的等待,不损任何状态。
    _os._exit(0)


async def _self_test() -> None:
    """声明自检:全域挂载 + 统计输出。契约有任何不合法此处即崩。"""
    bus, windows = await build_hub(fill_noop=True, journal_db="tmp/src_journal_selftest.db")
    topics = sum(len(h.PUBLISHES) for h in discover_headers().values())
    logger.info("枢纽自检通过:%d 个域挂载,%d 个 topic,订阅消费任务已启动",
                len(windows), topics, layer="CORE", component="HubSelfTest")
    store, store_windows = await build_store()
    logger.info("存储枢纽自检通过:%s", await store.snapshot(),
                layer="CORE", component="HubSelfTest")
    await store.close()
    await bus.close()
    if bus.journal is not None:
        await bus.journal.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="src 组装入口")
    parser.add_argument("--check", action="store_true",
                        help="只做声明自检后退出(CI/开发期契约验证)")
    parser.add_argument("--config", default=None, help="基建配置路径(默认 ./config.toml)")
    args = parser.parse_args()
    asyncio.run(_self_test() if args.check else serve(args.config))
