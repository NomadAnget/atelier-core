"""组装入口(模板)—— 产品仓在此接自己的域 handlers。

gcp-core 是**底座**:本文件只给一个域无关的最小 serve()(发现到几个域就挂几个,
未接线的订阅槽以 fill_noop 占位),让空总线能起来、`--check` 能自检。

**产品仓(变体)应替换本文件**:仿 Architecture 的 main.py,为自己拥有的每个域
写 `<域>_handlers: Handlers = {...}` 字面接线并构造各域服务/循环。装配原语
(build_hub/build_store/...)一律从 assembly.py 取,不要在这里重造。
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal

from . import logger
from .assembly import (
    _acquire_single_instance_lock,
    _housekeeping_loop,
    build_hub,
    build_store,
    self_test,
)


async def serve(config_path: str | None = None) -> None:
    """最小常驻:组装空/现有域 → 起 web(若域在)→ 等停止信号。产品仓按需扩写。"""
    from . import config
    cfg = config.load(config_path)
    logger.set_level(cfg.logger.level)
    logger.info("基建配置来源:%s", cfg.source, layer="CORE", component="Main")

    lock_fd = _acquire_single_instance_lock(cfg.paths.store_db)
    if lock_fd is None:
        logger.error("已有 hub 独占数据目录 %s,本进程拒绝启动", cfg.paths.store_db,
                     layer="CORE", component="Main")
        return

    bus, windows = await build_hub(
        fill_noop=True, journal_db=cfg.paths.journal_db,
        queue_size=cfg.bus.queue_size, max_attempts=cfg.bus.max_attempts,
        put_timeout=cfg.bus.put_timeout)
    store, _store_windows = await build_store(cfg.paths.store_db)
    from .core.log_store import LogStoreHandler
    log_sink = LogStoreHandler(cfg.paths.store_db, batch=cfg.logstore.batch,
                               flush_interval=cfg.logstore.flush_interval,
                               queue_max=cfg.logstore.queue_max)
    logging.getLogger(logger.ROOT).addHandler(log_sink)

    web_server = web_task = None
    if cfg.web.enabled and "web" in windows:
        import uvicorn

        from .web.impl.service import build_web_app       # 产品带 web 域时才有
        web_app = build_web_app(bus, store, windows["web"])
        web_server = uvicorn.Server(uvicorn.Config(
            web_app, host=cfg.web.host, port=cfg.web.port,
            log_config=None, access_log=False))
        web_task = asyncio.create_task(web_server.serve(), name="web")

    housekeeping_task = asyncio.create_task(
        _housekeeping_loop(bus, store, cfg.retention), name="housekeeping")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    logger.info("gcp-core 常驻运行中(%d 域,store=%s)。Ctrl-C 停机",
                len(windows), cfg.paths.store_db, layer="CORE", component="Main")
    await stop.wait()

    logger.info("停机中…", layer="CORE", component="Main")
    if web_server is not None:
        web_server.should_exit = True
        await asyncio.gather(web_task, return_exceptions=True)
    housekeeping_task.cancel()
    await asyncio.gather(housekeeping_task, return_exceptions=True)
    await store.close()
    await bus.close()
    if bus.journal is not None:
        await bus.journal.close()
    log_sink.flush(timeout=3.0)
    logging.getLogger(logger.ROOT).removeHandler(log_sink)
    log_sink.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="gcp-core 组装入口(模板)")
    parser.add_argument("--check", action="store_true",
                        help="只做声明自检后退出(CI/开发期契约验证)")
    parser.add_argument("--config", default=None, help="基建配置路径(默认 ./config.toml)")
    args = parser.parse_args()
    asyncio.run(self_test() if args.check else serve(args.config))
