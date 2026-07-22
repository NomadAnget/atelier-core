"""src 独立日志器 —— 新系统的黑匣子(零依赖:仅 stdlib)。

设计地位:比总线更底层的基础设施——内核、存储枢纽、各域、组装点都用它,
而它不依赖系统里任何其他东西(总线崩了它照常工作,这是黑匣子的本分)。

API 与旧系统 logger 兼容(``logger.info(msg, *args, layer=..., component=...)``),
API 与旧 logger 同形(``logger.info(msg, *args, layer=..., component=...)``)。
包内一律用相对 import(``from .. import logger`` 等);包目录名不硬编码在任何
功能代码里(顶层名从 ``__name__`` 推导,整包改名内部零改动)。

组成:
  控制台   紧凑单行(时间 级别 [layer/component] 消息),TTY 才染色;首次调用自动 setup
  订阅流   进程内扇出(asyncio.Queue),供 SSE/哨兵消费;慢订阅者丢帧不阻塞日志
  持久化   由组装点挂 LogStoreHandler(core/log_store.py)→ hub_logs 内核表;
           本模块对此不知情,保持零依赖
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from typing import Any

# 日志命名空间 = 顶层包名(从自身模块名推导,随目录改名自动跟随;
# main.py 挂 LogStoreHandler 时用同一 ROOT,两处永远指向同一 logger)
ROOT = __name__.split(".")[0]
_LOGGER = logging.getLogger(ROOT)
_configured = False
_subscribers: dict[asyncio.Queue, asyncio.AbstractEventLoop] = {}

_ANSI = {"DEBUG": "\033[2m", "INFO": "\033[36m", "WARNING": "\033[33m",
         "ERROR": "\033[31m", "CRITICAL": "\033[41m"}
_RESET = "\033[0m"


class _Formatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        layer = getattr(record, "layer", "") or ""
        comp = getattr(record, "component", "") or ""
        tag = f" [{layer}/{comp}]" if (layer or comp) else ""
        ts = time.strftime("%H:%M:%S", time.localtime(record.created))
        line = f"{ts} {record.levelname:<8}{tag} {record.getMessage()}"
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        if sys.stdout.isatty():
            color = _ANSI.get(record.levelname, "")
            line = f"{color}{line}{_RESET}" if color else line
        return line


def setup(level: str = "INFO") -> None:
    """幂等初始化:控制台 handler + 级别。首次记录日志时自动调用。"""
    global _configured
    if _configured:
        return
    _configured = True
    _LOGGER.setLevel(getattr(logging, level.upper(), logging.INFO))
    _LOGGER.propagate = False
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(_Formatter())
    _LOGGER.addHandler(h)


def set_level(level: str) -> None:
    """调整级别(组装点按 config.toml 调用;setup 幂等,此函数可随时调)。"""
    if not _configured:
        setup(level)
        return
    _LOGGER.setLevel(getattr(logging, level.upper(), logging.INFO))


# ── 订阅流(SSE/哨兵消费;不阻塞、不反噬)──────────────────────────────────────

def subscribe(maxsize: int = 512) -> asyncio.Queue:
    """订阅日志流,返回专属队列;每条日志一个 dict。慢订阅者满即丢帧。
    须在事件循环内调用(记录所属 loop,供 worker 线程日志跨线程投递)。"""
    q: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
    _subscribers[q] = asyncio.get_running_loop()
    return q


def unsubscribe(q: asyncio.Queue) -> None:
    _subscribers.pop(q, None)


def _put(q: asyncio.Queue, record: dict) -> None:
    try:
        q.put_nowait(record)
    except asyncio.QueueFull:
        pass


def _fanout(level: str, layer: str, component: str, message: str,
           phase: str = "", extra: dict | None = None) -> None:
    if not _subscribers:
        return
    record = {"ts": time.time(), "level": level, "layer": layer,
              "component": component, "message": message, "phase": phase}
    if extra:
        record.update(extra)
    for q, loop in list(_subscribers.items()):
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            _put(q, record)
        else:                          # worker 线程(引擎等)→ 跨线程安全投递
            try:
                loop.call_soon_threadsafe(_put, q, record)
            except RuntimeError:
                pass                   # loop 已关闭:订阅者已死,丢弃


# ── 记录 API(与旧 logger 同形)───────────────────────────────────────────────

def progress(msg: str, *, layer: str = "", component: str = "",
             key: str = "", frac: float | None = None, done: bool = False) -> None:
    """临时进度行:**只进程内扇出(SSE 实时),不落 hub_logs、不进控制台**。

    带稳定 ``key`` → 前端按 key **原地更新同一行**(``done=True`` 清除该行);
    ``frac`` ∈[0,1] 供渲染进度条。用于下载等长任务的 live 进度——黑匣子(hub_logs)
    只留 start/finish 两行,不被高频进度污染;无 SSE 订阅者时零开销直接返回。"""
    if not _subscribers:
        return
    extra: dict[str, Any] = {"ephemeral": True, "key": key}
    if frac is not None:
        extra["frac"] = frac
    if done:
        extra["done"] = True
    _fanout("INFO", layer, component, msg, extra=extra)


def _log(level: int, msg: str, *args: Any, layer: str = "", component: str = "",
         exc_info: Any = None, console_only: bool = False, phase: str = "") -> None:
    """console_only=True:仅终端可见(引擎心跳这类刷屏行)——不入库、不进订阅流。

    phase:引擎生命周期配对标记(如 "start"/"finish"),供前端/消费方按结构化
    字段配对同一执行的起止两行——而非猜测 message 文本前缀(见 engines/base.py
    用法)。未传即空串,绝大多数日志无需关心。"""
    if not _configured:
        setup()
    _LOGGER.log(level, msg, *args, exc_info=exc_info,
                extra={"layer": layer, "component": component,
                       "console_only": console_only, "phase": phase})
    if console_only or not _LOGGER.isEnabledFor(level):
        return
    try:
        text = msg % args if args else msg
    except (TypeError, ValueError):
        text = msg
    _fanout(logging.getLevelName(level), layer, component, text, phase)


def debug(msg: str, *args: Any, **kw: Any) -> None:
    _log(logging.DEBUG, msg, *args, **kw)


def info(msg: str, *args: Any, **kw: Any) -> None:
    _log(logging.INFO, msg, *args, **kw)


def warning(msg: str, *args: Any, **kw: Any) -> None:
    _log(logging.WARNING, msg, *args, **kw)


def error(msg: str, *args: Any, **kw: Any) -> None:
    _log(logging.ERROR, msg, *args, **kw)


def exception(msg: str, *args: Any, **kw: Any) -> None:
    kw.setdefault("exc_info", True)
    _log(logging.ERROR, msg, *args, **kw)
