"""内核日志落库 —— 黑匣子的持久出口(hub_logs 类型化内核表)。

设计:
  · hub_logs 是内核直管表(owner=kernel):建表/读/快照由存储枢纽负责
    (组装时 ``hub.attach_kernel_table(LOGS_TABLE, LOGS_COLUMNS, LOGS_INDEXES)``),
    写由本模块的独立线程完成——logger 记录路径零阻塞、零事件循环占用、
    零总线依赖(总线崩了日志照录,黑匣子本分);
  · LogStoreHandler 是标准 logging.Handler:组装点把它挂到 "src" logger 上,
    logger.py 本体保持零依赖不知情;
  · 批量落库(攒批 N 条或 T 秒),队列饱和丢帧不阻塞记录(控制台出口仍在);
  · WAL 多连接:写线程自持连接,与存储枢纽的连接并行无碍。
"""

from __future__ import annotations

import atexit
import logging
import queue
import sqlite3
import threading
import time
from pathlib import Path

LOGS_TABLE = "hub_logs"
LOGS_COLUMNS = {
    "ts": "REAL NOT NULL",
    "level": "TEXT NOT NULL",
    "layer": "TEXT DEFAULT ''",
    "component": "TEXT DEFAULT ''",
    "message": "TEXT NOT NULL",
    # 引擎生命周期配对标记("start"/"finish",见 logger.py `_log(phase=...)`);
    # 演进规则:只增带默认值字段——存量表挂载时自动 ADD COLUMN,老行读出为 ''
    "phase": "TEXT DEFAULT ''",
}
LOGS_INDEXES = ["ts", "level", "layer"]

_DDL = (f"CREATE TABLE IF NOT EXISTS {LOGS_TABLE} "
        "(id INTEGER PRIMARY KEY AUTOINCREMENT, "
        + ", ".join(f"{n} {frag}" for n, frag in LOGS_COLUMNS.items()) + ")")


class LogStoreHandler(logging.Handler):
    """logging.Handler → hub_logs:emit 只入内存队列(微秒级),写线程批量落库。"""

    def __init__(self, db_path: str | Path, *, batch: int = 200,
                 flush_interval: float = 0.5, queue_max: int = 10000) -> None:
        super().__init__()
        self._db_path = str(db_path)
        self._batch = batch
        self._interval = flush_interval
        self._q: queue.Queue = queue.Queue(maxsize=queue_max)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._writer, daemon=True,
                                        name="hub-logstore")
        self._thread.start()
        atexit.register(self.shutdown)

    # ── 记录路径(调用方线程,零阻塞)───────────────────────────────────────────

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(record, "console_only", False):
            return                     # 心跳刷屏行:仅终端,不落库
        try:
            self._q.put_nowait((record.created, record.levelname,
                                getattr(record, "layer", "") or "",
                                getattr(record, "component", "") or "",
                                record.getMessage(),
                                getattr(record, "phase", "") or ""))
        except queue.Full:
            pass   # 落库饱和丢帧:绝不阻塞记录路径(控制台出口不受影响)

    def flush(self, timeout: float | None = None) -> None:
        """等队列全部落库(测试/停机用)。timeout=None 无限等(测试用,写线程恒活故安全);
        给 timeout=有界等待(停机路径用):即便写线程异常缺席也绝不无限挂起。"""
        if timeout is None:
            self._q.join()
            return
        deadline = time.monotonic() + timeout
        while self._q.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.02)

    def close(self) -> None:
        """对 logging 框架的 close 免疫:第三方库(dora/paddle 等)在 import 时调
        logging.config.dictConfig → _clearExistingHandlers → 关停全进程 handler,
        黑匣子会被无辜灭口(实录:demucs→dora 的 import 关掉本 sink,hub_logs 断流
        数小时)。真正的停机走 shutdown()(serve 停机链/atexit 显式调用)。"""

    def shutdown(self) -> None:
        if not self._stop.is_set():
            self._stop.set()
            self._thread.join(timeout=5)
        super().close()

    # ── 写线程(自持连接,攒批提交)────────────────────────────────────────────

    def _writer(self) -> None:
        try:
            self._writer_impl()
        except BaseException:                       # noqa: BLE001
            # 写线程死亡=黑匣子失明+flush 永挂(靠有界超时兜底)。绝不允许静默死:
            # 遗言直打 stderr(logging 路径此刻可能已不可用)。
            import sys
            import traceback
            print("‼️ hub-logstore 写线程异常死亡(hub_logs 停止落库):",
                  file=sys.stderr, flush=True)
            traceback.print_exc()

    def _writer_impl(self) -> None:
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self._db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(_DDL)                      # 与 attach_kernel_table 同源 DDL,先到先建
        for col in LOGS_INDEXES:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{LOGS_TABLE}_{col} "
                         f"ON {LOGS_TABLE}({col})")
        conn.commit()

        buf: list[tuple] = []
        last_flush = time.monotonic()
        while not (self._stop.is_set() and self._q.empty() and not buf):
            try:
                buf.append(self._q.get(timeout=self._interval))
            except queue.Empty:
                pass
            now = time.monotonic()
            if buf and (len(buf) >= self._batch or now - last_flush >= self._interval
                        or self._stop.is_set()):
                try:
                    conn.executemany(
                        f"INSERT INTO {LOGS_TABLE} (ts, level, layer, component, message, phase) "
                        "VALUES (?,?,?,?,?,?)", buf)
                    conn.commit()
                except Exception as _wexc:
                    # 落库失败(如停机期与枢纽争 WAL 写锁的 database is locked)绝不杀死
                    # 写线程——否则 task_done 停摆,flush() 的 _q.join() 永挂(实测停机卡死主因)。
                    # 丢这批日志(可接受:黑匣子非账本),回滚后继续;失败必须可见(stderr,
                    # 不走 logging 防递归)。
                    import sys as _s
                    print(f"‼️ hub_logs 落库失败,丢弃 {len(buf)} 条: {type(_wexc).__name__}: {_wexc}",
                          file=_s.stderr, flush=True)
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                finally:
                    for _ in buf:
                        self._q.task_done()
                    buf.clear()
                    last_flush = now
        conn.close()
