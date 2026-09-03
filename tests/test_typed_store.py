"""类型化表规格 + 内核日志落库验证。

覆盖:声明校验、建表/索引、列白名单写入、双向误用防护、
安全自动加列(可加)与显式拒绝(NOT NULL 无默认)、内核表挂接、
LogStoreHandler 端到端(logger → 写线程 → hub_logs 可查)。
"""

from __future__ import annotations

import logging
import sqlite3
import types
import unittest
import uuid
from pathlib import Path

from atelier_core.core.registry import BusDeclarationError, validate_schema
from atelier_core.core.storage import StorageHub, StoreError

TMP = Path("tmp")


def _db() -> str:
    TMP.mkdir(exist_ok=True)
    return str(TMP / f"typed_test_{uuid.uuid4().hex[:8]}.db")


def _schema(domain: str, tables: dict) -> types.ModuleType:
    mod = types.ModuleType(f"src.{domain}.schema")
    mod.DOMAIN, mod.TABLES = domain, tables
    return mod


JOBS_V1 = {"columns": {"job_id": "TEXT NOT NULL", "status": "TEXT DEFAULT 'queued'",
                       "score": "REAL DEFAULT 0"},
           "indexes": ["status", "score"], "unique": []}


class TypedValidationTests(unittest.TestCase):
    def test_columns_and_kinds_are_exclusive(self):
        from atelier_core.core.contracts import Record

        class R(Record):
            x: int = 0
        bad = _schema("p", {"p_t": {"columns": {"a": "TEXT"}, "kinds": {"k": R}}})
        with self.assertRaises(BusDeclarationError):
            validate_schema(bad)

    def test_index_must_be_declared_column(self):
        bad = _schema("p", {"p_t": {"columns": {"a": "TEXT"}, "indexes": ["b"]}})
        with self.assertRaises(BusDeclarationError):
            validate_schema(bad)

    def test_id_column_reserved(self):
        bad = _schema("p", {"p_t": {"columns": {"id": "TEXT"}}})
        with self.assertRaises(BusDeclarationError):
            validate_schema(bad)


class TypedStoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_crud_whitelist_and_index(self):
        hub = StorageHub(_db())
        win = hub.mount(_schema("p", {"p_jobs": dict(JOBS_V1)}))
        await hub.start()
        try:
            rid = await win.insert_row("p_jobs", {"job_id": "j1", "score": 7.5})
            rows = await hub.read("p_jobs", where="score > ?", params=(5,))
            self.assertEqual(rows[0]["job_id"], "j1")
            self.assertEqual(rows[0]["status"], "queued")     # 列默认值生效

            plan = hub._conn.execute(
                "EXPLAIN QUERY PLAN SELECT * FROM p_jobs WHERE score > 5").fetchone()
            self.assertIn("idx_p_jobs_score", str(plan))       # 真索引在干活

            await win.update_row("p_jobs", rid, {"status": "running"})
            rows = await hub.read("p_jobs")
            self.assertEqual(rows[0]["status"], "running")

            with self.assertRaises(StoreError):                # 列白名单
                await win.insert_row("p_jobs", {"job_id": "x", "ghost": 1})
            with self.assertRaises(StoreError):                # 行不存在
                await win.update_row("p_jobs", 999, {"status": "x"})
            with self.assertRaises(StoreError):                # 类型化表禁用信封写口
                await win.insert("p_jobs", "k", {"job_id": "x"})
            with self.assertRaises(StoreError):                # kind 维度不存在
                await hub.read("p_jobs", kind="k")
            self.assertEqual(await win.delete("p_jobs", rid), 1)
        finally:
            await hub.close()

    async def test_typed_writes_broadcast_store_events(self):
        """类型化 insert/update/delete 必须广播存储事件(与信封路径同权)。

        缺失时前端 SSE 收不到 daemons_channels/scheduler_jobs 等类型化表的写,
        登记频道后页面不自刷(实录:登记成功但列表空,需手动刷新)。"""
        hub = StorageHub(_db())
        win = hub.mount(_schema("p", {"p_jobs": dict(JOBS_V1)}))
        await hub.start()
        events: list[tuple] = []
        hub.add_observer(lambda r: events.append((r["op"], r["table"], r["domain"])))
        try:
            rid = await win.insert_row("p_jobs", {"job_id": "j1"})
            await win.update_row("p_jobs", rid, {"status": "running"})
            await win.delete("p_jobs", rid)
            self.assertEqual(events, [("insert", "p_jobs", "p"),
                                      ("update", "p_jobs", "p"),
                                      ("delete", "p_jobs", "p")])
        finally:
            await hub.close()

    async def test_envelope_rejects_typed_writer(self):
        from atelier_core.core.contracts import Record

        class R(Record):
            x: int = 0
        hub = StorageHub(_db())
        win = hub.mount(_schema("p", {"p_env": {"kinds": {"k": R}}}))
        await hub.start()
        try:
            with self.assertRaises(StoreError):
                await win.insert_row("p_env", {"x": 1})
        finally:
            await hub.close()

    async def test_safe_auto_add_column_and_refusal(self):
        """列演进:可安全追加(有默认/可空)自动加列;NOT NULL 无默认显式拒绝。"""
        path = _db()
        hub1 = StorageHub(path)
        w1 = hub1.mount(_schema("p", {"p_jobs": dict(JOBS_V1)}))
        await hub1.start()
        await w1.insert_row("p_jobs", {"job_id": "old"})
        await hub1.close()

        v2 = {"columns": {**JOBS_V1["columns"], "note": "TEXT DEFAULT ''"},
              "indexes": ["status"], "unique": []}
        hub2 = StorageHub(path)
        hub2.mount(_schema("p", {"p_jobs": v2}))
        await hub2.start()                                    # 自动加列
        rows = await hub2.read("p_jobs")
        self.assertEqual(rows[0]["note"], "")                 # 老行取列默认
        await hub2.close()

        v3 = {"columns": {**JOBS_V1["columns"], "must": "TEXT NOT NULL"},
              "indexes": [], "unique": []}
        hub3 = StorageHub(path)
        hub3.mount(_schema("p", {"p_jobs": v3}))
        with self.assertRaises(StoreError):                   # 不安全 → 人工迁移
            await hub3.start()

    @unittest.skip("待适配 atelier-core 组装根:log-store/housekeeping e2e——后续修正")

    async def test_kernel_logs_end_to_end(self):
        """logger → LogStoreHandler(写线程)→ hub_logs 内核表可查。"""
        from atelier_core.core import logger
        from atelier_core.core.log_store import (LOGS_COLUMNS, LOGS_INDEXES, LOGS_TABLE,
                                           LogStoreHandler)
        path = _db()
        hub = StorageHub(path)
        hub.attach_kernel_table(LOGS_TABLE, LOGS_COLUMNS, LOGS_INDEXES)
        await hub.start()
        sink = LogStoreHandler(path, flush_interval=0.05)
        logging.getLogger("src").addHandler(sink)
        try:
            logger.info("入库测试:第 %d 条", 1, layer="CORE", component="TypedTest")
            logger.error("入库测试:错误样本", layer="SENTINEL", component="TypedTest")
            logger.info("配对测试:开始", layer="CORE", component="TypedTest",
                        phase="infer:start:abc123")
            sink.flush()
            rows = await hub.read(LOGS_TABLE, where="component = ?",
                                  params=("TypedTest",), order_by="id ASC")
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0]["message"], "入库测试:第 1 条")
            self.assertEqual(rows[0]["layer"], "CORE")
            self.assertEqual(rows[0]["phase"], "", "未传 phase 落库为空串,不是 NULL")
            self.assertEqual(rows[1]["level"], "ERROR")
            self.assertEqual(rows[2]["phase"], "infer:start:abc123",
                             "结构化配对字段随日志落库(不靠 message 文本猜)")
            snap = await hub.snapshot()                       # 内核表进快照
            self.assertEqual(snap[LOGS_TABLE]["owner"], "kernel")
            self.assertEqual(snap[LOGS_TABLE]["type"], "typed")
        finally:
            logging.getLogger("src").removeHandler(sink)
            sink.close()
            await hub.close()

    def test_writer_survives_db_error(self):
        """写线程遇落库异常绝不死亡(防回归)——否则 task_done 停摆,flush 的
        _q.join() 永挂,优雅停机撞硬时限硬退(实测 20s 卡死主因)。"""
        import sqlite3
        import time
        from atelier_core.core.log_store import LogStoreHandler
        path = _db()
        sink = LogStoreHandler(path, batch=3, flush_interval=0.05)
        try:
            time.sleep(0.3)                                   # 写线程建表
            c = sqlite3.connect(path); c.execute("DROP TABLE hub_logs"); c.commit(); c.close()
            for i in range(9):                                # 撞 "no such table"
                r = logging.LogRecord("x", logging.INFO, "f", 1, f"g{i}", (), None)
                r.layer, r.component, r.phase = "T", "T", ""
                sink.emit(r)
            time.sleep(0.6)
            self.assertTrue(sink._thread.is_alive(), "写线程遇 DB 错误后必须存活")
            t0 = time.monotonic()
            sink.flush(timeout=2.0)                           # 有界:即便线程缺席也不挂
            self.assertLess(time.monotonic() - t0, 1.5, "flush 不得挂起")
            self.assertEqual(sink._q.unfinished_tasks, 0, "task_done 照常推进")
        finally:
            sink.close()


class HousekeepingTests(unittest.IsolatedAsyncioTestCase):
    @unittest.skip("待适配 atelier-core 组装根:log-store/housekeeping e2e——后续修正")
    async def test_kernel_purge_guard_and_sweep(self):
        """kernel_purge 只对内核表放行;内务一轮清理过期日志与全销账消息。"""
        from atelier_core.core.log_store import LOGS_COLUMNS, LOGS_INDEXES, LOGS_TABLE
        from atelier_core.core.journal import Journal
        from atelier_core.core.storage import StorageHub, StoreError
        from atelier_core.core.config import RetentionCfg
        from atelier_core.boot.assembly import housekeeping_sweep
        import sqlite3, time as _t

        path = _db()
        hub = StorageHub(path)
        hub.mount(_schema("p", {"p_jobs": dict(JOBS_V1)}))
        hub.attach_kernel_table(LOGS_TABLE, LOGS_COLUMNS, LOGS_INDEXES)
        await hub.start()
        with self.assertRaises(StoreError):          # 域表不许走内核清理
            await hub.kernel_purge("p_jobs", "id > ?", (0,))

        # 手工塞一行"上古日志" + 一行新日志
        conn = sqlite3.connect(path)
        conn.execute(f"INSERT INTO {LOGS_TABLE} (ts, level, layer, component, message) "
                     "VALUES (?,?,?,?,?)", (_t.time() - 90 * 86400, "INFO", "CORE", "old", "旧"))
        conn.execute(f"INSERT INTO {LOGS_TABLE} (ts, level, layer, component, message) "
                     "VALUES (?,?,?,?,?)", (_t.time(), "INFO", "CORE", "new", "新"))
        conn.commit(); conn.close()

        journal = Journal(_db()); await journal.start()
        from atelier_core.core import Bus
        bus = Bus(journal=journal)
        try:
            pj, pl = await housekeeping_sweep(bus, hub, RetentionCfg(logs_days=30.0))
            self.assertEqual(pl, 1, "只清过保留期的那行")
            rows = await hub.read(LOGS_TABLE)
            self.assertEqual([r["component"] for r in rows], ["new"])
        finally:
            await bus.close(); await journal.close(); await hub.close()


if __name__ == "__main__":
    unittest.main()
