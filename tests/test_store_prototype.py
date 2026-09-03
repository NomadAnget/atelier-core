"""存储枢纽原型可行性验证。

覆盖:自增量建表、跨重启增量(新表加入且旧数据保留)、写权随目录走、
kind/形状强校验、字段演进(只增带默认值)、提升列索引与唯一约束、
信封漂移检测(不自动改表)。
"""

from __future__ import annotations

import sqlite3
import types
import unittest
import uuid
from pathlib import Path

from atelier_core.core.contracts import Record
from atelier_core.core.registry import BusDeclarationError, validate_schema
from atelier_core.core.storage import StorageHub, StoreError

TMP = Path("tmp")


def _db() -> str:
    TMP.mkdir(exist_ok=True)
    return str(TMP / f"store_test_{uuid.uuid4().hex[:8]}.db")


class AllocV1(Record):
    task_type: str
    start_at: float


class AllocV2(Record):
    task_type: str
    start_at: float
    priority: int = 5          # 演进:只增带默认值的字段


class NoteRec(Record):
    key: str
    body: str = ""


def _schema(domain: str, tables: dict) -> types.ModuleType:
    mod = types.ModuleType(f"src.{domain}.schema")
    mod.DOMAIN, mod.TABLES = domain, tables
    return mod


DAEMONS = _schema("daemons", {
    "daemons_allocs": {"kinds": {"allocation": AllocV1},
                       "indexes": {"start_at": "REAL"}},
})
WEB = _schema("web", {
    "web_notes": {"kinds": {"note": NoteRec},
                  "indexes": {"key": "TEXT"}, "unique": ["key"]},
})


class SchemaValidationTests(unittest.TestCase):
    def test_table_prefix_enforced(self):
        bad = _schema("daemons", {"jobs": {"kinds": {"x": AllocV1}}})
        with self.assertRaises(BusDeclarationError):
            validate_schema(bad)

    def test_index_field_must_be_declared(self):
        bad = _schema("daemons", {"daemons_t": {"kinds": {"x": AllocV1},
                                                "indexes": {"nope": "TEXT"}}})
        with self.assertRaises(BusDeclarationError):
            validate_schema(bad)

    def test_unique_requires_index(self):
        bad = _schema("daemons", {"daemons_t": {"kinds": {"x": AllocV1},
                                                "unique": ["start_at"]}})
        with self.assertRaises(BusDeclarationError):
            validate_schema(bad)


class StorageHubTests(unittest.IsolatedAsyncioTestCase):
    async def test_auto_create_and_ownership(self):
        hub = StorageHub(_db())
        wd, ww = hub.mount(DAEMONS), hub.mount(WEB)
        await hub.start()
        try:
            rid = await wd.insert("daemons_allocs", "allocation",
                                  AllocV1(task_type="news_digest", start_at=100.0))
            self.assertEqual(rid, 1)
            with self.assertRaises(StoreError):     # 越权:web 写 daemons 的表
                await ww.insert("daemons_allocs", "allocation",
                                AllocV1(task_type="x", start_at=1.0))
            with self.assertRaises(StoreError):     # 未声明的 kind
                await wd.insert("daemons_allocs", "ghost",
                                AllocV1(task_type="x", start_at=1.0))
            with self.assertRaises(StoreError):     # 形状不符
                await wd.insert("daemons_allocs", "allocation",
                                NoteRec(key="k"))
        finally:
            await hub.close()

    async def test_incremental_across_restart(self):
        """重启后声明新表:新表出现、旧表数据原样——无感自增量。"""
        path = _db()
        hub1 = StorageHub(path)
        wd = hub1.mount(DAEMONS)
        await hub1.start()
        await wd.insert("daemons_allocs", "allocation",
                        AllocV1(task_type="news_digest", start_at=42.0))
        await hub1.close()

        hub2 = StorageHub(path)
        hub2.mount(DAEMONS)
        hub2.mount(WEB)                              # 新域携新表加入
        await hub2.start()
        try:
            old = await hub2.read("daemons_allocs")
            self.assertEqual(len(old), 1)
            self.assertEqual(old[0]["data"]["start_at"], 42.0)
            snap = await hub2.snapshot()
            self.assertIn("web_notes", snap)         # 新表自动出现
            self.assertEqual(snap["web_notes"]["rows"], 0)
        finally:
            await hub2.close()

    async def test_field_evolution_old_rows_get_defaults(self):
        """v1 写入的老行,用 v2 契约读出:新字段走默认值,不迁移。"""
        path = _db()
        hub = StorageHub(path)
        wd = hub.mount(DAEMONS)
        await hub.start()
        await wd.insert("daemons_allocs", "allocation",
                        AllocV1(task_type="t", start_at=1.0))
        rows = await hub.read("daemons_allocs")
        evolved = AllocV2.model_validate(rows[0]["data"])
        self.assertEqual(evolved.priority, 5)
        await hub.close()

    async def test_promoted_column_query_and_unique(self):
        hub = StorageHub(_db())
        wd, ww = hub.mount(DAEMONS), hub.mount(WEB)
        await hub.start()
        try:
            for t in (10.0, 20.0, 30.0):
                await wd.insert("daemons_allocs", "allocation",
                                AllocV1(task_type="d", start_at=t))
            hit = await hub.read("daemons_allocs", where="start_at < ?", params=(25.0,),
                                 order_by="start_at ASC")
            self.assertEqual([r["data"]["start_at"] for r in hit], [10.0, 20.0])
            # 查询计划确认走了提升列索引
            plan = hub._conn.execute(
                "EXPLAIN QUERY PLAN SELECT * FROM daemons_allocs WHERE start_at < 25"
            ).fetchone()
            self.assertIn("idx_daemons_allocs_start_at", str(plan))
            # 唯一约束(经提升列的唯一索引)
            await ww.insert("web_notes", "note", NoteRec(key="k1"))
            with self.assertRaises(StoreError):
                await ww.insert("web_notes", "note", NoteRec(key="k1"))
        finally:
            await hub.close()

    async def test_envelope_drift_raises(self):
        """实表不符合信封规范 → 挂载即抛错,绝不自动改表。"""
        path = _db()
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE daemons_allocs (id INTEGER PRIMARY KEY, foo TEXT)")
        conn.commit(); conn.close()
        hub = StorageHub(path)
        hub.mount(DAEMONS)
        with self.assertRaises(StoreError):
            await hub.start()

    async def test_update_and_delete_lifecycle(self):
        """复刻分配生命周期:入库 → 到点消费删除;update 校验行存在与形状。"""
        hub = StorageHub(_db())
        wd = hub.mount(DAEMONS)
        await hub.start()
        try:
            rid = await wd.insert("daemons_allocs", "allocation",
                                  AllocV1(task_type="news_digest", start_at=1.0))
            await wd.update("daemons_allocs", rid,
                            AllocV1(task_type="news_digest", start_at=2.0))
            rows = await hub.read("daemons_allocs", where="id = ?", params=(rid,))
            self.assertEqual(rows[0]["data"]["start_at"], 2.0)
            with self.assertRaises(StoreError):      # 不存在的行
                await wd.update("daemons_allocs", 999,
                                AllocV1(task_type="x", start_at=1.0))
            self.assertEqual(await wd.delete("daemons_allocs", rid), 1)
            self.assertEqual(await hub.read("daemons_allocs"), [])
        finally:
            await hub.close()


if __name__ == "__main__":
    unittest.main()
