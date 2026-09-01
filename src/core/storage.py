"""存储枢纽 —— 无感自增量数据库(信封表规范)。

设计(与消息总线完全对称):
  · 表归属随目录走:域只能写自己 schema.py 声明的表(StoreWindow 机器校验);
    读自由——任何域可查任何表(hub.read)。
  · 统一信封列(永久冻结):id / kind / data(JSON) / created_at / updated_at。
    业务字段全部住在 data 里,形状由域声明的 Record(pydantic)契约化,
    写入时强校验;字段演进=只增带默认值的字段,永不改列。
  · 提升列:schema 里声明 indexes 的 data 字段,自动执行
    ALTER TABLE ... ADD COLUMN <f> GENERATED ALWAYS AS (json_extract(data,'$.<f>')) VIRTUAL
    并建索引——ADD COLUMN 是唯一天然安全的 ALTER,故可自动化;
    unique 声明生成唯一索引。
  · 漂移检测:实表信封列缺失/结构不符 → 挂载即抛错(报警自动化,动刀人工化)。
  · 并发语义:写串行(asyncio.Lock + 线程执行,不占事件循环);读并行
    (WAL,多只读连接,不持写锁);snapshot 同读路径。事件循环零阻塞。

至此"无感自增量"覆盖三层:建表自动、字段自动(JSON)、索引自动(生成列)。
其他节点新增/演进存储,永远不需要改本文件。
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any

from . import logger
from .contracts import Record, secret_field_names, settings_ui_spec
from .registry import validate_schema

_ENVELOPE = ("id", "kind", "data", "created_at", "updated_at")


class StoreError(Exception):
    """存储使用错误(越权写/未声明表或 kind/载荷不符/唯一冲突/漂移)。"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class StoreWindow:
    """域的唯一写窗口:写权受本域 schema 声明约束;读经 hub.read(自由)。"""

    def __init__(self, hub: "StorageHub", domain: str, tables: dict[str, dict]) -> None:
        self._hub = hub
        self._domain = domain
        self._tables = tables

    @property
    def domain(self) -> str:
        return self._domain

    def _model(self, table: str, kind: str) -> type[Record]:
        spec = self._tables.get(table)
        if spec is None:
            owner = self._hub.owner_of(table)
            raise StoreError(
                f"{self._domain}: 越权写表 '{table}'"
                + (f"(拥有域为 {owner})" if owner else "(未声明)"))
        model = spec["kinds"].get(kind)
        if model is None:
            raise StoreError(f"{self._domain}: 表 '{table}' 未声明 kind '{kind}'")
        return model

    def _validate(self, table: str, kind: str, record: Record | dict) -> Record:
        model = self._model(table, kind)
        if isinstance(record, dict):
            return model.model_validate(record)
        if not isinstance(record, model):
            raise StoreError(
                f"{self._domain}: '{table}.{kind}' 载荷类型 {type(record).__name__} 不符,"
                f"契约要求 {model.__name__}")
        return record

    def _typed_spec(self, table: str) -> dict:
        spec = self._tables.get(table)
        if spec is None:
            owner = self._hub.owner_of(table)
            raise StoreError(
                f"{self._domain}: 越权写表 '{table}'"
                + (f"(拥有域为 {owner})" if owner else "(未声明)"))
        if not spec.get("typed"):
            raise StoreError(f"{self._domain}: '{table}' 是信封表,请用 insert/update")
        return spec

    async def insert_row(self, table: str, values: dict[str, Any]) -> int:
        """类型化表写入一行(列白名单校验),返回 id。"""
        spec = self._typed_spec(table)
        unknown = values.keys() - spec["columns"].keys()
        if unknown:
            raise StoreError(f"{self._domain}: '{table}' 未声明的列 {sorted(unknown)}")
        cols = list(values)
        sql = (f"INSERT INTO {table} ({', '.join(cols)}) "
               f"VALUES ({', '.join('?' * len(cols))})")
        rid = await self._hub._execute_rowid(sql, tuple(values[c] for c in cols))
        # 与信封路径同权:类型化写入同样广播存储事件(曾漏——前端 SSE 收不到
        # daemons_channels/scheduler_jobs 等类型化表的写,登记频道后页面不自刷)
        self._hub._notify("insert", table, self._domain, rid)
        return rid

    async def update_row(self, table: str, row_id: int, values: dict[str, Any]) -> None:
        """类型化表按列更新;行不存在即抛错。"""
        spec = self._typed_spec(table)
        unknown = values.keys() - spec["columns"].keys()
        if unknown:
            raise StoreError(f"{self._domain}: '{table}' 未声明的列 {sorted(unknown)}")
        sets = ", ".join(f"{c} = ?" for c in values)
        n = await self._hub._execute(
            f"UPDATE {table} SET {sets} WHERE id = ?", (*values.values(), row_id))
        if n == 0:
            raise StoreError(f"{self._domain}: '{table}' 无 id={row_id} 的行")
        self._hub._notify("update", table, self._domain, row_id)

    async def insert(self, table: str, kind: str, record: Record | dict) -> int:
        """信封表写入一行,返回 id。"""
        spec_ = self._tables.get(table)
        if spec_ is not None and spec_.get("typed"):
            raise StoreError(f"{self._domain}: '{table}' 是类型化表,请用 insert_row")
        rec = self._validate(table, kind, record)
        now = _now_iso()
        rid = await self._hub._execute_rowid(
            f"INSERT INTO {table} (kind, data, created_at, updated_at) VALUES (?,?,?,?)",
            (kind, rec.model_dump_json(), now, now))
        self._hub._notify("insert", table, self._domain, rid)
        return rid

    async def update(self, table: str, row_id: int, record: Record | dict) -> None:
        """整体替换某行的 data(按该行 kind 的契约校验);行不存在即抛错。"""
        rows = await self._hub.read(table, where="id = ?", params=(row_id,), limit=1)
        if not rows:
            raise StoreError(f"{self._domain}: '{table}' 无 id={row_id} 的行")
        rec = self._validate(table, rows[0]["kind"], record)
        await self._hub._execute(
            f"UPDATE {table} SET data = ?, updated_at = ? WHERE id = ?",
            (rec.model_dump_json(), _now_iso(), row_id))
        self._hub._notify("update", table, self._domain, row_id)

    async def delete(self, table: str, row_id: int) -> int:
        """删除一行,返回删除行数(写权同 insert)。"""
        self._require_own(table)
        n = await self._hub._execute(
            f"DELETE FROM {table} WHERE id = ?", (row_id,))
        if n:
            self._hub._notify("delete", table, self._domain, row_id)
        return n

    def _require_own(self, table: str) -> None:
        if table not in self._tables:
            owner = self._hub.owner_of(table)
            raise StoreError(
                f"{self._domain}: 越权写表 '{table}'"
                + (f"(拥有域为 {owner})" if owner else "(未声明)"))


class StorageHub:
    """存储枢纽本体:挂载各域 schema → 自增量建表/提升列 → 发写窗口。"""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()
        self._owners: dict[str, str] = {}          # table → domain
        self._specs: dict[str, dict] = {}          # table → spec
        self._windows: dict[str, StoreWindow] = {}
        self._observers: list = []                 # 写操作观察者(观测台用)

    def add_observer(self, fn) -> None:
        """注册写操作观察者:每次 insert/update/delete 后收到摘要 record。"""
        self._observers.append(fn)

    def _notify(self, op: str, table: str, domain: str, row_id: int) -> None:
        import time as _time
        record = {"ts": _time.time(), "op": op, "table": table,
                  "domain": domain, "row_id": row_id}
        for fn in self._observers:
            try:
                fn(record)
            except Exception as exc:  # noqa: BLE001 — 观察者故障不影响写入
                logger.error("存储观察者异常 err=%s", exc,
                             layer="CORE", component="StorageHub")

    # ── 挂载与建库 ────────────────────────────────────────────────────────────

    def mount(self, schema: ModuleType) -> StoreWindow:
        """挂载一个域的存储声明,返回其写窗口(建表在 start() 统一执行)。"""
        domain, tables = validate_schema(schema)
        if domain in self._windows:
            raise StoreError(f"域 '{domain}' 的存储已挂载")
        for table in tables:
            if table in self._owners:
                raise StoreError(f"表 '{table}' 已被域 '{self._owners[table]}' 声明")
            self._owners[table] = domain
        self._specs.update(tables)
        win = StoreWindow(self, domain, tables)
        self._windows[domain] = win
        return win

    async def start(self) -> None:
        """开库并自增量对齐:缺表建表、缺提升列加列建索引、信封漂移抛错。"""
        def _sync_start() -> list[str]:
            Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            # 多进程语义:task 执行进程与主进程共写同一 store.db(WAL 跨进程
            # 多读者+串行写者)——跨进程写锁竞争必须是等待而非 SQLITE_BUSY 立抛
            conn.execute("PRAGMA busy_timeout=8000")
            actions: list[str] = []
            for table, spec in self._specs.items():
                actions += StorageHub._maybe_rename(conn, table, spec)
                actions += self._ensure_table(conn, table, spec)
            conn.commit()
            self._conn = conn
            return actions
        actions = await asyncio.to_thread(_sync_start)
        for a in actions:
            logger.info("存储枢纽:%s", a, layer="CORE", component="StorageHub")
        logger.info("存储枢纽就绪 db=%s 表=%d", self._db_path, len(self._specs),
                    layer="CORE", component="StorageHub")

    @staticmethod
    def _maybe_rename(conn: sqlite3.Connection, table: str, spec: dict) -> list[str]:
        """声明式改名迁移(renamed_from):新名缺、旧名在 → ALTER RENAME,数据原地保留;
        新旧同在 → 显式报错(不猜哪份是真相,人工裁决)。旧名前缀的索引一并清理
        (改名后 _ensure_* 会按新名重建索引,旧名索引留着是重复负担)。"""
        old = spec.get("renamed_from") or ""
        if not old:
            return []

        def _exists(name: str) -> bool:
            return conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (name,)).fetchone() is not None

        if not _exists(old):
            return []
        if _exists(table):
            raise StoreError(
                f"表改名冲突:'{old}' 与 '{table}' 同时存在——无法判定真相源,"
                "需人工迁移(不自动改表)")
        conn.execute(f"ALTER TABLE {old} RENAME TO {table}")
        dropped = 0
        for (idx_name,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? "
                "AND name LIKE ?", (table, f"idx_{old}_%")).fetchall():
            conn.execute(f"DROP INDEX {idx_name}")
            dropped += 1
        return [f"改名迁移 {old} → {table}(数据原地保留,清旧索引 {dropped})"]

    @staticmethod
    def _ensure_table(conn: sqlite3.Connection, table: str, spec: dict) -> list[str]:
        if spec.get("typed"):
            return StorageHub._ensure_typed(conn, table, spec)
        actions: list[str] = []
        # table_xinfo 而非 table_info:后者不列出生成列(hidden),会导致重复 ALTER
        cols = {r[1] for r in conn.execute(f"PRAGMA table_xinfo({table})")}
        if not cols:
            conn.execute(
                f"CREATE TABLE {table} ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, "
                "data TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
            cols = set(_ENVELOPE)
            actions.append(f"建表 {table}")
        else:
            missing = set(_ENVELOPE) - cols
            if missing:
                raise StoreError(
                    f"表 '{table}' 信封漂移:缺列 {sorted(missing)}——"
                    "结构不符合信封规范,需人工迁移(不自动改表)")
        for field, sql_type in spec["indexes"].items():
            if field not in cols:
                conn.execute(
                    f"ALTER TABLE {table} ADD COLUMN {field} {sql_type} "
                    f"GENERATED ALWAYS AS (json_extract(data, '$.{field}')) VIRTUAL")
                actions.append(f"提升列 {table}.{field}({sql_type})")
            uniq = "UNIQUE " if field in spec["unique"] else ""
            conn.execute(
                f"CREATE {uniq}INDEX IF NOT EXISTS idx_{table}_{field} ON {table}({field})")
        return actions

    @staticmethod
    def _ensure_typed(conn: sqlite3.Connection, table: str, spec: dict) -> list[str]:
        """类型化表对齐:缺表建表;缺列且可安全追加(非 NOT-NULL-无默认)则自动加列。"""
        actions: list[str] = []
        cols = {r[1] for r in conn.execute(f"PRAGMA table_xinfo({table})")}
        if not cols:
            defs = ", ".join(f"{n} {frag}" for n, frag in spec["columns"].items())
            conn.execute(
                f"CREATE TABLE {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, {defs})")
            actions.append(f"建表 {table}(类型化)")
        else:
            for n, frag in spec["columns"].items():
                if n in cols:
                    continue
                up = frag.upper()
                if "NOT NULL" in up and "DEFAULT" not in up:
                    raise StoreError(
                        f"表 '{table}' 缺列 '{n}' 且声明 NOT NULL 无默认——"
                        "无法安全自动加列,需人工迁移(不自动改表)")
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {n} {frag}")
                actions.append(f"加列 {table}.{n}")
        for col in spec["indexes"]:
            uniq = "UNIQUE " if col in spec["unique"] else ""
            conn.execute(
                f"CREATE {uniq}INDEX IF NOT EXISTS idx_{table}_{col} ON {table}({col})")
        return actions

    def attach_kernel_table(self, table: str, columns: dict[str, str],
                            indexes: list[str] | None = None,
                            unique: list[str] | None = None) -> None:
        """内核直管表(如 hub_logs):枢纽负责建表/读/快照;写由内核组件
        (自持连接的写线程)完成,不经任何域窗口。须在 start() 前挂接。"""
        if self._conn is not None:
            raise StoreError("内核表必须在 start() 前挂接")
        if table in self._specs:
            raise StoreError(f"表 '{table}' 已声明")
        self._owners[table] = "kernel"
        self._specs[table] = {"typed": True, "columns": dict(columns),
                              "indexes": list(indexes or []), "unique": list(unique or [])}

    async def close(self) -> None:
        if self._conn is not None:
            await asyncio.to_thread(self._conn.close)
            self._conn = None

    # ── 读(自由)与执行 ──────────────────────────────────────────────────────

    def owner_of(self, table: str) -> str | None:
        return self._owners.get(table)

    def table_info(self, table: str) -> dict | None:
        """表的公开元信息(web 通用查询端点的白名单来源);未声明返回 None。

        filterable = 可安全用于等值过滤/排序的列:id + 声明过索引的列
        (信封表另含 kind)——全部有真索引,查询不落全表扫。"""
        spec = self._specs.get(table)
        if spec is None:
            return None
        typed = bool(spec.get("typed"))
        filterable = ["id", *(spec["indexes"] if typed else spec["indexes"].keys())]
        if not typed:
            filterable.append("kind")
        return {"type": "typed" if typed else "envelope",
                "owner": self._owners[table],
                "filterable": filterable}

    def secret_fields(self, table: str) -> set[str]:
        """该 settings 表标了 ``secret=True`` 的字段名(密钥脱敏用);非 settings 表返回空集。
        web 通用查询端点据此把密钥值遮罩成占位符,明文绝不出服务端。"""
        spec = self._specs.get(table)
        if not spec or spec.get("typed"):
            return set()
        model = (spec.get("kinds") or {}).get("settings")
        return secret_field_names(model) if model else set()

    async def read(self, table: str, *, kind: str | None = None, where: str = "",
                   params: tuple = (), order_by: str | None = None,
                   limit: int | None = None) -> list[dict[str, Any]]:
        """读自由:任何域可查任何已声明的表。where 可用提升列或信封列。"""
        spec = self._specs.get(table)
        if spec is None:
            raise StoreError(f"读未声明的表 '{table}'")
        if spec.get("typed"):
            if kind is not None:
                raise StoreError(f"类型化表 '{table}' 无 kind 维度")
            return await self._read_typed(table, spec, where, params, order_by, limit)
        sql = f"SELECT id, kind, data, created_at, updated_at FROM {table}"
        conds, args = [], list(params)
        if kind is not None:
            conds.append("kind = ?"); args.insert(0, kind)
        if where:
            conds.append(f"({where})")
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"

        def _q() -> list[dict]:
            with self._read_conn() as rc:
                rows = rc.execute(sql, args).fetchall()
            return [{"id": r[0], "kind": r[1], "data": json.loads(r[2]),
                     "created_at": r[3], "updated_at": r[4]} for r in rows]
        # 读不持写锁:WAL 下读者与写者并行,读读之间各持独立只读连接,零争抢
        return await asyncio.to_thread(_q)

    async def distinct(self, table: str, column: str) -> list[Any]:
        """某列的去重取值集(升序,剔 NULL)。读自由;列限 filterable 白名单
        (id/kind/声明过索引的列——信封表索引列是提升列,同样可查),零注入面。
        用途:前端筛选器动态取值(如日志来源清单),不硬编码不漏历史值。"""
        info = self.table_info(table)
        if info is None:
            raise StoreError(f"读未声明的表 '{table}'")
        if column not in info["filterable"]:
            raise StoreError(
                f"'{column}' 不可取值集(可用列:{info['filterable']})")
        sql = (f"SELECT DISTINCT {column} FROM {table} "
               f"WHERE {column} IS NOT NULL ORDER BY {column}")

        def _q() -> list[Any]:
            with self._read_conn() as rc:
                return [r[0] for r in rc.execute(sql).fetchall()]
        return await asyncio.to_thread(_q)

    async def _read_typed(self, table: str, spec: dict, where: str, params: tuple,
                          order_by: str | None, limit: int | None) -> list[dict[str, Any]]:
        cols = ["id", *spec["columns"].keys()]
        sql = f"SELECT {', '.join(cols)} FROM {table}"
        if where:
            sql += f" WHERE ({where})"
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"

        def _q() -> list[dict]:
            with self._read_conn() as rc:
                rows = rc.execute(sql, list(params)).fetchall()
            return [dict(zip(cols, r)) for r in rows]
        return await asyncio.to_thread(_q)

    def _read_conn(self) -> sqlite3.Connection:
        """只读连接(每次读独立开):WAL 允许任意多读者与单写者并行。"""
        if self._conn is None:
            raise StoreError("存储枢纽未启动(先 await start())")
        rc = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        rc.execute("PRAGMA busy_timeout=8000")   # 跨进程检查点等待而非立抛
        return rc

    async def kernel_purge(self, table: str, where: str, params: tuple) -> int:
        """内核表专用清理(内务循环用);对域表调用抛错——域数据归域管。"""
        if self._owners.get(table) != "kernel":
            raise StoreError(f"kernel_purge 仅限内核表('{table}' 归 {self._owners.get(table)})")
        return await self._execute(f"DELETE FROM {table} WHERE {where}", params)

    async def snapshot(self) -> dict:
        """内省:表 → 拥有域/提升列/行数(观测台用)。只读连接,线程中执行。"""
        def _snap() -> dict:
            out = {}
            with self._read_conn() as rc:
                for table, spec in sorted(self._specs.items()):
                    n = rc.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    out[table] = {"owner": self._owners[table],
                                  "type": "typed" if spec.get("typed") else "envelope",
                                  "kinds": sorted(spec["kinds"]) if not spec.get("typed")
                                           else sorted(spec["columns"]),
                                  "indexes": sorted(spec["indexes"]),
                                  "rows": n}
                    # 设置表:附上从 Record 声明提取的字段 UI 规格 + 域中文名(前端设置页据此渲染,
                    # 元数据不再前后端分散,见 contracts.Setting)。
                    if not spec.get("typed") and "settings" in spec["kinds"]:
                        model = spec["kinds"]["settings"]
                        out[table]["settings_fields"] = settings_ui_spec(model)
                        out[table]["settings_label"] = getattr(model, "SECTION_LABEL", "")
            return out
        return await asyncio.to_thread(_snap)

    async def _execute(self, sql: str, params: tuple) -> int:
        def _run() -> int:
            assert self._conn is not None
            try:
                cur = self._conn.execute(sql, params)
                self._conn.commit()
                return cur.rowcount
            except sqlite3.IntegrityError as exc:
                raise StoreError(f"完整性冲突:{exc}") from exc
        async with self._lock:
            return await asyncio.to_thread(_run)

    async def _execute_rowid(self, sql: str, params: tuple) -> int:
        def _run() -> int:
            assert self._conn is not None
            try:
                cur = self._conn.execute(sql, params)
                self._conn.commit()
                return int(cur.lastrowid or 0)
            except sqlite3.IntegrityError as exc:
                raise StoreError(f"完整性冲突:{exc}") from exc
        async with self._lock:
            return await asyncio.to_thread(_run)
