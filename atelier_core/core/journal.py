"""总线持久账本(journal)—— "发出去 = 落盘了"。

投递保证的实现基座:
  · publish 返回前,消息本体 + 每个匹配订阅的一条投递账(pending)已在同一事务内落盘;
  · 处理成功 → mark_done 销账;处理失败 → bump_attempts,超限 → 死信(dead);
  · 进程崩溃 → 重启时 pending_for() 取回全部未销账投递,重新入队 —— at-least-once,
    处理器必须幂等(msg_id 恒定,可作去重键);
  · 死信可查(dead_letters)可重放(replay);全部销账且过保留期的消息由 purge 清理。

写路径与存储枢纽同模式:asyncio.Lock 串行 + 线程执行,事件循环零阻塞。
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
from pathlib import Path
from typing import Any

_SCHEMA = """
CREATE TABLE IF NOT EXISTS bus_journal (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic TEXT NOT NULL,
  source TEXT NOT NULL,
  payload TEXT NOT NULL,
  msg_id TEXT NOT NULL,
  corr TEXT NOT NULL,
  created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS bus_deliveries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  journal_id INTEGER NOT NULL,
  subscriber TEXT NOT NULL,             -- "域:槽位"
  status TEXT NOT NULL DEFAULT 'pending',   -- pending | done | dead
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT NOT NULL DEFAULT '',
  updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_deliv_pending ON bus_deliveries(subscriber, status, journal_id);
CREATE INDEX IF NOT EXISTS idx_deliv_journal ON bus_deliveries(journal_id);
"""


class Journal:
    """持久账本:journal(消息本体)+ deliveries(每订阅投递账)。"""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        def _open() -> None:
            Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(_SCHEMA)
            conn.commit()
            self._conn = conn
        await asyncio.to_thread(_open)

    async def close(self) -> None:
        if self._conn is not None:
            await asyncio.to_thread(self._conn.close)
            self._conn = None

    async def _run(self, fn):
        async with self._lock:
            return await asyncio.to_thread(fn)

    # ── 写入(publish 关键路径:单事务 = 消息 + 全部投递账)────────────────────

    async def append(self, topic: str, source: str, payload_json: str,
                     msg_id: str, corr: str, subscribers: list[str]) -> int:
        def _tx() -> int:
            assert self._conn is not None
            now = time.time()
            cur = self._conn.execute(
                "INSERT INTO bus_journal (topic, source, payload, msg_id, corr, created_at) "
                "VALUES (?,?,?,?,?,?)", (topic, source, payload_json, msg_id, corr, now))
            jid = int(cur.lastrowid or 0)
            self._conn.executemany(
                "INSERT INTO bus_deliveries (journal_id, subscriber, updated_at) VALUES (?,?,?)",
                [(jid, s, now) for s in subscribers])
            self._conn.commit()
            return jid
        return await self._run(_tx)

    # ── 销账 / 重试 / 死信 ───────────────────────────────────────────────────

    async def mark_done(self, journal_id: int, subscriber: str) -> None:
        def _tx() -> None:
            assert self._conn is not None
            self._conn.execute(
                "UPDATE bus_deliveries SET status='done', updated_at=? "
                "WHERE journal_id=? AND subscriber=? AND status='pending'",
                (time.time(), journal_id, subscriber))
            self._conn.commit()
        await self._run(_tx)

    async def bump_attempts(self, journal_id: int, subscriber: str, err: str,
                            max_attempts: int) -> tuple[int, bool]:
        """失败一次:attempts+1;达到上限则转死信。返回 (attempts, 是否死信)。"""
        def _tx() -> tuple[int, bool]:
            assert self._conn is not None
            row = self._conn.execute(
                "SELECT attempts FROM bus_deliveries WHERE journal_id=? AND subscriber=?",
                (journal_id, subscriber)).fetchone()
            attempts = (row[0] if row else 0) + 1
            dead = attempts >= max_attempts
            self._conn.execute(
                "UPDATE bus_deliveries SET attempts=?, last_error=?, status=?, updated_at=? "
                "WHERE journal_id=? AND subscriber=?",
                (attempts, err[:500], "dead" if dead else "pending", time.time(),
                 journal_id, subscriber))
            self._conn.commit()
            return attempts, dead
        return await self._run(_tx)

    # ── 恢复 / 观测 / 重放 ───────────────────────────────────────────────────

    async def max_id(self) -> int:
        """当前账本最大消息号(启动水位线:恢复只补投此线以下的遗留)。"""
        def _q() -> int:
            assert self._conn is not None
            row = self._conn.execute("SELECT COALESCE(MAX(id), 0) FROM bus_journal").fetchone()
            return int(row[0])
        return await self._run(_q)

    async def pending_for(self, subscriber: str) -> list[dict[str, Any]]:
        """某订阅的全部未销账投递(按消息序),重启重投用。"""
        def _q() -> list[dict]:
            assert self._conn is not None
            rows = self._conn.execute(
                "SELECT j.id, j.topic, j.source, j.payload, j.msg_id, j.corr "
                "FROM bus_deliveries d JOIN bus_journal j ON j.id = d.journal_id "
                "WHERE d.subscriber=? AND d.status='pending' ORDER BY j.id ASC",
                (subscriber,)).fetchall()
            return [{"journal_id": r[0], "topic": r[1], "source": r[2],
                     "payload": r[3], "msg_id": r[4], "corr": r[5]} for r in rows]
        return await self._run(_q)

    async def dead_letters(self, limit: int = 100) -> list[dict[str, Any]]:
        def _q() -> list[dict]:
            assert self._conn is not None
            rows = self._conn.execute(
                "SELECT d.id, j.topic, d.subscriber, d.attempts, d.last_error, j.payload, j.corr "
                "FROM bus_deliveries d JOIN bus_journal j ON j.id = d.journal_id "
                "WHERE d.status='dead' ORDER BY d.updated_at DESC LIMIT ?",
                (limit,)).fetchall()
            return [{"delivery_id": r[0], "topic": r[1], "subscriber": r[2],
                     "attempts": r[3], "last_error": r[4], "payload": r[5],
                     "corr": r[6]} for r in rows]
        return await self._run(_q)

    async def replay(self, delivery_id: int) -> dict[str, Any] | None:
        """死信重置为 pending(attempts 清零),返回可重投的消息;不存在返回 None。"""
        def _tx() -> dict | None:
            assert self._conn is not None
            row = self._conn.execute(
                "SELECT d.journal_id, d.subscriber, j.topic, j.source, j.payload, j.msg_id, j.corr "
                "FROM bus_deliveries d JOIN bus_journal j ON j.id = d.journal_id "
                "WHERE d.id=? AND d.status='dead'", (delivery_id,)).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "UPDATE bus_deliveries SET status='pending', attempts=0, updated_at=? WHERE id=?",
                (time.time(), delivery_id))
            self._conn.commit()
            return {"journal_id": row[0], "subscriber": row[1], "topic": row[2],
                    "source": row[3], "payload": row[4], "msg_id": row[5], "corr": row[6]}
        return await self._run(_tx)

    async def discard(self, delivery_id: int) -> bool:
        """死信销账:确认作废(修复已另行完成/消息已过时),置 done 交内务回收。
        与 replay 互为死信的两个终点:重放=再试一次,销账=盖棺。返回是否命中。"""
        def _tx() -> bool:
            assert self._conn is not None
            cur = self._conn.execute(
                "UPDATE bus_deliveries SET status='done', updated_at=? "
                "WHERE id=? AND status='dead'", (time.time(), delivery_id))
            self._conn.commit()
            return cur.rowcount > 0
        return await self._run(_tx)

    async def pending_stats(self) -> dict[str, float]:
        """未销账投递统计:{count, oldest_age_sec}——滞留告警(卡住的消息)用。"""
        def _q() -> dict[str, float]:
            assert self._conn is not None
            row = self._conn.execute(
                "SELECT COUNT(*), MIN(j.created_at) FROM bus_deliveries d "
                "JOIN bus_journal j ON j.id = d.journal_id WHERE d.status='pending'"
            ).fetchone()
            n, oldest = row[0], row[1]
            return {"count": float(n),
                    "oldest_age_sec": (time.time() - oldest) if oldest else 0.0}
        return await self._run(_q)

    async def counts(self) -> dict[str, int]:
        def _q() -> dict[str, int]:
            assert self._conn is not None
            out = {"pending": 0, "done": 0, "dead": 0}
            for st, n in self._conn.execute(
                    "SELECT status, COUNT(*) FROM bus_deliveries GROUP BY status"):
                out[st] = n
            return out
        return await self._run(_q)

    async def purge_done(self, retention_sec: float = 7 * 86400) -> int:
        """清理:全部投递均已 done 且过保留期的消息(连带投递账)。返回清理消息数。"""
        def _tx() -> int:
            assert self._conn is not None
            cutoff = time.time() - retention_sec
            rows = self._conn.execute(
                "SELECT j.id FROM bus_journal j WHERE j.created_at < ? "
                "AND NOT EXISTS (SELECT 1 FROM bus_deliveries d "
                "  WHERE d.journal_id = j.id AND d.status != 'done')",
                (cutoff,)).fetchall()
            ids = [r[0] for r in rows]
            if ids:
                q = ",".join("?" * len(ids))
                self._conn.execute(f"DELETE FROM bus_deliveries WHERE journal_id IN ({q})", ids)
                self._conn.execute(f"DELETE FROM bus_journal WHERE id IN ({q})", ids)
                self._conn.commit()
            return len(ids)
        return await self._run(_tx)
