"""总线地基级投递语义验证:落盘、崩溃重投、重试、死信、背压不丢弃。

语义目标:"发出去 = 一定会被处理到,或显式进死信"。
"""

from __future__ import annotations

import asyncio
import types
import unittest
import uuid
from pathlib import Path

from atelier_core.core import Bus, BusError, Envelope, Message
from atelier_core.core.journal import Journal

TMP = Path("tmp")


class Msg(Message):
    seq: int


def _header(domain: str, publishes: dict, subscribes: dict,
            ephemeral: set | None = None) -> types.ModuleType:
    mod = types.ModuleType(f"dur.{domain}")
    mod.DOMAIN, mod.PUBLISHES, mod.SUBSCRIBES = domain, publishes, subscribes
    if ephemeral:
        mod.EPHEMERAL = ephemeral
    return mod


def _db() -> str:
    TMP.mkdir(exist_ok=True)
    return str(TMP / f"dur_test_{uuid.uuid4().hex[:8]}.db")


class DurabilityTests(unittest.IsolatedAsyncioTestCase):
    async def _journal(self, path: str) -> Journal:
        j = Journal(path)
        await j.start()
        return j

    async def test_publish_is_persisted_before_return(self):
        """publish 返回时投递账已落盘(pending),与内存状态无关。"""
        path = _db()
        j = await self._journal(path)
        bus = Bus(journal=j)
        win = bus.mount(_header("p", {"p/evt/x": Msg}, {}))
        bus.mount(_header("s", {}, {"p/evt/x": "h"}),
                  handlers={"h": lambda env: asyncio.sleep(0)})
        # 故意不 start():消息只进账本,不被消费
        await win.publish("p/evt/x", Msg(seq=1))
        counts = await j.counts()
        self.assertEqual(counts["pending"], 1, "publish 返回前必须已落盘")
        await bus.close(); await j.close()

    async def test_crash_recovery_redelivers(self):
        """崩溃重投:第一进程发布后未处理即'崩溃',第二进程启动自动补投。"""
        path = _db()
        j1 = await self._journal(path)
        bus1 = Bus(journal=j1)
        w1 = bus1.mount(_header("p", {"p/evt/x": Msg}, {}))
        bus1.mount(_header("s", {}, {"p/evt/x": "h"}),
                   handlers={"h": lambda env: asyncio.sleep(0)})
        for k in range(5):
            await w1.publish("p/evt/x", Msg(seq=k))
        await bus1.close(); await j1.close()      # 未 start 即"崩溃":0 条被处理

        got: list[int] = []
        done = asyncio.Event()

        async def h(env: Envelope) -> None:
            got.append(env.payload.seq)
            if len(got) == 5:
                done.set()

        j2 = await self._journal(path)
        bus2 = Bus(journal=j2)
        bus2.mount(_header("p", {"p/evt/x": Msg}, {}))
        bus2.mount(_header("s", {}, {"p/evt/x": "h"}), handlers={"h": h})
        await bus2.start()
        await asyncio.wait_for(done.wait(), timeout=5)
        self.assertEqual(got, [0, 1, 2, 3, 4], "重投必须按原序")
        counts = await j2.counts()
        self.assertEqual(counts["pending"], 0)
        self.assertEqual(counts["done"], 5)
        await bus2.close(); await j2.close()

    async def test_retry_then_success(self):
        """前两次失败第三次成功:自动退避重试,最终销账,不进死信。"""
        path = _db()
        j = await self._journal(path)
        bus = Bus(journal=j, max_attempts=5)
        win = bus.mount(_header("p", {"p/evt/x": Msg}, {}))
        calls = 0
        done = asyncio.Event()

        async def flaky(env: Envelope) -> None:
            nonlocal calls
            calls += 1
            if calls < 3:
                raise RuntimeError(f"第{calls}次失败")
            done.set()

        bus.mount(_header("s", {}, {"p/evt/x": "h"}), handlers={"h": flaky})
        await bus.start()
        await win.publish("p/evt/x", Msg(seq=1))
        await asyncio.wait_for(done.wait(), timeout=10)
        counts = await j.counts()
        self.assertEqual(calls, 3)
        self.assertEqual(bus.stats["retried"], 2)
        self.assertEqual(counts["done"], 1)
        self.assertEqual(counts["dead"], 0)
        await bus.close(); await j.close()

    async def test_dead_letter_and_replay(self):
        """重试超限进死信(可查);replay 后重投,处理器修好即销账。"""
        path = _db()
        j = await self._journal(path)
        bus = Bus(journal=j, max_attempts=2)
        win = bus.mount(_header("p", {"p/evt/x": Msg}, {}))
        broken = True
        done = asyncio.Event()

        async def h(env: Envelope) -> None:
            if broken:
                raise RuntimeError("坏的")
            done.set()

        bus.mount(_header("s", {}, {"p/evt/x": "h"}), handlers={"h": h})
        await bus.start()
        await win.publish("p/evt/x", Msg(seq=7))
        for _ in range(100):
            if bus.stats["dead_letters"] == 1:
                break
            await asyncio.sleep(0.05)
        dead = await j.dead_letters()
        self.assertEqual(len(dead), 1)
        self.assertEqual(dead[0]["topic"], "p/evt/x")
        self.assertEqual(dead[0]["attempts"], 2)

        broken = False                               # "修好"处理器
        await bus.close()
        bus2 = Bus(journal=j)                        # 重启:replay 后 pending 被恢复流程重投
        bus2.mount(_header("p", {"p/evt/x": Msg}, {}))
        bus2.mount(_header("s", {}, {"p/evt/x": "h"}), handlers={"h": h})
        replayed = await j.replay(dead[0]["delivery_id"])
        self.assertIsNotNone(replayed)
        await bus2.start()
        await asyncio.wait_for(done.wait(), timeout=5)
        counts = await j.counts()
        self.assertEqual(counts["dead"], 0)
        self.assertEqual(counts["done"], 1)
        await bus2.close(); await j.close()

    async def test_dead_letter_discard(self):
        """死信的另一个终点:discard 销账(置 done),计数归零可被内务回收。"""
        path = _db()
        j = await self._journal(path)
        bus = Bus(journal=j, max_attempts=1)
        win = bus.mount(_header("p", {"p/evt/x": Msg}, {}))

        async def always_broken(env: Envelope) -> None:
            raise RuntimeError("永远修不好——业务上决定作废")

        bus.mount(_header("s", {}, {"p/evt/x": "h"}), handlers={"h": always_broken})
        await bus.start()
        await win.publish("p/evt/x", Msg(seq=1))
        for _ in range(100):
            if bus.stats["dead_letters"] == 1:
                break
            await asyncio.sleep(0.05)
        dead = await j.dead_letters()
        self.assertEqual(len(dead), 1)

        self.assertTrue(await j.discard(dead[0]["delivery_id"]))
        self.assertFalse(await j.discard(dead[0]["delivery_id"]), "重复销账不命中")
        counts = await j.counts()
        self.assertEqual(counts["dead"], 0)
        self.assertEqual(counts["done"], 1)
        self.assertEqual(await j.purge_done(retention_sec=0), 1,
                         "销账后的消息可被内务回收")
        await bus.close(); await j.close()

    async def test_dead_letter_online_replay(self):
        """死信在线重放:总线不重启,replay_dead 后消息重投,处理器修好即销账。"""
        path = _db()
        j = await self._journal(path)
        bus = Bus(journal=j, max_attempts=1)
        win = bus.mount(_header("p", {"p/evt/x": Msg}, {}))
        broken = True
        done = asyncio.Event()

        async def h(env: Envelope) -> None:
            if broken:
                raise RuntimeError("坏的")
            done.set()

        bus.mount(_header("s", {}, {"p/evt/x": "h"}), handlers={"h": h})
        await bus.start()
        await win.publish("p/evt/x", Msg(seq=9))
        for _ in range(100):
            if bus.stats["dead_letters"] == 1:
                break
            await asyncio.sleep(0.05)
        dead = await j.dead_letters()
        self.assertEqual(len(dead), 1)

        broken = False                                   # 修好处理器,在线重放
        self.assertTrue(await bus.replay_dead(dead[0]["delivery_id"]))
        await asyncio.wait_for(done.wait(), timeout=5)
        await asyncio.sleep(0.1)
        counts = await j.counts()
        self.assertEqual(counts["dead"], 0)
        self.assertEqual(counts["done"], 1)
        self.assertFalse(await bus.replay_dead(dead[0]["delivery_id"]),
                         "已销账的投递不可再重放")
        await bus.close(); await j.close()

    async def test_backpressure_no_loss(self):
        """队列 8 + 慢消费者 + 500 条洪峰:全部送达,零丢失(发布方被背压)。"""
        n = 500
        bus = Bus(queue_size=8)                      # 无 journal:纯内存也必须不丢
        win = bus.mount(_header("p", {"p/evt/x": Msg}, {}))
        got = 0
        done = asyncio.Event()

        async def slow(env: Envelope) -> None:
            nonlocal got
            await asyncio.sleep(0.001)
            got += 1
            if got == n:
                done.set()

        bus.mount(_header("s", {}, {"p/evt/x": "h"}), handlers={"h": slow})
        await bus.start()
        for k in range(n):
            await win.publish("p/evt/x", Msg(seq=k))
        await asyncio.wait_for(done.wait(), timeout=15)
        self.assertEqual(got, n)
        self.assertEqual(bus.stats["delivered"], n)
        await bus.close()

    async def test_enqueue_timeout_raises_loudly(self):
        """消费者卡死 + 队列满 → 入队超时抛显式 BusError,而非无声丢弃;
        持久消息已在账,重启后可达。"""
        path = _db()
        j = await self._journal(path)
        bus = Bus(journal=j, queue_size=1, put_timeout=0.3)
        win = bus.mount(_header("p", {"p/evt/x": Msg}, {}))
        stuck = asyncio.Event()

        async def dead_handler(env: Envelope) -> None:
            await stuck.wait()                       # 永远卡住

        bus.mount(_header("s", {}, {"p/evt/x": "h"}), handlers={"h": dead_handler})
        await bus.start()
        await win.publish("p/evt/x", Msg(seq=0))     # 被消费任务取走后卡住
        await asyncio.sleep(0.05)
        await win.publish("p/evt/x", Msg(seq=1))     # 占满 queue(size=1)
        with self.assertRaises(BusError):
            await win.publish("p/evt/x", Msg(seq=2))  # 背压超时 → 显式抛错
        counts = await j.counts()
        self.assertEqual(counts["pending"], 3, "三条都已在账,重启后必达")
        stuck.set()
        await bus.close(); await j.close()

    async def test_ephemeral_skips_journal(self):
        """EPHEMERAL topic 不落账本(快路径),持久 topic 落账本。"""
        path = _db()
        j = await self._journal(path)
        bus = Bus(journal=j)
        win = bus.mount(_header("p", {"p/evt/dur": Msg, "p/evt/tele": Msg},
                                {}, ephemeral={"p/evt/tele"}))
        bus.mount(_header("s", {}, {"p/#": "h"}),
                  handlers={"h": lambda env: asyncio.sleep(0)})
        await bus.start()
        await win.publish("p/evt/dur", Msg(seq=1))
        await win.publish("p/evt/tele", Msg(seq=2))
        await asyncio.sleep(0.2)
        counts = await j.counts()
        self.assertEqual(counts["pending"] + counts["done"], 1, "只有持久 topic 记账")
        await bus.close(); await j.close()


if __name__ == "__main__":
    unittest.main()
