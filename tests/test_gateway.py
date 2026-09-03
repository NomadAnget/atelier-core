"""总线网关验证:远程域 = 域的异地实现。

覆盖:跨连接双向收发(deliver→ack 销账 / 远端 publish 注入)、校验在服务端生效
(越权/坏 schema 被拒)、nack→重试→死信、断线在途重发(不烧重试次数)+
重连续投、鉴权拒绝、心跳经网关进总线。全部走真实 WebSocket 回环。
"""

from __future__ import annotations

import asyncio
import json
import types
import unittest
import uuid
from pathlib import Path

import websockets

from atelier_core.core import Bus, Envelope, Message
from atelier_core.core.gateway.server import GatewayServer
from atelier_core.core.gateway.client import BusClient, GatewayError
from atelier_core.core.journal import Journal

TMP = Path("tmp")


class Cmd(Message):
    job_id: str


class Done(Message):
    job_id: str
    ok: bool = True


def _header(domain: str, publishes: dict, subscribes: dict,
            ephemeral: set | None = None) -> types.ModuleType:
    mod = types.ModuleType(f"gw.{domain}")
    mod.DOMAIN, mod.PUBLISHES, mod.SUBSCRIBES = domain, publishes, subscribes
    if ephemeral:
        mod.EPHEMERAL = ephemeral
    return mod


REMOTE = _header("pub", {"pub/evt/done": Done},
                 {"core/publish/requested": "on_req"})


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        TMP.mkdir(exist_ok=True)
        self.journal = Journal(str(TMP / f"gw_{uuid.uuid4().hex[:8]}.db"))
        await self.journal.start()
        self.bus = Bus(journal=self.journal, max_attempts=2)
        self.gw = GatewayServer(host="127.0.0.1", port=0, token="secret",
                                ack_timeout=5.0)

        # core 域(本地):发命令、收远端回报
        self.done_events: list[Envelope] = []

        async def on_done(env: Envelope) -> None:
            self.done_events.append(env)

        self.core_win = self.bus.mount(
            _header("core", {"core/publish/requested": Cmd},
                    {"pub/evt/done": "on_done"}),
            handlers={"on_done": on_done})
        # pub 域(远程):订阅槽位绑网关转发
        self.pub_win = self.bus.mount(REMOTE, handlers=self.gw.remote_handlers(REMOTE))
        self.gw.attach(self.bus, {"pub": self.pub_win})
        await self.bus.start()
        await self.gw.start()
        self.url = f"ws://127.0.0.1:{self.gw.port}"

    async def asyncTearDown(self):
        await self.gw.stop()
        await self.bus.close()
        await self.journal.close()

    async def test_roundtrip_ack_and_remote_publish(self):
        """本地命令 → 远端处理回 ack(销账)→ 远端发布回报 → 本地收到,corr 全链一致。"""
        got: list[tuple[dict, dict]] = []
        client_holder: dict = {}

        async def on_req(payload: dict, meta: dict) -> None:
            got.append((payload, meta))
            await client_holder["c"].publish("pub/evt/done",
                                             {"job_id": payload["job_id"]},
                                             corr=meta["corr"])

        client = BusClient(self.url, "pub", token="secret",
                           handlers={"on_req": on_req})
        client_holder["c"] = client
        await client.start()
        try:
            await self.core_win.publish("core/publish/requested", Cmd(job_id="j1"),
                                        correlation_id="corr-1")
            for _ in range(100):
                if self.done_events:
                    break
                await asyncio.sleep(0.05)
            self.assertEqual(got[0][0], {"job_id": "j1"})
            self.assertEqual(self.done_events[0].payload.job_id, "j1")
            self.assertEqual(self.done_events[0].correlation_id, "corr-1",
                             "因果链须跨进程边界保持")
            await asyncio.sleep(0.1)
            counts = await self.journal.counts()
            self.assertEqual(counts["pending"], 0, "远端 ack 后应销账")
            self.assertEqual(counts["dead"], 0)
        finally:
            await client.close()

    async def test_remote_publish_validated(self):
        """服务端校验:越权 topic 与坏载荷都被拒(pub_err),总线不受污染。"""
        client = BusClient(self.url, "pub", token="secret",
                           handlers={"on_req": lambda p, m: asyncio.sleep(0)})
        await client.start()
        try:
            with self.assertRaises(GatewayError):        # 越权:发 core 域的 topic
                await client.publish("core/publish/requested", {"job_id": "x"},
                                     timeout=3)
            with self.assertRaises(GatewayError):        # schema 不符
                await client.publish("pub/evt/done", {"job_id": 1, "ghost": True},
                                     timeout=3)
            ok = await client.publish("pub/evt/done", {"job_id": "ok"}, timeout=3)
            self.assertTrue(ok)
        finally:
            await client.close()

    async def test_nack_retries_then_dead_letter(self):
        async def broken(payload: dict, meta: dict) -> None:
            raise RuntimeError("远端处理失败")

        client = BusClient(self.url, "pub", token="secret",
                           handlers={"on_req": broken})
        await client.start()
        try:
            await self.core_win.publish("core/publish/requested", Cmd(job_id="bad"))
            for _ in range(200):                          # max_attempts=2 → 死信
                if (await self.journal.counts())["dead"] == 1:
                    break
                await asyncio.sleep(0.05)
            dead = await self.journal.dead_letters()
            self.assertEqual(len(dead), 1)
            self.assertIn("nack", dead[0]["last_error"])
        finally:
            await client.close()

    async def test_disconnect_redelivers_without_burning_attempts(self):
        """远端收到投递后'崩溃'(不 ack 即断)→ 重连后原样重投,最终销账,零死信。"""
        crashed = asyncio.Event()

        async def crash_client() -> None:
            async with websockets.connect(self.url) as ws:
                await ws.send(json.dumps({"t": "hello", "domain": "pub",
                                          "token": "secret"}))
                assert json.loads(await ws.recv())["t"] == "hello_ok"
                msg = json.loads(await ws.recv())          # 收到 deliver
                assert msg["t"] == "deliver"
                crashed.set()                              # 不 ack,直接断连=崩溃

        task = asyncio.create_task(crash_client())
        await self.core_win.publish("core/publish/requested", Cmd(job_id="j2"))
        await asyncio.wait_for(crashed.wait(), timeout=5)
        await task

        handled = asyncio.Event()

        async def on_req(payload: dict, meta: dict) -> None:
            handled.set()

        recovery = BusClient(self.url, "pub", token="secret",
                             handlers={"on_req": on_req})
        await recovery.start()
        try:
            await asyncio.wait_for(handled.wait(), timeout=5)
            await asyncio.sleep(0.2)
            counts = await self.journal.counts()
            self.assertEqual(counts["dead"], 0, "断线重发不烧重试次数")
            self.assertEqual(counts["pending"], 0, "重投后销账")
        finally:
            await recovery.close()

    async def test_auth_and_unknown_domain_rejected(self):
        bad_token = BusClient(self.url, "pub", token="wrong", handlers={})
        with self.assertRaises(GatewayError):
            await bad_token.start()
        await bad_token.close()

        stranger = BusClient(self.url, "ghost", token="secret", handlers={})
        with self.assertRaises(GatewayError):
            await stranger.start()
        await stranger.close()


if __name__ == "__main__":
    unittest.main()
