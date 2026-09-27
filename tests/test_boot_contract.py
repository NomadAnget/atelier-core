"""引导契约(docs/boot.md):子进程的网关地址由拉起方显式给出,不从配置文件自取。

防的是这个真实故障(2026-09-27 生产):组装点用 `--config user-config.toml` 启动、网关在 8790,
而子进程命令行不带 --config、`config.load()` 回落 ./config.toml 读到 8793 → 连错端口、
发布出闸送不到、任务卡 pending。
"""

from __future__ import annotations

import unittest
import uuid
from pathlib import Path

from atelier_core.boot.assembly import build_supervisor_and_gateway
from atelier_core.boot.supervisor import alloc_ports
from atelier_core.core import config

TMP = Path("tmp")


def _cfg(body: str):
    TMP.mkdir(exist_ok=True)
    p = TMP / f"boot_test_{uuid.uuid4().hex[:8]}.toml"
    p.write_text(body, encoding="utf-8")
    return config.load(str(p))


def _worker_cmds(sup):
    return [sp["cmd"] for sp in sup._specs if sp["name"].endswith("-worker")]


class AllocPortsTests(unittest.TestCase):
    def test_batch_unique(self):
        """同批分配必须互不相同 —— 逐个 bind/close 会让内核把刚释放的口再发一次。"""
        ports = alloc_ports(12)
        self.assertEqual(len(ports), 12)
        self.assertEqual(len(set(ports)), 12, f"同批出现重复端口: {ports}")

    def test_zero(self):
        self.assertEqual(alloc_ports(0), [])


class GatewayBootstrapTests(unittest.TestCase):
    """网关端口的确定与传递。用本仓自身当域根(0 个 process 域)+ 显式构造验证。"""

    def test_default_port_is_dynamic(self):
        """默认必须是 0(动态)。固定默认值会让多变体并行全撞同一个口(实录)。"""
        self.assertEqual(_cfg("").gateway.port, 0)

    def test_cross_machine_requires_fixed_port(self):
        """声明 remote_domains(跨机连入)却不给端口 → 显式失败(铁律#4),不静默动态分配。

        跨机接入方无从得知动态口,静默分配等于让它永远连不上。
        """
        bad = _cfg('[gateway]\nenabled = true\nport = 0\nremote_domains = ["ext"]\n')
        with self.assertRaises(ValueError) as cm:
            build_supervisor_and_gateway(bad, {}, "atelier_core")
        self.assertIn("remote_domains", str(cm.exception))


class GatewayAddrPassedToChildTests(unittest.TestCase):
    """**本契约的核心**:网关地址必须出现在 process 域子进程的命令行里。

    造一个最小 process 域(build.py 声明 KIND="process"),看 assembly 生成的 cmd。
    """

    def setUp(self):
        import shutil, sys
        self.root = TMP / f"domroot_{uuid.uuid4().hex[:8]}"
        d = self.root / "probe"
        d.mkdir(parents=True)
        (self.root / "__init__.py").write_text("", encoding="utf-8")
        (d / "__init__.py").write_text("", encoding="utf-8")
        (d / "header.py").write_text(                     # DOMAIN/PUBLISHES/SUBSCRIBES 三件套必需
            'DOMAIN = "probe"\nPUBLISHES = {}\nSUBSCRIBES = {}\n', encoding="utf-8")
        (d / "build.py").write_text(
            'KIND = "process"\nPROCESS = {"lazy": True}\n\ndef build(ctx):\n    return None\n',
            encoding="utf-8")
        sys.path.insert(0, str(TMP.resolve()))
        self.addCleanup(lambda: shutil.rmtree(self.root, ignore_errors=True))
        self.addCleanup(lambda: sys.path.remove(str(TMP.resolve()))
                        if str(TMP.resolve()) in sys.path else None)

    def test_cmd_carries_gateway_addr(self):
        cfg = _cfg("[gateway]\nenabled = true\n")            # port 留空 → 动态
        sup, gw = build_supervisor_and_gateway(cfg, {}, self.root.name)
        self.assertIsNotNone(gw, "发现 process 域时应建网关")
        cmds = _worker_cmds(sup)
        self.assertEqual(len(cmds), 1, f"应为 probe 域登记一个 worker,实得 {cmds}")
        cmd = cmds[0]
        self.assertEqual(cmd[1:3], ["-m", "atelier_core.boot.remote"])
        addr = cmd[-1]
        self.assertIn(":", addr, f"末位应是 host:port 形态,实得 {addr!r}")
        self.assertEqual(int(addr.rsplit(":", 1)[1]), gw._port,
                         "子进程 cmd 里的端口必须 == 网关实际监听的端口")

    def test_child_uses_passed_addr_not_config(self):
        """子进程宿主签名接受 gateway_addr,且优先于 cfg —— 防回归到读配置。"""
        import inspect
        from atelier_core.boot.composition import run_domain_subprocess
        sig = inspect.signature(run_domain_subprocess)
        self.assertIn("gateway_addr", sig.parameters)
        src = inspect.getsource(run_domain_subprocess)
        self.assertIn("gateway_addr or", src, "应为『传入优先,cfg 仅兜底』")


if __name__ == "__main__":
    unittest.main()
