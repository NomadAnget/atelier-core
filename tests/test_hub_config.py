"""基建配置验证:默认值、文件解析、显式失败(未知节/键、类型不符)。"""

from __future__ import annotations

import unittest
import uuid
from pathlib import Path

from atelier_core.core import config
from atelier_core.core.config import ConfigError

TMP = Path("tmp")


def _write(body: str) -> str:
    TMP.mkdir(exist_ok=True)
    p = TMP / f"cfg_test_{uuid.uuid4().hex[:8]}.toml"
    p.write_text(body, encoding="utf-8")
    return str(p)


class HubConfigTests(unittest.TestCase):
    def test_missing_file_all_defaults(self):
        cfg = config.load("tmp/不存在的配置.toml")
        self.assertEqual(cfg.bus.queue_size, 4096)
        self.assertEqual(cfg.paths.store_db, "data/hub/store.db")
        self.assertIn("缺省值", cfg.source)

    def test_repo_sample_parses(self):
        cfg = config.load("config.toml")           # 仓库里的样例必须永远可解析
        self.assertEqual(cfg.source, "config.toml")
        self.assertEqual(cfg.web.port, 8000)
        self.assertEqual(cfg.sentinel.heartbeat_grace, 3.0)

    def test_partial_override(self):
        p = _write("[bus]\nqueue_size = 128\n")
        cfg = config.load(p)
        self.assertEqual(cfg.bus.queue_size, 128)
        self.assertEqual(cfg.bus.max_attempts, 3)     # 未覆盖的键保持默认

    def test_int_promotes_to_float(self):
        p = _write("[bus]\nput_timeout = 5\n")        # TOML 整数 → float 字段
        self.assertEqual(config.load(p).bus.put_timeout, 5.0)

    def test_unknown_section_raises(self):
        p = _write("[runtime]\nvoice = \"x\"\n")      # 运行时配置不许进基建文件
        with self.assertRaises(ConfigError):
            config.load(p)

    def test_unknown_key_raises(self):
        p = _write("[bus]\nqueu_size = 1\n")          # 打字错误保护
        with self.assertRaises(ConfigError):
            config.load(p)

    def test_wrong_type_raises(self):
        p = _write("[logger]\nlevel = 3\n")
        with self.assertRaises(ConfigError):
            config.load(p)


if __name__ == "__main__":
    unittest.main()
