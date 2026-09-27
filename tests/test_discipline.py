"""atelier_core.discipline —— 宪法静态纪律(铁律 4 跨域 import / 铁律 5 引擎分级与不常驻)的单元测试。

每个用例在临时目录里造一棵最小假域根(header.py + 若干模块),断言扫描器报出(或不报)对应违规。
扫描器纯 AST,不执行假域代码,所以假模块里 import torch 之类不需要真装。
"""

from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from atelier_core.discipline import check


def _tree(files: dict[str, str]) -> Path:
    root = Path(tempfile.mkdtemp()) / "src"
    for rel, body in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(textwrap.dedent(body), encoding="utf-8")
    return root


_H = 'DOMAIN = "{d}"\nPUBLISHES = {{}}\nSUBSCRIBES = {{}}\n'


def _hdr(d: str, extra: str = "") -> str:
    return _H.format(d=d) + textwrap.dedent(extra)


def _rules(root: Path) -> list[str]:
    return [f"{v.rule}|{v.where}|{v.detail}" for v in check(root).violations]


class ImportDisciplineTests(unittest.TestCase):
    def test_header_schema_always_allowed(self):
        root = _tree({"a/header.py": _hdr("a"), "a/schema.py": "DOMAIN = \"a\"\nTABLES = {}\n",
                      "b/header.py": _hdr("b"),
                      "b/impl/x.py": "from ...a import header\nfrom ...a.schema import T\n"})
        self.assertEqual(_rules(root), [])

    def test_impl_rejected(self):
        root = _tree({"a/header.py": _hdr("a"), "a/impl/svc.py": "",
                      "b/header.py": _hdr("b"), "b/impl/x.py": "from ...a.impl import svc\n"})
        r = _rules(root)
        self.assertEqual(len(r), 1)
        self.assertIn("M6|b/impl/x.py|越界 import a.impl.svc", r[0])

    def test_public_module_and_name(self):
        root = _tree({"a/header.py": _hdr("a", 'PUBLIC = ("registry", "Store")\n'),
                      "a/registry.py": "", "a/__init__.py": "from .impl.s import Store\n",
                      "b/header.py": _hdr("b"),
                      "b/impl/x.py": "from ...a.registry import JobCancelled\nfrom ...a import Store\n"})
        self.assertEqual(_rules(root), [])

    def test_reexport_without_declaration_rejected(self):
        """域根 __init__ 转出 impl 里的东西但未声明 PUBLIC → 照样越界(堵转手漏洞)。"""
        root = _tree({"a/header.py": _hdr("a"), "a/__init__.py": "from .impl.s import Store\n",
                      "b/header.py": _hdr("b"), "b/impl/x.py": "from ...a import Store\n"})
        self.assertTrue(any("越界 import a.Store" in x for x in _rules(root)))

    def test_star_public(self):
        root = _tree({"eng/header.py": _hdr("eng", 'PUBLIC = ("*",)\n'), "eng/media/ff.py": "",
                      "b/header.py": _hdr("b"), "b/impl/x.py": "from ...eng.media import ff\n"})
        self.assertEqual(_rules(root), [])

    def test_pipelines_bound_by_public_too(self):
        root = _tree({"a/header.py": _hdr("a"), "a/impl/svc.py": "",
                      "pipelines/video/steps.py": "from ...a.impl import svc\n"})
        self.assertTrue(any(x.startswith("M6|pipelines/video/steps.py") for x in _rules(root)))

    def test_absolute_root_import_resolved(self):
        root = _tree({"a/header.py": _hdr("a"), "a/impl/svc.py": "",
                      "b/header.py": _hdr("b"), "b/impl/x.py": "from src.a.impl import svc\n"})
        self.assertTrue(any("越界 import a.impl.svc" in x for x in _rules(root)))

    def test_reserved_and_non_domain_ignored(self):
        root = _tree({"services/tool/x.py": "from ...a.impl import svc\n",
                      "a/header.py": _hdr("a"), "a/impl/svc.py": ""})
        self.assertEqual(_rules(root), [])


class EngineTierTests(unittest.TestCase):
    def _eng(self, extra_header: str = "", **files: str) -> Path:
        base = {"eng/header.py": _hdr("eng", 'PUBLIC = ("*",)\n' + extra_header)}
        base.update({k.replace("__", "/") + ".py": v for k, v in files.items()})
        return _tree(base)

    def test_framework_module_needs_level(self):
        root = self._eng(eng__m__vad="import torch\ndef f():\n    pass\n")
        self.assertTrue(any("eng/m/vad.py" in x and "没有声明级别的引擎类" in x for x in _rules(root)))

    def test_declared_levels_pass(self):
        root = self._eng(eng__gpu="import torch\nclass G:\n    HEAVY_INFERENCE = True\n",
                         eng__cpu="import spacy\nclass C:\n    CPU_ONLY = True\n    RAM_GB = 0.5\n")
        self.assertEqual(_rules(root), [])

    def test_helper_whitelist_and_stale(self):
        root = self._eng('FRAMEWORK_HELPERS = {"common": "张量工具", "gone": "已删"}\n',
                         eng__common="import torch\n")
        r = _rules(root)
        self.assertEqual(len(r), 1)
        self.assertIn("FRAMEWORK_HELPERS 登记的 gone 已不存在", r[0])

    def test_level_conflicts(self):
        root = self._eng(eng__x="import torch\nclass A:\n    HEAVY_INFERENCE = True\n    CPU_ONLY = True\n    RAM_GB = 1\n"
                                "class B:\n    CPU_ONLY = True\n")
        r = " ".join(_rules(root))
        self.assertIn("A 同时声明 HEAVY_INFERENCE 与 CPU_ONLY", r)
        self.assertIn("B 为 CPU_ONLY 但未声明 RAM_GB>0", r)

    def test_resident_state_detected(self):
        root = self._eng(eng__sp="import spacy\n_cache = {}\nclass S:\n    CPU_ONLY = True\n    RAM_GB = 1\n"
                                 "    @classmethod\n    def get(cls):\n        cls._m = 1\n"
                                 "def load(k):\n    _cache[k] = 1\n")
        r = " ".join(_rules(root))
        self.assertIn("_cache(item-assign)", r)
        self.assertIn("_m(class-attr)", r)

    def test_state_allowed_and_stale(self):
        root = self._eng('STATE_ALLOWED = {("t", "_TABLE"): "静态表", ("t", "_OLD"): "已删"}\n',
                         eng__t="_TABLE = None\ndef g():\n    global _TABLE\n    _TABLE = 1\n",
                         eng__e="import torch\nclass E:\n    HEAVY_INFERENCE = True\n")
        r = _rules(root)
        self.assertEqual(len(r), 1)
        self.assertIn("('t', '_OLD') 代码已不再改写", r[0])

    def test_resident_check_only_for_model_domains(self):
        """无引擎类/辅助模块的普通域,其服务状态不受不常驻规则约束。"""
        root = _tree({"web/header.py": _hdr("web"), "web/impl/s.py": "_x = {}\ndef f():\n    _x[1] = 2\n"})
        self.assertEqual(_rules(root), [])

    def test_gpu_import_only_own_domain_and_pipelines(self):
        root = self._eng(eng__g="import torch\nclass G:\n    HEAVY_INFERENCE = True\n",
                         eng__c="import torch\nclass C:\n    CPU_ONLY = True\n    RAM_GB = 1\n",
                         eng__user="from . import g\n")
        root = _tree({**{str(p.relative_to(root)): p.read_text() for p in root.rglob("*.py")},
                      "web/header.py": _hdr("web"),
                      "web/impl/s.py": "from ...eng import g\nfrom ...eng import c\n",
                      "pipelines/v/step.py": "from ...eng import g\n"})
        r = _rules(root)
        self.assertEqual(len(r), 1)
        self.assertIn("M8|web/impl/s.py|import GPU 引擎 eng.g", r[0])

    def test_vendored_skipped(self):
        root = self._eng('VENDORED = ("m/third",)\n', eng__m__third__x="import torch\ndef f():\n    global Y\n")
        self.assertEqual(_rules(root), [])

    def test_non_literal_declaration_fails_loud(self):
        root = _tree({"a/header.py": _hdr("a", "PUBLIC = tuple(['x'])\n")})
        with self.assertRaises(ValueError):
            check(root)


class DeclarationAndCodeRuleTests(unittest.TestCase):
    def test_m2_topic_prefix(self):
        root = _tree({"a/header.py": "from atelier_core.core.contracts import Message\n"
                                     "class E(Message):\n    x: int = 0\n"
                                     'DOMAIN = "a"\nPUBLISHES = {"b/evt/done": E}\nSUBSCRIBES = {}\n'})
        self.assertTrue(any(x.startswith("M2|a/header.py") for x in _rules(root)))

    def test_m1_domain_mismatch(self):
        root = _tree({"a/header.py": _hdr("zzz")})
        self.assertTrue(any(x.startswith("M1|a/header.py") and "不一致" in x for x in _rules(root)))

    def test_m3_table_prefix(self):
        root = _tree({"a/header.py": _hdr("a"),
                      "a/schema.py": 'DOMAIN = "a"\nTABLES = {"b_items": {"kinds": {}}}\n'})
        self.assertTrue(any(x.startswith("M3|a/schema.py") for x in _rules(root)))

    def test_m4_header_function(self):
        root = _tree({"a/header.py": _hdr("a", "def helper():\n    return 1\n")})
        self.assertTrue(any(x.startswith("M4|a/header.py") for x in _rules(root)))

    def test_m10_print_everywhere(self):
        root = _tree({"a/header.py": _hdr("a"), "a/__main__.py": "print('hi')\n",
                      "pipelines/v/step.py": "def f():\n    print(1)\n"})
        r = _rules(root)
        self.assertTrue(any(x.startswith("M10|a/__main__.py:1") for x in r))
        self.assertTrue(any(x.startswith("M10|pipelines/v/step.py:2") for x in r))

    def test_m11_platform_branch(self):
        root = _tree({"a/header.py": _hdr("a"),
                      "a/x.py": "import os, sys, platform\n"
                                "A = 'x.exe' if os.name == 'nt' else 'x'\n"
                                "B = sys.platform.startswith('win')\nC = platform.system()\n"})
        self.assertEqual(sum(x.startswith("M11|a/x.py") for x in _rules(root)), 3)


class ModeDetectionTests(unittest.TestCase):
    def test_domain_mode_standalone_repo(self):
        """域仓单独检出:仓目录名带连字符、DOMAIN 取自 header;impl 越界照判,公开件判定与 M8 报跳过。"""
        repo = Path(tempfile.mkdtemp()) / "atelier-eng"
        files = {"header.py": _hdr("eng", 'PUBLIC = ("*",)\n'),
                 "g.py": "import torch\nclass G:\n    HEAVY_INFERENCE = True\n",
                 "impl/x.py": "from ...web.impl import svc\nfrom ...scheduler import registry\n",
                 "__main__.py": "print(1)\n"}
        for rel, body in files.items():
            p = repo / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(body, encoding="utf-8")
        rep = check(repo)
        self.assertEqual(rep.mode, "domain")
        rules = sorted(f"{v.rule}|{v.where}" for v in rep.violations)
        self.assertEqual(rules, ["M10|eng/__main__.py:1", "M6|eng/impl/x.py"])
        self.assertTrue(any(s.startswith("M6 1 处") for s in rep.skipped))
        self.assertTrue(any(s.startswith("M8 本域 1 个") for s in rep.skipped))

    def test_domain_mode_header_importing_other_domain_is_skipped(self):
        repo = Path(tempfile.mkdtemp()) / "atelier-b"
        repo.mkdir(parents=True)
        (repo / "header.py").write_text("from ..web import header as _w\n" + _hdr("b"), encoding="utf-8")
        rep = check(repo)
        self.assertEqual(rep.violations, [])
        self.assertTrue(any(s.startswith("M1/M2 b/header.py") for s in rep.skipped))

    def test_pipeline_mode(self):
        repo = Path(tempfile.mkdtemp()) / "atelier-pipeline-video"
        for rel, body in {"plugin.py": "", "steps/s.py": "from ....engines.impl import x\nprint(1)\n"}.items():
            p = repo / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(body, encoding="utf-8")
        rep = check(repo)
        self.assertEqual(rep.mode, "pipeline")
        rules = sorted(v.rule for v in rep.violations)
        self.assertEqual(rules, ["M10", "M6"])

    def test_variant_mode(self):
        root = _tree({"a/header.py": _hdr("a")})
        self.assertEqual(check(root).mode, "variant")


if __name__ == "__main__":
    unittest.main()
