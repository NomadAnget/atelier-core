"""第一级 · 机器宪法的静态执法(docs/CONSTITUTION.md「宪法分两级」,编号 M1–M11 中可静态判定者)。

宪法:**项目级宪法校验一律在内核库里**。域不自带宪法校验,只在自己的 ``header.py`` 里声明
(纯字面量,按 AST 读、不执行):

- ``PUBLIC``            本域允许被跨域 import 的公开件(域根下的模块名或名字;``"*"`` = 整体公开)  → M6
- ``FRAMEWORK_HELPERS`` ``{模块: 理由}`` import 推理框架但不载模型的辅助模块                     → M7
- ``STATE_ALLOWED``     ``{(模块, 名字): 理由}`` 允许函数改写的非模型模块/类级状态               → M9
- ``VENDORED``          随域携带的第三方代码目录(域内相对路径),免扫

头文件/存储规则(M1–M3)**复用**组装期的同一份校验(``registry.validate_header`` / ``storage.validate_schema``),
不另写一份;为此会 import 各域的 header/schema(只依赖内核与 pydantic,不触发重依赖)。

**自动识别路径**(一条命令,变体 CI 与域仓 CI 通用)::

    .venv/bin/python -m atelier_core.discipline <路径>

- 路径下有 ``header.py``   → 单域模式(域仓 CI);
- 路径下有 ``plugin.py``   → 单管线模式(管线仓 CI);
- 否则                      → 变体域根(变体 CI,如 ``src``),扫全部域 + ``pipelines/``。

单域/单管线模式看不到其他域:需要全局视野的判定(公开件是否声明、GPU 引擎被谁 import)报"跳过",
不算通过也不算失败。有违规 → 退出码 1。第二级行为规范(B1–B7)机器判不了,须人工对照宪法。
"""

from __future__ import annotations

import ast
import importlib
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from .core.registry import RESERVED_DIRS, BusDeclarationError, validate_header

# 推理框架:import 其一 = 该模块会(或能)载模型,须分级(M7)。
FRAMEWORKS = frozenset({
    "torch", "torchaudio", "torchvision", "paddle", "paddleocr", "onnxruntime", "tensorrt",
    "transformers", "diffusers", "ctranslate2", "faster_whisper", "nemo", "pyannote",
    "speechbrain", "wtpsplit", "spacy", "demucs", "sentence_transformers", "silero_vad",
    "rapidocr_onnxruntime",
})
_LEVEL_KEYS = ("HEAVY_INFERENCE", "CPU_ONLY", "RAM_GB", "VRAM_GB")
_MUTATORS = frozenset({"update", "append", "extend", "setdefault", "add", "insert", "pop", "clear",
                       "__setitem__", "popitem", "discard", "remove"})
_SKIP_DIRS = frozenset({"__pycache__", "tests", "node_modules"})
PIPELINES = "pipelines"                      # 管线插件挂载点(RESERVED,非域;可 import 任何公开件与 GPU 引擎)
_DECL_KEYS = ("PUBLIC", "FRAMEWORK_HELPERS", "STATE_ALLOWED", "VENDORED", "DOMAIN")


@dataclass(frozen=True)
class Violation:
    rule: str          # 机器宪法编号 M1…
    where: str         # 相对扫描根的位置
    detail: str

    def __str__(self) -> str:
        return f"[{self.rule}] {self.where}:{self.detail}"


@dataclass
class Report:
    mode: str                                          # variant / domain / pipeline
    root: Path
    violations: list[Violation] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)   # 本模式判不了、须由变体检查覆盖的条目

    @property
    def ok(self) -> bool:
        return not self.violations


@dataclass
class _Unit:
    """一个被扫描单元:一个域,或一个管线目录。"""
    name: str
    path: Path
    is_domain: bool
    public: tuple = ()
    helpers: dict = field(default_factory=dict)
    state_allowed: dict = field(default_factory=dict)
    vendored: tuple = ()
    modules: dict = field(default_factory=dict)       # 点分名(相对单元) → (Path, ast.Module)


# ── 读声明 / 发现单元 ────────────────────────────────────────────────────────
def _header_decls(header: Path) -> dict:
    """header 顶层赋值的纪律声明(只认字面量;非字面量 = 声明写错,显式报错)。"""
    out: dict = {}
    tree = ast.parse(header.read_text(encoding="utf-8"), filename=str(header))
    for n in tree.body:
        tgt = (n.targets[0] if isinstance(n, ast.Assign) and len(n.targets) == 1
               else n.target if isinstance(n, ast.AnnAssign) else None)
        if isinstance(tgt, ast.Name) and tgt.id in _DECL_KEYS:
            try:
                out[tgt.id] = ast.literal_eval(n.value)
            except ValueError as e:
                raise ValueError(f"{header}: {tgt.id} 必须是纯字面量(内核按 AST 读,不执行)") from e
    return out


def _domain_unit(d: Path, name: str | None = None) -> _Unit:
    decl = _header_decls(d / "header.py")
    return _Unit(
        name=name or d.name, path=d, is_domain=True,
        public=tuple(decl.get("PUBLIC", ())),
        helpers=dict(decl.get("FRAMEWORK_HELPERS", {})),
        state_allowed={tuple(k): v for k, v in dict(decl.get("STATE_ALLOWED", {})).items()},
        vendored=tuple(decl.get("VENDORED", ())))


def _load_modules(u: _Unit) -> None:
    for p in sorted(u.path.rglob("*.py")):
        rel = p.relative_to(u.path)
        if any(part in _SKIP_DIRS or part.startswith(".") for part in rel.parts):
            continue
        if any(rel.as_posix().startswith(v.rstrip("/") + "/") for v in u.vendored):
            continue
        mod = ".".join(rel.with_suffix("").parts).removesuffix(".__init__")
        u.modules[mod or "__init__"] = (p, ast.parse(p.read_text(encoding="utf-8"), filename=str(p)))


def _detect(path: Path) -> str:
    if (path / "header.py").is_file():
        return "domain"
    if (path / "plugin.py").is_file():
        return "pipeline"
    return "variant"


# ── AST 工具 ─────────────────────────────────────────────────────────────────
def _framework_imports(tree: ast.Module) -> set[str]:
    out: set[str] = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            out.update(a.name.split(".")[0] for a in n.names)
        elif isinstance(n, ast.ImportFrom) and n.level == 0 and n.module:
            out.add(n.module.split(".")[0])
    return out & FRAMEWORKS


def _class_levels(tree: ast.Module) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for c in (n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)):
        flags: dict = {}
        for st in c.body:
            tgt = (st.targets[0] if isinstance(st, ast.Assign) and len(st.targets) == 1
                   else st.target if isinstance(st, ast.AnnAssign) else None)
            if (isinstance(tgt, ast.Name) and tgt.id in _LEVEL_KEYS
                    and isinstance(getattr(st, "value", None), ast.Constant)):
                flags[tgt.id] = st.value.value
        if flags:
            out[c.name] = flags
    return out


def _level(flags: dict) -> str | None:
    if flags.get("HEAVY_INFERENCE") is True:
        return "gpu"
    if flags.get("CPU_ONLY") is True:
        return "cpu"
    return None


def _state_writes(tree: ast.Module) -> set[tuple[str, str]]:
    """函数/方法体内对模块级或类级状态的改写 → {(名字, 形态)}(实例属性 self.x 随实例释放,不算)。"""
    mod_names: set[str] = set()
    for n in tree.body:
        targets = n.targets if isinstance(n, ast.Assign) else [n.target] if isinstance(n, ast.AnnAssign) else []
        mod_names.update(t.id for t in targets if isinstance(t, ast.Name))
    class_names = {n.name for n in ast.walk(tree) if isinstance(n, ast.ClassDef)}
    out: set[tuple[str, str]] = set()
    for fn in (n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))):
        for n in ast.walk(fn):
            if isinstance(n, ast.Global):
                out.update((x, "global") for x in n.names)
            elif isinstance(n, (ast.Assign, ast.AugAssign, ast.AnnAssign)):
                for t in (n.targets if isinstance(n, ast.Assign) else [n.target]):
                    if isinstance(t, ast.Subscript) and isinstance(t.value, ast.Name) and t.value.id in mod_names:
                        out.add((t.value.id, "item-assign"))
                    elif isinstance(t, ast.Attribute):
                        o = t.value
                        if isinstance(o, ast.Name) and (o.id == "cls" or o.id in class_names):
                            out.add((t.attr, "class-attr"))
                        elif isinstance(o, ast.Call) and isinstance(o.func, ast.Name) and o.func.id == "type":
                            out.add((t.attr, "class-attr"))
            elif (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                  and isinstance(n.func.value, ast.Name) and n.func.value.id in mod_names
                  and n.func.attr in _MUTATORS):
                out.add((n.func.value.id, f".{n.func.attr}()"))
    return out


def _print_calls(tree: ast.Module) -> list[int]:
    return [n.lineno for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "print"]


def _platform_branches(tree: ast.Module) -> list[tuple[int, str]]:
    """判平台的表达式:``os.name`` / ``sys.platform`` / ``platform.system()``(M11)。"""
    out: list[tuple[int, str]] = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name):
            if (n.value.id, n.attr) in (("os", "name"), ("sys", "platform")):
                out.append((n.lineno, f"{n.value.id}.{n.attr}"))
        elif (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
              and isinstance(n.func.value, ast.Name) and n.func.value.id == "platform"
              and n.func.attr == "system"):
            out.append((n.lineno, "platform.system()"))
    return out


def _cross_imports(pkg_parts: list[str], root_pkg: str, own: str, tree: ast.Module) -> list[tuple[str, str]]:
    """文件里指向**其他单元**的 import → [(目标单元名, 单元内点分名)]。

    ``pkg_parts`` = 文件所在包(含根包名);相对 import 按它解析,绝对 import 只认以根包名开头的。"""
    raw: list[str] = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            raw.extend(a.name for a in n.names)
        elif isinstance(n, ast.ImportFrom):
            if n.level == 0:
                if n.module:
                    raw.extend(f"{n.module}.{a.name}" for a in n.names)
            else:
                if n.level > len(pkg_parts):
                    continue                                     # 越过根(Python 也会报错),不归本规则
                anchor = pkg_parts[: len(pkg_parts) - (n.level - 1)]
                base = anchor + (n.module.split(".") if n.module else [])
                raw.extend(".".join(base + [a.name]) for a in n.names)
    out: list[tuple[str, str]] = []
    for t in raw:
        parts = t.split(".")
        if parts[0] != root_pkg or len(parts) < 2 or parts[1] == own:
            continue
        out.append((parts[1], ".".join(parts[2:])))
    return out


# ── 头文件 / 存储声明(复用组装期同一份校验)────────────────────────────────
def _why_unimportable(e: ImportError, root_pkg: str) -> str:
    """跳过原因要说准:缺的是他域(单域检出看不到)还是第三方包(当前环境没装)。"""
    name = e.name or ""
    if name == root_pkg or name.startswith(root_pkg + "."):
        return f"导入依赖他域({name.removeprefix(root_pkg + '.')}),由变体检查覆盖"
    return f"当前环境缺第三方包 {name or '?'},在装齐该域依赖的环境里跑可覆盖"


def _import_decls(root_pkg: str, unit: _Unit, report: Report) -> None:
    """import 本单元 header/schema,交 registry.validate_header / storage.validate_schema(M1/M2/M3)。
    依赖他域 header 而导入失败(单域模式)→ 记为跳过;本域声明错误 → 违规。"""
    from .core.storage import validate_schema
    where = f"{unit.name}/header.py"
    try:
        hdr = importlib.import_module(f"{root_pkg}.{unit.name}.header")
    except ImportError as e:
        report.skipped.append(f"M1/M2 {where}:{_why_unimportable(e, root_pkg)}")
        return
    try:
        validate_header(hdr, unit.name if report.mode == "variant" else None)
    except BusDeclarationError as e:
        report.violations.append(Violation("M2" if "topic" in str(e) or "Message" in str(e) else "M1",
                                           where, str(e)))
    if (unit.path / "schema.py").is_file():
        try:
            sch = importlib.import_module(f"{root_pkg}.{unit.name}.schema")
        except ImportError as e:
            report.skipped.append(f"M3 {unit.name}/schema.py:{_why_unimportable(e, root_pkg)}")
            return
        try:
            validate_schema(sch)
        except Exception as e:  # noqa: BLE001 —— StoreError 等一律是声明违规
            report.violations.append(Violation("M3", f"{unit.name}/schema.py", str(e)))


# ── 裁决 ─────────────────────────────────────────────────────────────────────
def check(path: str | Path) -> Report:
    """扫描路径(自动识别变体域根 / 单域 / 单管线),返回报告。"""
    root = Path(path).resolve()
    mode = _detect(root)
    report = Report(mode=mode, root=root)

    # 组织单元 + 可导入的根包
    tmp: tempfile.TemporaryDirectory | None = None
    if mode == "variant":
        units = [_domain_unit(d) for d in sorted(p for p in root.iterdir() if p.is_dir())
                 if d.name not in RESERVED_DIRS and not d.name.startswith((".", "_"))
                 and (d / "header.py").is_file()]
        if (root / PIPELINES).is_dir():
            units += [_Unit(name=f"{PIPELINES}/{p.name}", path=p, is_domain=False)
                      for p in sorted((root / PIPELINES).iterdir()) if p.is_dir() and not p.name.startswith((".", "_"))]
        root_pkg, import_base = root.name, root.parent
    else:
        # 单域/单管线:造一个临时根包,把本仓软链成其中的一员(仓目录名可能带连字符,不能直接 import)
        tmp = tempfile.TemporaryDirectory(prefix="atelier_disc_")
        root_pkg, import_base = "_atelier_disc_root", Path(tmp.name)
        pkg = import_base / root_pkg
        pkg.mkdir()
        (pkg / "__init__.py").write_text("", encoding="utf-8")
        if mode == "domain":
            name = str(_header_decls(root / "header.py").get("DOMAIN") or root.name)
            (pkg / name).symlink_to(root, target_is_directory=True)
            units = [_domain_unit(root, name)]
            units[0].path = pkg / name
        else:
            (pkg / PIPELINES).mkdir()
            (pkg / PIPELINES / "__init__.py").write_text("", encoding="utf-8")
            (pkg / PIPELINES / root.name).symlink_to(root, target_is_directory=True)
            units = [_Unit(name=f"{PIPELINES}/{root.name}", path=pkg / PIPELINES / root.name, is_domain=False)]
    for u in units:
        _load_modules(u)
    by_name = {u.name: u for u in units if u.is_domain}
    v = report.violations

    def _purge() -> None:                              # 同名根包(如 src)不得读到上一次扫描的缓存模块
        for k in [k for k in sys.modules if k == root_pkg or k.startswith(root_pkg + ".")]:
            del sys.modules[k]

    try:
        _purge()
        sys.path.insert(0, str(import_base))
        for u in units:
            if u.is_domain:
                _import_decls(root_pkg, u, report)
    finally:
        sys.path.remove(str(import_base))
        _purge()

    def _where(u: _Unit, p: Path) -> str:
        return f"{u.name}/{p.relative_to(u.path).as_posix()}"

    # M4 / M10 / M11:逐文件
    for u in units:
        for mod, (p, tree) in u.modules.items():
            if u.is_domain and mod == "header":
                for n in tree.body:
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        v.append(Violation("M4", _where(u, p), f"header 定义了函数 {n.name}(头文件零实现)"))
            for ln in _print_calls(tree):
                v.append(Violation("M10", f"{_where(u, p)}:{ln}", "裸 print(一律走 logger,含 __main__ 入口)"))
            for ln, expr in _platform_branches(tree):
                v.append(Violation("M11", f"{_where(u, p)}:{ln}", f"判平台 {expr}(Linux only,不写平台分支)"))

    # M7 / M9:分级声明、声明一致、白名单防腐、不常驻;收集 GPU 模块(M8)
    gpu_modules: dict[str, set[str]] = {}
    for u in units:
        if not u.is_domain:
            continue
        loads_models = bool(u.helpers)
        gpu_modules[u.name] = set()
        for mod, (p, tree) in u.modules.items():
            levels = _class_levels(tree)
            if any(_level(f) for f in levels.values()):
                loads_models = True
            if any(_level(f) == "gpu" for f in levels.values()):
                gpu_modules[u.name].add(mod)
            for cls, f in levels.items():
                if f.get("HEAVY_INFERENCE") is True and f.get("CPU_ONLY") is True:
                    v.append(Violation("M7", _where(u, p), f"{cls} 同时声明 HEAVY_INFERENCE 与 CPU_ONLY(互斥)"))
                if f.get("CPU_ONLY") is True and not (isinstance(f.get("RAM_GB"), (int, float)) and f["RAM_GB"] > 0):
                    v.append(Violation("M7", _where(u, p), f"{cls} 为 CPU_ONLY 但未声明 RAM_GB>0"))
            fw = _framework_imports(tree)
            if fw and mod not in u.helpers and not any(_level(f) for f in levels.values()):
                v.append(Violation("M7", _where(u, p),
                                   f"import {'/'.join(sorted(fw))} 却没有声明级别的引擎类"
                                   "(载模型须封装为引擎类;只做数据处理的辅助模块登记 FRAMEWORK_HELPERS)"))
        for mod in sorted(set(u.helpers) - set(u.modules)):
            v.append(Violation("M7", f"{u.name}/header.py", f"FRAMEWORK_HELPERS 登记的 {mod} 已不存在"))
        live: set[tuple[str, str]] = set()
        if loads_models:
            for mod, (p, tree) in u.modules.items():
                for name, how in sorted(_state_writes(tree)):
                    live.add((mod, name))
                    if (mod, name) not in u.state_allowed:
                        v.append(Violation("M9", _where(u, p),
                                           f"函数改写模块级/类级状态 {name}({how})——模型存进去就常驻;"
                                           "非模型状态请登记 STATE_ALLOWED"))
        for key in sorted(set(u.state_allowed) - live):
            v.append(Violation("M9", f"{u.name}/header.py", f"STATE_ALLOWED 登记的 {key} 代码已不再改写,删掉登记"))

    # M6 / M8:跨域 import
    global_view = mode == "variant"
    pkg_base = root if global_view else import_base / root_pkg
    skipped_m6 = skipped_m8 = 0
    for u in units:
        own = u.name.split("/")[0]
        for mod, (p, tree) in u.modules.items():
            pkg_parts = [root_pkg] + list(p.relative_to(pkg_base).parts)[:-1]
            for top, sub in _cross_imports(pkg_parts, root_pkg, own, tree):
                if top in RESERVED_DIRS:
                    continue                                   # 内核保留目录(pipelines 等)不归本规则
                first = sub.split(".")[0] if sub else ""
                if first in ("header", "schema"):
                    continue
                tgt = by_name.get(top)
                if tgt is None:
                    if global_view:
                        continue                               # 变体里不存在的名字 = 非域,不归本规则
                    if "impl" in sub.split("."):
                        v.append(Violation("M6", _where(u, p), f"越界 import {top}.{sub}(他域 impl 永不公开)"))
                    else:
                        skipped_m6 += 1                        # 公开件是否声明:需对方 header
                    continue
                if not ("*" in tgt.public or first in tgt.public):
                    v.append(Violation("M6", _where(u, p), f"越界 import {top}.{sub or '(域包本身)'}"
                                       f"(未在 {top} 的 PUBLIC 声明)"))
                    continue
                if not u.name.startswith(PIPELINES) and any(
                        sub == g or sub.startswith(g + ".") for g in gpu_modules.get(top, ())):
                    v.append(Violation("M8", _where(u, p), f"import GPU 引擎 {top}.{sub}(只准本域与 pipelines/)"))
    if not global_view:
        if skipped_m6:
            report.skipped.append(f"M6 {skipped_m6} 处跨域 import 的公开件判定(需对方 header),由变体检查覆盖")
        if mode == "domain" and gpu_modules.get(units[0].name):
            skipped_m8 += len(gpu_modules[units[0].name])
            report.skipped.append(f"M8 本域 {skipped_m8} 个 GPU 引擎模块被谁 import(需全局视野),由变体检查覆盖")
    if tmp is not None:
        tmp.cleanup()
    return report


def main(argv: list[str] | None = None) -> int:
    from .core import logger
    args = sys.argv[1:] if argv is None else argv
    report = check(args[0] if args else "src")
    kw = dict(layer="CORE", component="Discipline")
    for x in report.violations:
        logger.error("%s", x, **kw)
    for s in report.skipped:
        logger.info("跳过 %s", s, **kw)
    mode = {"variant": "变体域根", "domain": "单域", "pipeline": "单管线"}[report.mode]
    logger.info("机器宪法扫描(%s)%s:%s;第二级行为规范 B1–B7 须人工对照 docs/CONSTITUTION.md",
                mode, report.root, f"{len(report.violations)} 条违规" if report.violations else "全部合规", **kw)
    return 1 if report.violations else 0


if __name__ == "__main__":
    raise SystemExit(main())
