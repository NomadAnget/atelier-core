"""头文件发现与声明校验:目录即路由的机器化约束。

规则(违反即抛 BusDeclarationError,不静默):
  1. src/ 下每个子目录(除保留名)= 一个域,域内必须有 header.py;
  2. header 必须声明 DOMAIN / PUBLISHES / SUBSCRIBES 三件套;
  3. DOMAIN 必须等于目录名;
  4. PUBLISHES 的每个 topic 首段必须等于 DOMAIN(发布权随目录走),
     且为具体路径(不含通配符),schema 必须是 Message 子类;
  5. SUBSCRIBES 的 pattern 支持 "+"(单段)与 "#"(子树,仅限末段),
     值为处理器槽位名(字符串),实际处理器在 mount 时绑定。
"""

from __future__ import annotations

import importlib
import pkgutil
from types import ModuleType

from .contracts import Message, Record

# 顶层包名从自身模块名推导(registry 是 <根>.core.registry)——域发现/加载不硬编码
# "src",整个包目录改名即自动跟随,内部代码零改动。
_ROOT = __name__.split(".")[0]


class BusDeclarationError(Exception):
    """头文件声明不合法。"""


def _check_topic(topic: str, domain: str) -> None:
    segs = topic.split("/")
    if len(segs) < 2 or any(not s for s in segs):
        raise BusDeclarationError(f"{domain}: topic '{topic}' 必须为 '域/子路径/事件' 形式且无空段")
    if segs[0] != domain:
        raise BusDeclarationError(f"{domain}: 越权声明 '{topic}' —— topic 首段必须等于域名(发布权随目录走)")
    if any(s in ("+", "#") for s in segs):
        raise BusDeclarationError(f"{domain}: 发布 topic '{topic}' 不允许含通配符")


def _check_pattern(pattern: str, domain: str) -> None:
    segs = pattern.split("/")
    if not segs or any(not s for s in segs):
        raise BusDeclarationError(f"{domain}: 订阅 pattern '{pattern}' 含空段")
    for i, s in enumerate(segs):
        if s == "#" and i != len(segs) - 1:
            raise BusDeclarationError(f"{domain}: '#' 只能作 pattern 末段('{pattern}')")


def validate_header(
    header: ModuleType, expected_domain: str | None = None
) -> tuple[str, dict[str, type[Message]], dict[str, str], set[str], set[str]]:
    """校验一份头文件,返回 (domain, publishes, subscribes, ephemeral, volatile)。

    EPHEMERAL(可选):本域 topic 子集,纯遥测——跳过账本、不抗崩溃、不重试、不计投递。
    VOLATILE(可选):本域 topic 子集,易失命令/事件——同样跳过账本、at-most-once,
    但是工作消息(计入发布/投递、受悬空校验约束)。二者互斥;默认全部持久。"""
    for attr in ("DOMAIN", "PUBLISHES", "SUBSCRIBES"):
        if not hasattr(header, attr):
            raise BusDeclarationError(f"{header.__name__}: 缺少声明 {attr}")
    domain: str = header.DOMAIN
    if not isinstance(domain, str) or not domain:
        raise BusDeclarationError(f"{header.__name__}: DOMAIN 必须是非空字符串")
    if expected_domain is not None and domain != expected_domain:
        raise BusDeclarationError(
            f"{header.__name__}: DOMAIN='{domain}' 与目录名 '{expected_domain}' 不一致")

    publishes: dict[str, type[Message]] = dict(header.PUBLISHES)
    for topic, schema in publishes.items():
        _check_topic(topic, domain)
        if not (isinstance(schema, type) and issubclass(schema, Message)):
            raise BusDeclarationError(f"{domain}: '{topic}' 的 schema 必须是 Message 子类")

    subscribes: dict[str, str] = dict(header.SUBSCRIBES)
    for pattern, slot in subscribes.items():
        _check_pattern(pattern, domain)
        if not isinstance(slot, str) or not slot:
            raise BusDeclarationError(f"{domain}: 订阅 '{pattern}' 的槽位名必须是非空字符串")
    ephemeral: set[str] = set(getattr(header, "EPHEMERAL", ()) or ())
    if ephemeral - publishes.keys():
        raise BusDeclarationError(
            f"{domain}: EPHEMERAL 含未发布的 topic {sorted(ephemeral - publishes.keys())}")
    volatile: set[str] = set(getattr(header, "VOLATILE", ()) or ())
    if volatile - publishes.keys():
        raise BusDeclarationError(
            f"{domain}: VOLATILE 含未发布的 topic {sorted(volatile - publishes.keys())}")
    if volatile & ephemeral:
        raise BusDeclarationError(
            f"{domain}: topic 不能同时是 EPHEMERAL 与 VOLATILE {sorted(volatile & ephemeral)}")
    return domain, publishes, subscribes, ephemeral, volatile


def load_header(domain: str, root: str | None = None) -> ModuleType:
    """按域名加载头文件模块(root=域根包,默认内核自身;变体传自己的包)。"""
    return importlib.import_module(f"{root or _ROOT}.{domain}.header")


RESERVED_DIRS = {"core", "boot", "services", "pipelines"}   # core=内核;boot=组装层;services=远程域宿主;pipelines=变体管线插件挂载点(非域)


def discover_headers(root: str | None = None) -> dict[str, ModuleType]:
    """扫描域根包下全部域目录,加载并按目录名校验各头文件。

    root=域根包(变体传自己的包,默认内核自身)。保留名(RESERVED_DIRS)不是域;
    其余子目录都必须有 header.py,缺了就抛错。
    """
    root = root or _ROOT
    pkg = importlib.import_module(root)

    headers: dict[str, ModuleType] = {}
    for info in pkgutil.iter_modules(pkg.__path__):
        if not info.ispkg or info.name in RESERVED_DIRS:
            continue
        header = load_header(info.name, root)
        validate_header(header, expected_domain=info.name)
        headers[info.name] = header
    return headers


# ── 存储声明(schema.py)──────────────────────────────────────────────────────

_ENVELOPE_COLS = ("id", "kind", "data", "created_at", "updated_at")
_INDEX_TYPES = {"TEXT", "INTEGER", "REAL"}


def validate_schema(schema: ModuleType, expected_domain: str | None = None) -> tuple[str, dict]:
    """校验一份存储声明,返回 (domain, tables)。

    规则(与消息窗口对称):
      · 表归属随目录走:表名必须以 "<域名>_" 为前缀;
      · kinds:集合名 → Record 子类(data 列的形状契约);
      · indexes:要提升为生成列+索引的 data 字段(TEXT/INTEGER/REAL),
        字段必须在至少一个 kind 模型中声明,且不得与信封列重名;
      · unique ⊆ indexes。
    """
    for attr in ("DOMAIN", "TABLES"):
        if not hasattr(schema, attr):
            raise BusDeclarationError(f"{schema.__name__}: 缺少声明 {attr}")
    domain: str = schema.DOMAIN
    if expected_domain is not None and domain != expected_domain:
        raise BusDeclarationError(
            f"{schema.__name__}: DOMAIN='{domain}' 与目录名 '{expected_domain}' 不一致")

    tables: dict[str, dict] = {t: dict(spec) for t, spec in schema.TABLES.items()}
    for table, raw in tables.items():
        if not table.startswith(f"{domain}_"):
            raise BusDeclarationError(
                f"{domain}: 表 '{table}' 必须以 '{domain}_' 为前缀(表归属随目录走)")
        if "columns" in raw and "kinds" in raw:
            raise BusDeclarationError(
                f"{domain}: 表 '{table}' 只能二选一:信封(kinds)或类型化(columns)")
        if "columns" in raw:
            tables[table] = _validate_typed(domain, table, raw)
        else:
            tables[table] = _validate_envelope(domain, table, raw)
    return domain, tables


def _validate_renamed_from(domain: str, table: str, raw: dict) -> str:
    """声明式改名迁移:renamed_from=旧表名(跨域迁移/更名时数据原地保留)。
    枢纽 start() 见"新名缺、旧名在"即执行一次 ALTER RENAME——声明的迁移,
    不是自动猜测;新旧同在则显式报错(不猜哪份是真相)。"""
    old = str(raw.get("renamed_from") or "")
    if old and old == table:
        raise BusDeclarationError(f"{domain}: 表 '{table}' 的 renamed_from 不得指向自身")
    return old


def _validate_envelope(domain: str, table: str, raw: dict) -> dict:
    unknown = set(raw) - {"kinds", "indexes", "unique", "renamed_from"}
    if unknown:
        raise BusDeclarationError(f"{domain}: 表 '{table}' 含未知声明键 {sorted(unknown)}")
    kinds = raw.get("kinds") or {}
    if not kinds:
        raise BusDeclarationError(f"{domain}: 表 '{table}' 至少声明一个 kind")
    declared_fields: set[str] = set()
    for kind, model in kinds.items():
        if not (isinstance(model, type) and issubclass(model, Record)):
            raise BusDeclarationError(
                f"{domain}: '{table}.{kind}' 的形状必须是 Record 子类")
        declared_fields |= set(model.model_fields)
    indexes = dict(raw.get("indexes") or {})
    for field, sql_type in indexes.items():
        if sql_type not in _INDEX_TYPES:
            raise BusDeclarationError(
                f"{domain}: '{table}' 索引字段 '{field}' 类型须为 {sorted(_INDEX_TYPES)}")
        if field in _ENVELOPE_COLS:
            raise BusDeclarationError(
                f"{domain}: '{table}' 索引字段 '{field}' 与信封列重名")
        if field not in declared_fields:
            raise BusDeclarationError(
                f"{domain}: '{table}' 索引字段 '{field}' 未在任何 kind 模型中声明")
    unique = list(raw.get("unique") or [])
    if set(unique) - set(indexes):
        raise BusDeclarationError(
            f"{domain}: '{table}' unique 字段必须先声明为 indexes")
    return {"typed": False, "kinds": kinds, "indexes": indexes, "unique": unique,
            "renamed_from": _validate_renamed_from(domain, table, raw)}


def _validate_typed(domain: str, table: str, raw: dict) -> dict:
    """类型化表(高频大表:真实列,无信封):columns / indexes(列名表)/ unique。"""
    unknown = set(raw) - {"columns", "indexes", "unique", "renamed_from"}
    if unknown:
        raise BusDeclarationError(f"{domain}: 表 '{table}' 含未知声明键 {sorted(unknown)}")
    columns = dict(raw.get("columns") or {})
    if not columns:
        raise BusDeclarationError(f"{domain}: 类型化表 '{table}' 至少声明一列")
    for name, frag in columns.items():
        if not name.isidentifier() or name == "id":
            raise BusDeclarationError(
                f"{domain}: '{table}' 列名 '{name}' 非法(须为标识符且不得为 id,id 由枢纽自管)")
        if not isinstance(frag, str) or not frag.strip():
            raise BusDeclarationError(f"{domain}: '{table}.{name}' 的类型声明必须是非空字符串")
    indexes = list(raw.get("indexes") or [])
    if set(indexes) - columns.keys():
        raise BusDeclarationError(
            f"{domain}: '{table}' 索引列 {sorted(set(indexes) - columns.keys())} 未声明")
    unique = list(raw.get("unique") or [])
    if set(unique) - set(indexes):
        raise BusDeclarationError(f"{domain}: '{table}' unique 列必须先声明为 indexes")
    return {"typed": True, "columns": columns, "indexes": indexes, "unique": unique,
            "renamed_from": _validate_renamed_from(domain, table, raw)}


def load_schema(domain: str, root: str | None = None) -> ModuleType:
    """按域名加载存储声明模块(schema.py;root=域根包,默认内核自身)。"""
    return importlib.import_module(f"{root or _ROOT}.{domain}.schema")


def discover_schemas(root: str | None = None) -> dict[str, ModuleType]:
    """扫描域根包下全部域的 schema.py;没有该文件的域=不持久数据,合法跳过。"""
    root = root or _ROOT
    pkg = importlib.import_module(root)

    schemas: dict[str, ModuleType] = {}
    for info in pkgutil.iter_modules(pkg.__path__):
        if not info.ispkg or info.name in RESERVED_DIRS:
            continue
        try:
            schema = load_schema(info.name, root)
        except ModuleNotFoundError as exc:
            if exc.name == f"{root}.{info.name}.schema":
                continue   # 无存储声明,合法
            raise
        validate_schema(schema, expected_domain=info.name)
        schemas[info.name] = schema
    return schemas
