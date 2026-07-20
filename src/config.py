"""src 基建配置 —— 只管"系统怎么搭起来"(零依赖:仅 stdlib tomllib)。

边界(与运行时配置的分界,README 单一真源):
  · 组装点(main.py)组装时需要的,进 config.toml —— 库路径、端口、总线参数、
    日志级别、哨兵阈值;
  · 组装完成后才有人关心的(日报声纹、监控频率、发布目标…)是**域数据**:
    存于存储枢纽、归拥有域、前端可改、域自己广播变更——与本模块无关。
  基建配置改文件 + 重启生效:无热重载、无 DB 覆盖层、无变更事件,按设计不需要。

用法(组装点):
    from . import config
    cfg = config.load()          # 默认 ./config.toml,可被 HUB_CONFIG 环境变量覆盖
    cfg.bus.queue_size

显式失败:未知的节/键、类型不符 → 立即抛错(打字错误保护);
文件缺失合法 = 全默认值(cfg.source 说明来源)。
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, fields
from pathlib import Path


class ConfigError(Exception):
    """基建配置不合法(未知节/键、类型不符)。"""


@dataclass(frozen=True)
class PathsCfg:
    journal_db: str = "data/hub/journal.db"   # 总线持久账本
    store_db: str = "data/hub/store.db"       # 存储枢纽(含 hub_logs)
    models_dir: str = "checkpoints"           # 模型权重根目录(引擎库经 GCP_MODELS_DIR 取用)


@dataclass(frozen=True)
class BusCfg:
    queue_size: int = 4096       # 每订阅队列深度(满则背压)
    max_attempts: int = 3        # 持久投递重试上限(超限进死信)
    put_timeout: float = 30.0    # 背压等待超时(秒,超时抛 BusError)


@dataclass(frozen=True)
class LoggerCfg:
    level: str = "INFO"


@dataclass(frozen=True)
class LogStoreCfg:
    batch: int = 200             # 日志落库攒批条数
    flush_interval: float = 0.5  # 攒批最长等待(秒)
    queue_max: int = 10000       # 落库队列上限(满则丢帧,不阻塞记录)


@dataclass(frozen=True)
class SentinelCfg:
    patrol_interval: float = 10.0
    queue_backlog_threshold: int = 1024
    pending_stale_sec: float = 600.0
    job_stall_sec: float = 1800.0    # running 无进度写入告警阈值(任务活性规则)
    heartbeat_grace: float = 3.0
    error_burst_threshold: int = 5   # 一个巡检周期内 ERROR+ 达此数即告警


@dataclass(frozen=True)
class SupervisorCfg:
    children: tuple = ()             # [[supervisor.children]]:name/cmd 必填,cwd/env 可选


@dataclass(frozen=True)
class RetentionCfg:
    journal_days: float = 7.0      # 账本:全销账消息的保留期
    logs_days: float = 30.0        # hub_logs:日志行保留期
    sweep_interval_h: float = 6.0  # 内务循环扫除间隔(小时)


@dataclass(frozen=True)
class GatewayCfg:
    enabled: bool = False
    host: str = "127.0.0.1"          # 只绑本机;跨机部署再显式放开
    port: int = 8790
    token: str = ""                  # 非空则远端 hello 必须携带
    ack_timeout: float = 600.0       # 远端处理一条投递的最长时限(如过风控可达分钟级)
    remote_domains: tuple = ()       # 声明为"异地实现"的域(如 "publishers")


@dataclass(frozen=True)
class WebCfg:
    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 8000


@dataclass(frozen=True)
class HubConfig:
    paths: PathsCfg
    bus: BusCfg
    logger: LoggerCfg
    logstore: LogStoreCfg
    sentinel: SentinelCfg
    retention: RetentionCfg
    gateway: GatewayCfg
    web: WebCfg
    supervisor: SupervisorCfg
    source: str                  # 配置来源(文件路径或"缺省值"说明)


_SECTIONS = {"paths": PathsCfg, "bus": BusCfg, "logger": LoggerCfg,
             "logstore": LogStoreCfg, "sentinel": SentinelCfg,
             "retention": RetentionCfg, "gateway": GatewayCfg, "web": WebCfg,
             "supervisor": SupervisorCfg}


def _build_section(cls: type, name: str, raw: dict) -> object:
    known = {f.name: f.type for f in fields(cls)}
    unknown = raw.keys() - known.keys()
    if unknown:
        raise ConfigError(f"[{name}] 含未知键 {sorted(unknown)}(打字错误?)")
    values: dict = {}
    for f in fields(cls):
        if f.name not in raw:
            continue
        v = raw[f.name]
        ftype = str(f.type)
        if ftype == "float" and isinstance(v, int) and not isinstance(v, bool):
            v = float(v)
        if ftype == "tuple" and isinstance(v, list):
            if all(isinstance(x, str) for x in v) or all(isinstance(x, dict) for x in v):
                v = tuple(v)
            else:
                raise ConfigError(f"[{name}].{f.name} 列表元素须同为字符串或同为表")
        if ftype == "bool":
            if not isinstance(v, bool):
                raise ConfigError(f"[{name}].{f.name} 类型应为 bool,得到 {type(v).__name__}")
        else:
            expected = {"int": int, "float": float, "str": str, "tuple": tuple}.get(ftype)
            if expected is not None and (not isinstance(v, expected) or isinstance(v, bool)):
                raise ConfigError(
                    f"[{name}].{f.name} 类型应为 {expected.__name__},得到 {type(v).__name__}")
        values[f.name] = v
    return cls(**values)


def load(path: str | None = None) -> HubConfig:
    """读取基建配置:显式 path > 环境变量 HUB_CONFIG > ./config.toml > 全默认。

    同时加载 ``.env`` 进环境变量(对齐旧 src/app.py 的 load_dotenv):
    平台密钥(YOUTUBE_CLIENT_ID/SECRET、API key 等)全走 env,不进 TOML。"""
    from dotenv import load_dotenv
    load_dotenv()
    p = Path(path or os.environ.get("HUB_CONFIG", "config.toml"))
    raw: dict = {}
    source = f"缺省值(未找到 {p})"
    if p.exists():
        with open(p, "rb") as fh:
            raw = tomllib.load(fh)
        source = str(p)
    unknown = raw.keys() - _SECTIONS.keys()
    if unknown:
        raise ConfigError(f"含未知配置节 {sorted(unknown)}(合法节:{sorted(_SECTIONS)})")
    sections = {name: _build_section(cls, name, raw.get(name) or {})
                for name, cls in _SECTIONS.items()}
    return HubConfig(source=source, **sections)
