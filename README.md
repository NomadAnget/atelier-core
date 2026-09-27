# atelier-core

**Atelier 内核** —— 消息总线 + 存储枢纽 + 装配注册表。

Atelier 是一套**编排众模型、批量产出媒体的基座**:一堆各有专长的模型(**artisans**)在自治域(**studios**)里,按管线接受**commission**(任务),协同产出**œuvre**(媒体作品)。`atelier-core` 是所有产品变体共享的地基,零业务逻辑。

## 命名体系

| 层级 | 名字 | 说明 |
|---|---|---|
| 品牌 / 系统 | **Atelier** | 编排众模型批量产媒体的基座 |
| 内核 / 基座 | **atelier-core** | 总线 + 存储枢纽 + 装配注册表(本仓)|
| 变体 / 产品 | **atelier-\<品类\>** | 某类媒体任务的执行载体,长在 core 上 |

变体按**产出媒体品类**命名:`atelier-reel`(视频本地化/配音)、`atelier-press`(新闻日报)、`atelier-audio`、`atelier-frame`…… 各自持有自己的 `main.py` 域接线,共享本仓的内核与装配原语。

## 文档(宪法)

`docs/` 是 **Atelier 体系的宪法** —— 架构/契约/纪律的唯一真源(不止本仓,整个体系以它为准):

| 篇 | 内容 |
|---|---|
| [docs/CONSTITUTION.md](docs/CONSTITUTION.md) | 纲领:铁律 + 认知隔离 + 三层结构 + 宪法同步律 |
| [docs/architecture.md](docs/architecture.md) | 三层结构、目录即路由、domain_root、组装点、新增域清单 |
| [docs/bus.md](docs/bus.md) | 消息总线:契约/闭环校验/投递语义/handler 契约/网关/执行交接 |
| [docs/storage.md](docs/storage.md) | 存储枢纽:两种表、写权、无感自增量、设置基座 |
| [docs/domains.md](docs/domains.md) | 域构建准则:三形态、文件结构、import 白名单、重推理闸 |
| [docs/boot.md](docs/boot.md) | 组装层:build_hub/store、监督器、process 域(保留·默认未用) |
| [docs/logging.md](docs/logging.md) | 日志三用法(分级 / 临时进度 / 结构化中继) |

**宪法同步律**:契约/规则/纪律的变更,先改 `docs/` 对应篇再写代码。

## 构成

```
atelier_core/           # 包根(可被变体 uv add 后绝对导入 atelier_core.*)
  main.py               组装入口模板;最小 serve()(空总线可起 + --check),
                        产品仓照它写自己的 <域>_handlers 接线(组装点唯一)
  core/                 内核(地基,RESERVED,不是域)
    core.py             消息总线内核——目录即路由、投递语义、闭环校验
    contracts.py        总线载荷契约基类(Message)
    journal.py          持久账本——"发出去 = 落盘了"
    storage.py          存储枢纽——无感自增量、信封表、按域写权
    settings.py         运行时设置底座(单行信封设置)
    registry.py         装配注册表——头文件发现 + 声明校验
    log_store.py        内核日志落库(hub_logs 类型化表)
    logger.py           独立日志器——黑匣子(内核级:core 自身依赖它)
    config.py           基建配置加载(仅 stdlib tomllib)
    gateway/            网关——远程域 ws:// 桥(client/server)
  boot/                 组装层(RESERVED,不是域)
    assembly.py         装配原语(域无关):build_hub / build_store / housekeeping / 单实例锁 / self_test
    composition.py      按 KIND 装配(local / process / remote)
    supervisor.py       子进程监督器(拉起 / 看活 / 停机收编)
    remote.py           process 域子进程入口(python -m atelier_core.boot.remote <域> [域根])
```

三层分明:`main.py`(产品接线)· `core/`(内核 + logger/config)· `boot/`(域无关装配层)。

## 定位:通用依赖包,与变体解耦

**底座,不是完整系统**:不含任何域(studio)。内核对"域住哪儿"一无所知——**发现/装配全部按 `domain_root` 参数走**,变体传自己的包,内核代码永不因变体而改。

变体消费内核只需三件事(其余内核全包):

| 变体特有的 | 怎么给内核 |
|---|---|
| 域住哪 | `build_hub(domain_root=__package__)`(模板已写死这句,自定位)|
| 配置 | `config.load("自己的 config.toml")`(9 个基建节通用;功能配置走 settings 表)|
| 引内核 / 引兄弟域 | 绝对 `from atelier_core.core import …` / 相对 `from ..域 import …` |

发现按"目录即路由"扫**变体的包**(`core`/`boot`/`services` 是 RESERVED,不当域);无域即空总线。**装配原语在 `boot/`(随内核走);"装配哪些域"在各变体的 `main.py::serve()`(随产品走)——组装点唯一。**

## 包索引

**唯一发布源:Forgejo PyPI**(由 `.forgejo/workflows/publish.yml` 自动构建发布;GitHub 仓不构建、不发包)。

| | 地址 |
|---|---|
| 包页面 | http://debian.lan:3257/Carnation/-/packages/pypi/atelier-core |
| PyPI simple 索引 | http://debian.lan:3257/api/packages/Carnation/pypi/simple/ |

变体侧 `pyproject.toml` 配置索引后即可按版本约束消费(`uv sync` 自动升):

```toml
[project]
dependencies = ["atelier-core>=0.2"]

[[tool.uv.index]]
name = "forgejo"
url = "http://debian.lan:3257/api/packages/Carnation/pypi/simple/"

[tool.uv.sources]
atelier-core = { index = "forgejo" }
```

私有包需认证:`export UV_INDEX_FORGEJO_USERNAME=Carnation UV_INDEX_FORGEJO_PASSWORD=<有 package 读权限的 token>`。

## 依赖

轻量,无重型推理栈:`pydantic` / `python-dotenv` / `websockets` / `uvicorn`。Python 3.11。

```bash
# 本仓自检(空总线,域根=atelier_core 自身=0 域)
uv run python -m atelier_core.main --check

# 变体侧:装为依赖后
uv add atelier-core            # 或从 Gitea 装/可编辑装
# 变体 main.py:from atelier_core.boot.assembly import build_hub, build_store
#              from atelier_core.core import logger
#              DOMAIN_ROOT = __package__   # 内核据此扫本变体的域
```
