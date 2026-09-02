# 域构建准则

## 域的三种构建形态(基础设施按类型通用组装)

域露三张声明面:`header.py`(总线契约)、`schema.py`(存储)、`build.py`(装配声明,可选)。
**组装是基础设施的职责,域只声明"我是什么类型、我怎么构造"。**

| 类型 KIND | 实现在哪 | 基础设施怎么装 | 何时用 |
|---|---|---|---|
| **local**(默认) | 主进程内 | 调域 `build(ctx)` 得 handlers/loops → `bus.mount` + 起循环 | 绝大多数域 |
| **process** | 按需子进程 | 主进程:网关转发 + lazy 子进程 + 起停控制;子进程(`python -m atelier_core.boot.remote <域> [域根]`)调**同一** `build(ctx)`(bus 句柄换 BusClient) | 需整域出进程做完美资源回收+故障隔离(纯 Python 重活、无引擎级子进程可依赖时)。**当前无 process 域**——见下 |
| **remote** | 独立进程/异地/别语言 | 网关转发(域无 `build.py`,`cfg.gateway.remote_domains` 声明);公民资格由外部实现自取 | 异地/外部实现(发布引擎) |

`build.py` 声明面(local/process 必备;remote 无):

```python
KIND = "local"            # 或 "process"
PROCESS = {"lazy": True}  # 仅 process
def build(ctx: BuildContext) -> DomainRuntime:
    """基础设施在正确进程里调用。只用 ctx(hub/stores/bus/publish句柄/loop/cfg/infra)
    构造服务、init 本域依赖的跨域读门面、注册管线;返回 handlers + 循环 + 落种/停机钩子。
    绝不 import bus.mount / 网关 / supervisor。"""
    return DomainRuntime(handlers={...}, loops=[...], on_seed=..., on_shutdown=...)
```

**同一 `build` 三形态复用**:从 local 改 process 只改 `KIND` 一行,构造代码零改动。

> **process 域现状**:task 域曾是唯一 process 域,**2026-08-30 回落 local**。原因:①重推理引擎默认
> 已**单次推理级子进程隔离**(`SUBPROCESS_INFER`,见下),GPU 回收+崩溃隔离在**引擎层**已具备,整域
> 再进程化=重复隔离;②process 域**懒下线**会使域内常驻轻量职责(名词对照/知识库 handler)在域休眠时
> 无法即时响应。**本能力完整保留**(`boot/composition.py` + `boot/remote.py` + 监督器懒起),供将来确有
> "整域需出进程"需求的域使用。

## 域的标准文件结构

**目录即隔离:域根只放"外界能碰的东西",私有实现全收进 `impl/`。**

```
src/<domain>/
├── __init__.py    一句话域说明
├── header.py      消息窗口(必须):DOMAIN / PUBLISHES / SUBSCRIBES / EPHEMERAL·VOLATILE(可选)
├── schema.py      存储声明(仅持久数据的域):TABLES
├── build.py       装配声明(local/process 域;或直接在变体 main.py 接线)
├── requirements.txt  本域运行时依赖闭环(变体动态聚合)
└── impl/          域私有实现(域外永不 import)
```

**例外:白名单公开件也留域根**(本就允许被跨域直调):如 `scheduler/registry.py`(管线插座)、
`platform_adapters/` 的协议件(`youtube/`、`youtube_token.py`、`oauth_state.py`、`ingest.py`)、
`daemons/sources.py`(采集源门面)。`engines/`(纯函数库)整体公开,根即接口,不套 impl。

判定口诀:**这文件允许被别的域 import 吗?能→留域根;不能→进 impl/。** 机器可核
(`test_import_discipline`:跨域 import 只放行 `他域.header`/`他域.schema` 与白名单,`他域.impl.*` 一律拒)。

## 域内代码的三种身份

| 身份 | 是什么 | 规矩 |
|---|---|---|
| **handler(消费端)** | 绑定订阅槽、由总线消费任务调用 | 轻量律:毫秒返回;只编排;禁 CPU 重活/同步阻塞;**必须幂等**;`correlation_id` 自动透传 |
| **自有循环** | 服务常驻 asyncio 任务(排产/轮询) | 长 await/长睡合法;异常自捕获后循环继续,`CancelledError` 必 re-raise;同禁 CPU 重活 |
| **发布端** | 经 Window 发消息 | 只发本域 header 声明的 topic,载荷用本域 Message |

## import 白名单

**一律相对 import,不硬编码顶层包名。** 域实现文件只允许 import:

1. **内核/基础设施**:`from atelier_core.core import …` / `atelier_core.core.config` /
   `atelier_core.boot.composition`(仅装配契约类型 `BuildContext` / `DomainRuntime`;组装机制不碰);
2. **本域自己的文件**(相对);
3. **他域的 header / schema**(契约级依赖:拿消息与数据形状,相对 `from ..域 import header`);
4. **`engines` 纯函数库**。**重推理引擎除外**——见下"重推理闸";
5. **`platform_adapters` 协议件** + `daemons.sources` 等域根门面(纯函数/网络 IO,不发消息);
6. **task SDK** `from ..task.sdk import …`(PipelineStep/StepContext/CheckpointStore/… 供管线插件);
7. **`atelier_core.core.logger`**(黑匣子)。

**禁止**:import 他域的 service/impl 文件;任何绕过窗口的旁路。唯一跨域 import 特例:
`scheduler.registry`(任务类型注册表=管线插座,管线域只碰注册函数)。

## 重推理闸(铁律 #5)

引擎文件凡 import 推理框架(torch/paddle/onnxruntime/nemo 等,`engines/base.py` 基类豁免)= 重引擎
——载权重上 GPU,**只准 `pipelines/` 内 import**(引擎包内互调不受限)。域 handler/服务层拉起重引擎
会和运行中的任务抢显卡。双档执法:

- **静态**:声明式自动识别,越界 import 即红灯(`test_import_discipline`);
- **运行时**:重引擎类标 `HEAVY_INFERENCE=True`,`BaseModel.__enter__` 在管线上下文
  (`heavy_inference_scope`,管线 runner 打开,线程局部)之外**当场拒绝载权重**;逃生门
  `GCP_ALLOW_HEAVY_OUTSIDE_PIPELINE=1`。

**CPU 重活的唯一合法去处**:engines 纯函数库,由 worker 经线程池/独立进程调用,永不直接跑在事件循环。
引擎并发/单次推理子进程隔离(`map_concurrent` 纯进程级 + `SUBPROCESS_INFER`)见引擎域(`atelier-engines`)
自身文档。

## 命名规范

- 域名 = 目录名 = topic 首段 = 表名前缀(机器校验);
- topic:`域/名词/事件` 全小写(如 `daemons/schedule/due`);
- 槽位名:`on_<事件>`;表:`<域>_<复数名词>`;kind:单数名词;Message/Record:名词短语驼峰。

## 域服务标准形态

```python
class XxxService:
    def __init__(self, bus_win, hub, store_win, <外部依赖均为参数>):
        ...                      # 只存引用,不做任何 IO(可测性)
    async def on_some_event(self, env) -> None:
        ...                      # handler:轻量律管辖
    async def run_loop(self) -> None:
        ...                      # 自有循环:组装点 create_task
```

构造器零 IO;外部世界一律以可调用参数注入(测试传假的,生产传真的,服务代码零改动)。
