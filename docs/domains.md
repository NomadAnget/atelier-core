# 域构建准则

## 域的三种构建形态(基础设施按类型通用组装)

域露三张声明面:`header.py`(总线契约)、`schema.py`(存储)、`build.py`(装配声明,可选)。
**组装是基础设施的职责,域只声明"我是什么类型、我怎么构造"。**

| 类型 KIND | 实现在哪 | 基础设施怎么装 | 何时用 |
|---|---|---|---|
| **local**(默认) | 主进程内 | 调域 `build(ctx)` 得 handlers/loops → `bus.mount` + 起循环 | 绝大多数域 |
| **process** | 按需子进程 | 主进程:网关转发 + lazy 子进程 + 起停控制;子进程(`python -m atelier_core.boot.remote <域> [域根]`)调**同一** `build(ctx)`(bus 句柄换 BusClient) | 需整域出进程做完美资源回收+故障隔离(纯 Python 重活、无引擎级子进程可依赖时)。在用:**publishers**;生命周期两形态见 [boot.md](boot.md) |
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

> **process 域现状**:在用 process 域 = **publishers**(纯 Python 浏览器重活,整域出进程做完美资源
> 回收+故障隔离;生命周期用 [boot.md](boot.md) 形态 B——网关自动拉起、心跳续命、闲时回收)。
> task 域曾用形态 A,**2026-08-30 回落 local**:①重推理引擎默认已**单次推理级子进程隔离**
> (`SUBPROCESS_INFER`,见下),GPU 回收+崩溃隔离在**引擎层**已具备,整域再进程化=重复隔离;
> ②形态 A 的**懒下线**会使域内常驻轻量职责(名词对照/知识库 handler)在域休眠时无法即时响应。
> 判据:域是否**只做按需重活**(是→process 划算)还是**兼有常驻轻量 handler**(是→留 local)。

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

**例外:公开件也留域根,并在本域 header 的 `PUBLIC` 里声明**(本就允许被跨域直调):如 scheduler
`PUBLIC=("registry",)`(管线插座)、task `("sdk",)`、daemons `("sources",)`(采集源门面)、
platform_adapters 的协议件、media 的只读门面 `MediaStore`。`engines/`(纯函数库)整体公开,
声明 `PUBLIC=("*",)`,根即接口,不套 impl。

`VENDORED`(可选,域内相对路径元组):随域携带的第三方代码(如引擎域的 `media/indextts` 子模块),
不受纪律扫描——那不是我们写的代码,也不照我们的规矩写。

`PUBLIC` 的每一项是域根下的**模块名或名字**:`"registry"` 放行 `他域.registry` 及其下一切;`"MediaStore"`
放行 `from ..media import MediaStore`。**只有声明过的才放行**——域根 `__init__.py` 把 impl 里的东西转出来
而不声明,照样判越界(堵"经域根转手 impl"的漏洞)。

判定口诀:**这文件允许被别的域 import 吗?能→留域根并写进 PUBLIC;不能→进 impl/。** 机器可核
(`atelier_core.discipline`:跨域 import 只放行 `他域.header`/`他域.schema` 与他域 `PUBLIC`,其余一律拒)。

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
4. **他域 header 声明的 `PUBLIC` 公开件**(engines 纯函数库整体公开、task SDK、scheduler.registry 管线插座、
   协议件与门面等,以各域声明为准)。**GPU 重引擎除外**(只准本域与 `pipelines/` 内)——见下「引擎分级与不常驻」;
5. **`atelier_core.core.logger`**(黑匣子)。

**禁止**:import 他域未声明公开的文件(含 service/impl);任何绕过窗口的旁路。

## 引擎分级与不常驻(M7 / M8 / M9)

引擎域是**全系统的计算中心**,不专属管线。约束按**设备**分两级(旧判据"import 了推理框架即重"
已废:同一个 torch 模型跑 CPU 并不抢显卡,却被一刀切锁进管线)。

| 级 | 判据 | 谁能调 | 硬约束 |
|---|---|---|---|
| **重引擎** | 在本进程把权重载上 **GPU**(`HEAVY_INFERENCE=True`) | **只准 `pipelines/` 上下文** | 单次推理默认走独立子进程(故障隔离 + 显存完美回收,`SUBPROCESS_INFER`);宜声明 `VRAM_GB`(`map_concurrent` 据此判并发) |
| **轻引擎** | 纯 CPU 推理(`CPU_ONLY=True`),或只调外部服务(ollama 等,显存归外部服务管,无需标记) | **任何域** | 永不跑在事件循环(worker 线程池 / 独立进程);`CPU_ONLY` 须声明 `RAM_GB` |

**载模型的代码一律是引擎类**:模块级函数直接载模型(`load_xxx()` 后当场推理)无从分级、无从管生命周期,
不允许;须封装为 `BaseModel` 子类并二选一声明 `HEAVY_INFERENCE=True`(GPU)或 `CPU_ONLY=True`(CPU),
两者互斥。只 import 推理框架做数据处理、不载模型的**辅助模块**(如某引擎包的 `_common.py`)须在所属域
header 的 `FRAMEWORK_HELPERS`(`{模块: 理由}`)里逐条登记。本条对**所有域**一视同仁(内核不认识哪个是引擎域):
哪个域有载模型的代码,就在哪个域按本节约束。

`CPU_ONLY` 引擎(用推理框架跑 CPU)额外:

- 强制独立子进程推理(内存完美回收),子进程**屏蔽 GPU**(`CUDA_VISIBLE_DEVICES=""`)——
  物理上碰不到显卡,不靠自觉;
- 同一模型既要 GPU 批量又要 CPU 零散用时,**拆两个引擎类**(同权重、同输出空间),各按其级约束。

**不常驻(两级通用)**:模型只活在一次调用 / 一个 `with` 作用域内,用完即释放。禁止把模型对象存进
模块级 / 类级变量或域服务实例上复用(`_cache[lang] = spacy.load(...)` 这类即违规,不论是否引擎类);
调外部服务的引擎退出时须让服务卸载(ollama `keep_alive=0` 并确认显存归还)。理由:本机与生产
**同机**,常驻的每一份模型都在和生产任务的峰值抢内存 / 显存,且无人负责回收。
(权重文件被 OS 页缓存不算常驻——那是内核可随时回收的缓存。)

执法:

- **运行时**(生效中):`BaseModel.__enter__` 对 `HEAVY_INFERENCE=True` 在管线上下文
  (`heavy_inference_scope`,管线 runner 打开,线程局部)之外**当场拒绝载权重**;逃生门
  `GCP_ALLOW_HEAVY_OUTSIDE_PIPELINE=1`。CPU 框架引擎的子进程由基类统一设 `CUDA_VISIBLE_DEVICES=""`。
- **静态**:`atelier_core.discipline`(AST,不执行任何域代码;header 声明按字面量读),对全部域一视同仁:
  ① import 推理框架的模块须含已声明级别的引擎类(或登记在本域 `FRAMEWORK_HELPERS`);② `HEAVY_INFERENCE`/`CPU_ONLY`
  互斥,`CPU_ONLY` 须 `RAM_GB>0`;③ **不常驻**:载模型的域(有引擎类或辅助模块的域)其模块的函数/方法不得改写
  模块级状态(`global`、`_x[k]=`、`_x.update()` 等)或类级状态(`cls._x=`),非模型状态(配置快照/静态表)登记在
  本域 header 的 `STATE_ALLOWED`(`{(模块, 名字): 理由}`),登记项失效即报错;④ **跨域**:定义 GPU 引擎的模块只准
  本域与 `pipelines/` 内 import。跑法:变体根 `.venv/bin/python -m atelier_core.discipline src`。

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
