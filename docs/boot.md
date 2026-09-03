# 组装层(boot/)

域无关的装配原语,随内核走。变体 `main.py::serve()` 调用它们把域组装成运行系统。

## 装配原语(boot/assembly.py)

| 原语 | 职责 |
|---|---|
| `build_hub(remote_handlers, *, domain_root, journal_db, queue_size, ...)` | 发现域 header → 建总线 → mount 各域 handler → 闭环校验 → 返回 `(bus, windows)`;`fill_noop=True` 占位漏接槽(见 [bus.md](bus.md)) |
| `build_store(store_db, *, domain_root)` | 发现域 schema → 建/对齐表 → 返回 `(store, store_windows)`(每域一个写窗口) |
| `self_test(domain_root)` | `--check`:发现+挂载+存储自检后退出(不起服务),报"N 域挂载 / M topic 闭环 / K 表" |
| `_housekeeping_loop(bus, store, retention)` | 内务循环:死信/账本/日志按 `[retention]` 回收 |
| `_acquire_single_instance_lock(store_db)` | 单实例锁:防两个 hub 抢同一数据目录 |

变体接线套路:`build_hub` → (网关) → `build_store` → 挂 `LogStoreHandler` → 各域服务实例化+注入 →
`create_task` 起循环 → 起监督器 → 起 web。

## 按 KIND 装配(boot/composition.py)

- `load_build(d, root)` / `domain_kind(d, root)`:读域 `build.py` 的 `KIND`。
- `topic_schema_map(root)`:topic→schema 映射(网关远程域绑定用)。
- `run_domain_subprocess(d, root)`:process 域子进程通用宿主(不为任何域特化):开存储枢纽(挂全部
  schema,DDL 幂等)→ 调**同一** `build(ctx)`(bus 句柄换 BusClient)→ 绑 handler → BusClient 接网关领
  订阅槽 → 常驻直至信号。在用 process 域:**publishers**(见 [domains.md](domains.md) 与下文两种生命周期形态)。
- `BuildContext` / `DomainRuntime`:域 `build.py` 与基础设施对接的装配契约类型(域只碰这两个)。

## 子进程监督器(boot/supervisor.py)

非总线公民的哑进程(bgutil、publish-engine 宿主等)由监督器管理(config.toml `[[supervisor.children]]`,
name/cmd 必填):

- **输出接管**:stdout/stderr 逐行 → logger(layer=SUBPROC)→ hub_logs;
- 退出自动重启(指数退避 cap 30s,存活 60s 重置);停机 SIGTERM→宽限→SIGKILL。
- 总线公民(远程域)不归它管——那是网关+心跳公约的辖区。

**懒起子进程(`lazy=true`)**:按需进程的托管形态(process 域用)。不随 start() 拉起,由需求方
`ensure_child(name)` 懒起(幂等);退出**不自动重启**(生死归需求方 `stop_child(name)`);意外退出触发
`set_exit_listener(name, cb)`(需求方收拾残局,如把在跑任务置 failed)。

**动态端口分配(`dynamic_port=true`)——托管边车的通用能力**:凡"端口由我们选、只有本机消费方要连"的哑
边车(如 bgutil POT 供给器),端口不写死、由监督器构造时抓空闲口(`bind(:0)`)替换 `cmd`/`env` 里的
`{port}`,重启复用同口,经 `child_port(name)` 暴露。组装点把端口注入消费域(如 ingest 的 `pot_base_url`)。
**dev/prod 并行各自分到空闲口、永不撞**。边界:仅适用"内部私有口";对外口(web)、连入口(gateway 跨机)
必须固定端口。

**process 域的懒起子进程由基础设施推导注册,不写进 config**:`[[supervisor.children]]` 只登记外部哑进程
(启动命令是部署事实)。process 域启动命令是框架已知的 `python -m atelier_core.boot.remote <域> [域根]`,
组装点发现即自动注册为 lazy 子进程。**加一个 process 域只写它的 build.py,config 零改动。**

## process 域的完整语义

**任何域**只需 `build.py` 声明 `KIND="process"` + `PROCESS={"lazy": True}`,执行即搬进按需子进程:进程退出
= OS 彻底回收(CUDA 上下文/显存碎片/原生堆全清)+ 故障隔离。**契约与调用方零改动**——同一 `build(ctx)`
在主进程或子进程复用,改形态只动 `KIND` 一行。

共性(两种形态都成立):
- **registry 双注册**:主进程也调一次 build(填 registry 元数据供 web 建表单 + local 回退);process 模式下
  这份 handler 挂而不用(被网关转发覆盖);子进程注册后真正执行。
- **崩溃语义**:意外退出触发 `set_exit_listener` → 收拾残局(在跑任务置 failed + 放行队列);配合网关
  "VOLATILE 在途断线放弃" + 收端执行前状态校验(双保险),旧命令不会在新进程复活僵尸执行。
- **handler 轻量律照旧**:子进程里 handler 仍毫秒返回、`create_task` 交 worker(见 [bus.md](bus.md))——
  阻塞子进程事件循环会卡死它自己的其它投递(取消信号/并发请求/心跳)。

生命周期分**两种形态**,按"谁触发这个域"选:

**形态 A — 单一需求方起停**(旧 task 域的模型):存在唯一需求方,以"命令→await finished 期物"
串行派发(见 [bus.md](bus.md) 执行交接)。它派发前 `ensure_child` 懒起、收到 finished 后 `stop_child`
优雅回收(防"自退瞬间恰有新投递在途"的竞态)。**心跳不 enroll**——卡死由需求方既有规则(job_stalled)覆盖。

**形态 B — 网关自动拉起(多需求方 fire-and-forget)**:被**多个域**经 fire-and-forget 消息触发、
无单一需求方握生命周期时(如 publishers:scheduler 发布 / web 登录 / daemons 保活三方触发),生命周期
交给**网关**(`GatewayServer.register_lazy(域, ensure=, stop=, idle_sec=)`,`ensure`/`stop` 委托监督器):
- **按投递自动拉起**:网关 forward 命中未连接的懒域 → 先 `ensure` 懒起、等首连、再投。
- **回收活性 = 传输层 busy 帧,不是执行时长、不是日志**:handler fire-and-forget 后 inflight 归零,
  但后台活还在跑——子进程宿主据域的 `busy()` 钩子(DomainRuntime.busy,有活跃工作返回 True)**周期给
  网关发 `t:"busy"` 传输帧**;网关收到 busy(或业务 publish/ack)刷 `last_active`,**日志 `t:"log"`
  不计活性**(liveness 不搭日志)。**无在飞投递 且 活性静默超 `idle_sec`** 才 `stop` 回收——正在跑
  (登录 holding/发布长静默步)的子进程靠 busy 帧续命,真正空闲才回收。busy 帧是**网关↔子进程的传输
  信号**(作用域=网关),不是业务 topic、不进哨兵、不上账本。
- **进程死活不靠心跳、哨兵不 enroll 进程域**:进程域**短命**(闲时被优雅回收),**不发 `<域>/health/heartbeat`**
  ——那是给**真常驻循环**的活性承诺(哨兵 auto-enroll、沉默=死);进程域发了会让哨兵把"正常闲时回收"
  误报 `heartbeat_silence`。进程域的死活由**掌握其存在性的权威**判定,分两条:①**崩溃/退出**——网关
  ws 断连 / 监督器 `set_exit_listener` 直接感知(需求方收拾:在跑任务置 failed),不需心跳;②**卡死**
  (连着但活不推进)——由**需求方**的执行交接超时判(如 scheduler 发 publish/requested 后久等无
  finished = job_stalled)。哨兵只作这些权威的告警消费端,不自己盯进程域。
- **记账仍走执行交接**:长任务完成经独立 finished(VOLATILE)回触发方记账(publishers/publish/finished
  → scheduler),与形态 A 的 finished 期物同源;网关拉起/回收只管**进程在不在**,不管**任务成没成**。

> 何时整域进程化:纯 Python 重活、无引擎级子进程可依赖,又要完美资源回收+故障隔离时。重推理已在
> **引擎层**做单次推理级子进程隔离(见 [domains.md](domains.md)),故引擎重活的域无需整域进程化。
> 首个在用 process 域 = **publishers**(形态 B);task 域曾用形态 A,2026-08-30 回落 local(引擎层隔离已覆盖)。
