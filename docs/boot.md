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
  订阅槽 → 常驻直至信号。**当前无 process 域**(见 [domains.md](domains.md)),机制保留待用。
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

## process 域的完整语义(保留能力,当前未用)

**任何域**只需 `build.py` 声明 `KIND="process"` + `PROCESS={"lazy": True}`,执行即搬进按需子进程:进程退出
= OS 彻底回收(CUDA 上下文/显存碎片/原生堆全清)+ 故障隔离。**契约与调用方零改动**——同一 `build(ctx)`
在主进程或子进程复用,改形态只动 `KIND` 一行。

- **起停归"需求方"**(哨兵不起停;进程不自作主张退出):需求方派发前 `ensure_child` 懒起,不再需要即
  `stop_child` 优雅回收。防"自退瞬间恰有新投递在途"的竞态。
- **崩溃语义**:意外退出触发 `set_exit_listener` → 需求方收拾(在跑任务置 failed + 放行串行队列);配合网关
  "VOLATILE 在途断线放弃" + 收端执行前状态校验(双保险),旧命令不会在新进程复活僵尸执行。
- **registry 双注册**:主进程也调一次 build(填 registry 元数据供 web 建表单 + local 回退);process 模式下
  这份 handler 挂而不用(被网关转发覆盖);子进程注册后真正执行。
- **心跳不 enroll**:按需短命进程,卡死由需求方既有规则(job_stalled)覆盖。

> 为何回落 local:重推理已在**引擎层**做单次推理级子进程隔离(GPU 回收+崩溃隔离),整域再进程化=重复;
> 且懒下线拖累域内常驻轻量 handler 的即时性。详见 [domains.md](domains.md)。
