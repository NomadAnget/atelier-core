# 消息总线

内核 `core/core.py`。契约在 header 声明,组装期闭环校验,持久投递默认。

## 契约声明(header.py)

- `DOMAIN`:域名(= 目录名 = topic 首段)。
- `PUBLISHES`:`{topic: MessageSubclass}`,只有本域能发布首段=域名的 topic。
- `SUBSCRIBES`:`{pattern: 槽位名}`,通配 `+`(单段)/ `#`(子树,仅末段),如 `scheduler/#`。
- `EPHEMERAL` / `VOLATILE`:量纲声明(见下)。
- **消息即契约**:载荷是 `Message` 子类(frozen、禁额外字段),定义写在拥有域 header;
  订阅方只允许 import 发布方的 **header**,禁 import 实现。头文件零实现。

## 组装期闭环校验

挂载全部域后,三向闭环,任一悬空即拒绝启动(`BusDeclarationError`):

1. **每个持久 topic 必须有 ≥1 订阅匹配**——"发布即蒸发"从运行时沉默变成组装期红灯;
   `EPHEMERAL` 豁免,`VOLATILE` **不豁免**(命令发进虚空=红灯)。
2. **每个订阅必有发布方**(反向)。
3. **本地域 header 的每个订阅槽位,必须在组装点接到真实处理器**;远程域
   (`cfg.gateway.remote_domains`)经网关绑定,豁免本检。

> `fill_noop=True` 是自检/移植期占位:会把漏接的槽**静默吞成空转**(消息签收 done、零报错、
> 处理器从未执行)——生产上最阴险的失效形态。历史案例:`daemons.on_job_deleted` 漏接,
> 视频溯源零清理累积 617 条悬空、投递账 625 次全 done 无一生效。变体接线务必接全。

## 通信分类(什么进总线,什么不进)

| 类型 | 走什么 |
|---|---|
| 命令/事件(异步,"发生了什么") | 总线 |
| 查询(同步要答案:配置、仓储读写) | 门面直调,不进总线 |
| 媒体大文件 | 文件系统(data/jobs),消息里只传路径 |
| 管线内部 s01→s12 | 直调,task 域整体挂总线 |

## 投递语义（"发出去 = 一定会被处理到,或显式进死信"）

- **持久投递(默认)**:publish 返回前,消息+每个匹配订阅的投递账在单事务落盘
  (`core/journal.py`);成功销账(ACK);**崩溃重启自动重投**未销账投递(原序)——
  at-least-once,**处理器必须幂等**(`env.msg_id` 重投间恒定,可作去重键)。
- **重试+死信**:处理器抛错自动退避重试(0.5s×2ⁿ,默认 3 次),超限进死信
  (`bus.replay_dead()` 在线重放 / `discard()` 销账),绝不无声蒸发。
- **背压取代丢弃**:队列满时发布方等待(默认 30s 超时抛 `BusError`)。持久消息即使入队超时
  也已在账,重启必达。**没有丢弃这回事。**

### 三种投递量纲(header 显式声明,默认持久)

| 类别 | 落账本 | 崩溃重投 | 重试/死信 | 悬空校验 | 计数归属 |
|---|---|---|---|---|---|
| 持久(默认) | 是 | 是(at-least-once) | 是 | 受约束 | 发布 + 投递 |
| `VOLATILE` | 否 | 否(**at-most-once**) | 否 | **受约束** | 发布 + 投递 |
| `EPHEMERAL` | 否 | 否 | 否 | 豁免 | 遥测(**不计投递**) |

- **EPHEMERAL = 纯遥测**(进度/心跳/发布态):推高"遥测"而非"发布/投递";即便有真订阅者,
  签收也不计工作投递(否则高频遥测淹没投递账,量纲失真)。
- **VOLATILE = 易失命令/事件**(真工作但**不可幂等重放**,如执行交接):跳过账本、崩溃即丢、
  至多一次;但计入发布与投递、受悬空校验约束。用途:执行不可自动重放(重跑=重下载/腐化产物),
  又不愿被误读成遥测而在观测台隐身。
- 每订阅独立队列+消费任务:慢/抛错处理器不拖垮他人;换 RabbitMQ 时语义一一对应
  (persistent / ACK / DLX)。
- 组装:生产必须传 journal(`build_hub(journal_db=...)`);单测可不传(全 EPHEMERAL 语义,仍背压无损)。

## 处理器契约(handler 轻量律)

**handler 里只准编排,不准干活。** 订阅处理器跑在事件循环上,必须毫秒级返回:

- 允许——建任务入队、读写存储枢纽(内部已线程化)、续发消息、改内存态;
- 禁止——CPU 重活(模型推理/媒体处理)、同步阻塞(requests / time.sleep / 同步大 IO)。
- 重活的正确姿势:handler 只投递(入队 / `asyncio.create_task` 交给 worker),worker 在
  线程池/独立进程执行。违反的代价:整个事件循环(所有域的消息投递)一起卡死。
- **必须幂等**(at-least-once 重投,`env.msg_id` 作去重键);续发的 `correlation_id`
  由内核 contextvar 自动透传。

## 任务执行交接(scheduler ↔ task,经总线)

排队/状态机/终态记账归 scheduler(拥有 `scheduler_jobs`);管线执行(重活)归 task。二者不进程内
直调 runner,改由总线交接(为 task 出进程预留,执行链对总线透明):

1. scheduler worker 取 queued → 置 running → 发 `scheduler/job/run_requested`(VOLATILE,载荷含 job 行)
   → **await 一个 finished 期物**(串行:未回 finished 绝不取下一个);
2. task 订阅 → 查注册表取 runner → 线程池跑管线(重推理闸在管线上下文放行)→
   成功/取消/失败一律发 `task/job/finished`(VOLATILE,outcome + result/error);
3. scheduler 订阅 finished → 落终态(succeeded 触发发布出闸)+ 解期物放行 worker
   (finished 不可重放 → at-most-once:已终态即忽略;worker 放行由 `finally` 兜底);
4. 进度:task 发 `task/job/progress`(EPHEMERAL)→ scheduler 落 `jobs.progress` 并转发;
5. 运行中取消:scheduler 发 `scheduler/job/cancel_signal`(EPHEMERAL)→ task 置协作取消标志,
   runner 的 `should_cancel` 探针命中即抛 `JobCancelled`。

崩溃即 running→failed 由重启对账兜底 + 人工重启,**绝不自动重投执行**。runner 由 task 注册进
`scheduler/registry`(import 白名单特例,插座只碰注册函数)。

## 总线网关(远程域:子服务 = 域的异地实现)

**域的契约与实现所在进程解耦**:契约永远只声明一份;实现可住主进程,也可住别进程/别语言——
后者经 WebSocket 网关接入,成为"远程域"。"子服务"不是架构概念,只是部署事实。

接入:config.toml `[gateway]` 开启并列 `remote_domains`;组装点把这些域的订阅槽绑成网关转发;
子服务用客户端库(`core/gateway/client.py`,语言无关)连上、声明域名,即获完整总线公民资格。

语义与本地同源(有测试钉死):远端 **ack 才销账**;断线订阅暂停、在途投递重连原样重发不烧重试次数
(幂等军规覆盖);**例外——VOLATILE 在途断线即放弃**(at-most-once),但**首连等待不算放弃**
(冷启动窗口仍是首投);跨窗口陈旧命令由**收端执行前状态校验**兜底。心跳:远端 publish 心跳公约
topic,哨兵 auto-enroll。安全:默认只绑 127.0.0.1,token 鉴权,每域单连接。

**网关托管 process 域生命周期(形态 B,见 [boot.md](boot.md))**:同一网关既转发 remote 域(外部常驻
实现),也可**按需拉起/回收** process 域子进程——用于被多需求方 fire-and-forget 触发、无单一需求方握
生命周期的 process 域。`register_lazy(域, ensure=, stop=, idle_sec=)`:forward 命中未连接的懒域先
`ensure` 懒起再投;**活性判据 = 子进程上行流量(心跳/publish/log),不是执行时长**——子进程有活跃任务时
发心跳公约续命,**无在飞投递且活性静默超 `idle_sec`** 才 `stop` 回收(正在跑的登录/发布远超 ack_timeout
也不误杀)。`ensure`/`stop` 委托监督器懒起子进程,网关只判"该不该在、还活着没"。

**发布语义铁律**(publishers,现为 process 域):业务失败(平台拒稿/超时)= `finished(ok=False)` 并 ack,
绝不 nack(重试即重复投稿事故);只有载荷缺 packet 才 nack 进死信。
