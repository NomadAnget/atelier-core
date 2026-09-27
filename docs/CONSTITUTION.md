# Atelier 宪法(纲领)

> 本目录是 Atelier 体系的**宪法**:架构 / 契约 / 纪律以它为准。各仓 README 只给本地导引。
> 本篇是纲领(宪法两级:机器宪法 M1–M13 + 行为规范 B1–B7;认知隔离;三层结构);机制细节见同目录分篇。

Atelier 只做一件事的地基:**模块间通信的中转站 + 存储枢纽 + 装配注册表**。
一切设计服务于一个目标——**认知隔离**:新增/修改一个域时,需要理解的上下文半径 =
**本域 `header.py` + 本 docs**,不必读其他域的实现。

## 宪法分两级

| 级 | 是什么 | 谁来守 |
|---|---|---|
| **第一级 · 机器宪法** | 机器能判定对错的规则(编号 M1…) | 内核:静态 `atelier_core.discipline` + 组装期 `registry`/`storage` + 运行时闸 |
| **第二级 · 行为规范** | 全部规则 = 第一级 + 机器判不了的(编号 B1–B7) | 写代码的人与 Claude 自律;审查把关 |

**项目级宪法校验一律在内核库里**:域不自带宪法校验,只在自己的 `header.py` 里**声明**(公开件、白名单),
由内核统一裁决。静态检查一条命令:

```bash
.venv/bin/python -m atelier_core.discipline <路径>
```

**自动识别路径**:路径下直接有 `header.py` = **单域模式**(域仓 CI 用);否则按**变体域根**扫全部域(变体 CI 用,
如 `src`)。单域模式看不到其他域,需要全局视野的条目明确报"跳过(由变体检查覆盖)",不算通过也不算失败。
有违规退出码 1。

### 第一级 · 机器宪法

| 编号 | 规则 | 静态(discipline) | 组装期 / 运行时 | 细则 |
|---|---|---|---|---|
| **M1** | 目录即路由:域目录必有 `header.py`,`DOMAIN`=目录名,三件套 `DOMAIN`/`PUBLISHES`/`SUBSCRIBES` 齐全;`EPHEMERAL`/`VOLATILE` 为本域发布子集且互斥 | ✓ 复用 `registry.validate_header` | 组装期同一函数 | [bus.md](bus.md) |
| **M2** | 发布权随目录走:topic 首段=域名、具体路径无通配符;载荷为 `Message` 子类 | ✓ 同上 | 组装期 | [bus.md](bus.md) |
| **M3** | 写权随目录走:表名前缀=域名 | ✓ 复用 `storage.validate_schema` | 运行时 `StoreWindow` 拒越域写 | [storage.md](storage.md) |
| **M4** | 头文件零实现:`header.py` 不定义函数 | ✓ | — | [bus.md](bus.md) |
| **M5** | 契约图不悬空:发布无订阅 / 订阅无发布 / 槽位漏接 | — | 组装期 | [bus.md](bus.md) |
| **M6** | 跨域 import 只放行他域 `header`/`schema` 与他域 header **自己声明的 `PUBLIC`** | ✓(公开件判定需全局视野;单域模式只判 `impl` 越界) | — | [domains.md](domains.md) |
| **M7** | 引擎分级:载模型的代码一律是引擎类并声明 `HEAVY_INFERENCE`(GPU)或 `CPU_ONLY`(须 `RAM_GB`),二者互斥 | ✓ | 类定义期自检 | [domains.md](domains.md) |
| **M8** | GPU 重引擎只准本域与 `pipelines/` 使用 | ✓(需全局视野) | 运行时闸:管线外 `__enter__` 即拒 | [domains.md](domains.md) |
| **M9** | 引擎不常驻:载模型的域,函数/方法不得改写模块级/类级状态(非模型状态登记 `STATE_ALLOWED`) | ✓ | CPU_ONLY 强制子进程、用完即回收 | [domains.md](domains.md) |
| **M10** | 日志禁裸 `print`(含 `__main__` 入口) | ✓ | — | [logging.md](logging.md) |
| **M11** | Linux only:不写平台分支(`os.name`/`sys.platform`/`platform.system()` 判平台) | ✓ | — | — |
| **M12** | 配置显式:未知节/键、类型不符启动即抛错 | — | 启动期 `config.load` | [architecture.md](architecture.md) |
| **M13** | 存储结构漂移:实表与声明不符挂载即抛错,绝不自动改表 | — | 启动期 `storage.start` | [storage.md](storage.md) |

域在 `header.py` 里的可选纪律声明(纯字面量,内核按 AST 读、不执行):`PUBLIC`(M6 公开件)、
`FRAMEWORK_HELPERS`(M7 辅助模块)、`STATE_ALLOWED`(M9 非模型状态)、`VENDORED`(随域携带的第三方代码,免扫)。
规则变更**先改本篇与对应分篇,再改内核执法代码**。

### 第二级 · 行为规范

全集 = 第一级全部 M1–M13 + 以下机器判不了的。前五条是铁律:

- **B1 单一逻辑**:同一逻辑不散落多处。加新实现前先搜现有的。
- **B2 认知隔离**:代码只放对的域;跨域协作走消息,不直调他域实现(M6 只守得住 import,守不住"绕着 import 的直调")。
- **B3 显式失败,不静默兜底**:前置条件缺失就抛清晰错误,不走隐形回退路径。机器只覆盖配置/契约/存储几处(M5/M12/M13),
  代码里的静默降级靠自律;有意设计的降级须写明理由。
- **B4 Linux only**:不做跨平台兼容改动(M11 只拦平台分支,拦不住"为别的平台加的兼容层")。
- **B5 宪法同步律**:契约/规则/纪律的变更,**先改本 docs 对应篇再写代码**——docs + 本域 header 是其他域作者的
  **全部上下文**,docs 落后一步,认知隔离就塌一角。
- **B6 handler 纪律**:毫秒返回、只编排、禁 CPU 重活与同步阻塞、**必须幂等**(见 [domains.md](domains.md) 三种身份)。
- **B7 组装归基础设施**:域只声明"我是什么类型、我怎么构造",绝不接管组装机制(`bus.mount`/网关/子进程生死)。

## 认知隔离

- 改一个域 = 只碰它的 header/schema/impl + 它在 docs 里的段落。跨域协作走消息,不直接调别域实现。
- 域向系统只露三张声明面:`header.py`(总线契约)、`schema.py`(存储)、`build.py`(装配声明,
  可选)。**组装是基础设施的职责**,域只声明"我是什么类型、我怎么构造",绝不接管组装机制
  (`bus.mount` / 网关 / 子进程生死全归基础设施)。

## 三层结构(内核 / 域 / 变体)

| 层 | 是什么 | 仓 |
|---|---|---|
| **内核** | 总线 + 存储枢纽 + 装配注册表 + 组装原语,零业务 | `atelier-core`(uv 依赖) |
| **域** | 消息总线上的自治单元(web/scheduler/task/engines/…) | `atelier-<域>`(git 子模块,挂 `src/<域>`) |
| **管线** | 任务域的可组合插件(video/digest) | `atelier-pipeline-<名>`(子模块,挂 `src/pipelines/<名>`) |
| **变体** | 组装某几个域+管线+前端 = 一个产品 | `atelier-<品类>`(reel-solo / reel-creator / studio) |

内核对"域住哪"一无所知:发现/装配全按 `domain_root` 参数走(变体传自己的包)。详见
[architecture.md](architecture.md)。

## 显式失败

- 配置:未知节/键、类型不符 → 启动即抛错;文件缺失合法(全默认,来源入日志)。
- 契约:声明的图上不允许悬空边(发布无订阅 / 订阅无发布 / 槽位漏接)——组装期红灯。
- 存储:实表结构漂移 → 挂载即抛错,绝不自动改表(报警自动化,动刀人工化)。
- 投递:"发出去 = 一定会被处理到,或显式进死信",没有无声蒸发。

## 分篇导航

- [architecture.md](architecture.md) — 三层结构、目录即路由、domain_root、组装点、新增域清单
- [bus.md](bus.md) — 消息总线:契约/闭环校验/通信分类/投递语义/handler 契约/网关/执行交接
- [storage.md](storage.md) — 存储枢纽:信封vs类型化表、写权、无感自增量、改名迁移
- [domains.md](domains.md) — 域构建准则:三形态、文件结构、三身份、import 白名单、引擎分级与不常驻
- [boot.md](boot.md) — 组装层:build_hub/store、监督器、process 域(保留·默认未用)、composition
- [logging.md](logging.md) — 日志三用法(分级/临时进度/结构化中继)
