# 存储枢纽(无感自增量数据库)

内核 `core/storage.py`。与消息总线完全对称的第二根支柱。

| | 消息 | 存储 |
|---|---|---|
| 声明文件 | `header.py` | `schema.py`(无持久需求的域可无) |
| 契约基类 | `Message` | `Record`(data 列的形状) |
| 权限 | 发布权随目录走 | **写权随目录走**(表名前缀=域名),读自由(`hub.read`) |
| 窗口 | `Window.publish` | `StoreWindow.insert/update/delete` |

## 两种表规格

**信封表**(列结构永久冻结,业务字段全在 data JSON):`id / kind / data / created_at / updated_at`。
声明 `TABLES = {"<域>_<复数名词>": {"kinds": [...], "indexes": [...], "unique": [...]}}`。

**类型化表**(高频大表用):声明 `columns`(真实列)而非 kinds,写口 `insert_row/update_row`
(列白名单校验),写权/读权规则与信封表相同。jobs/logs/channels 等按此规格。

## 无感自增量的三层(其他节点演进存储,永远不改内核 storage.py)

1. **建表自动**:挂载时 `CREATE TABLE`(只建缺失的);
2. **字段自动**:改自己的 Record 模型即可,演进规则=**只增带默认值的字段**,老行读出走默认;
   设置基座对**删字段**同样容错(旧行多余键投影剥离并自愈回写);
3. **索引自动**:schema 声明的 `indexes` data 字段 → 生成列(VIRTUAL)+ 索引,`unique` → 唯一索引。
   ADD COLUMN 是唯一天然安全的 ALTER,故可自动化。类型化表 NOT NULL 无默认列 → 挂载即抛错要求人工迁移。

**演进规则的两半**:模型只增带默认值的字段;读取端必须 `model_validate` 后返回(老行缺新字段走默认)
——改字段类型是违规,会留脏行。

## 声明式改名迁移(`renamed_from`)

表跨域迁移/更名时在新声明标注旧名——枢纽启动见"新名缺、旧名在"即 `ALTER RENAME`(数据原地保留,
旧名索引清理重建);新旧同在则显式报错(不猜真相源,人工裁决)。这是"声明的迁移",不是自动猜测。

## 数据访问规范

- 写:只经本域 `StoreWindow`(insert/update/delete),只写本域声明的表;
- 读:`hub.read(table, ...)` 全域开放,跨域随便查;热字段过滤用拥有域声明的提升列;
- 强类型:`拥有域.schema.SomeRecord.model_validate(row["data"])`;
- **禁止自建 sqlite 连接/自开数据库文件——存储只有枢纽这一个入口。**

## 通用查询端点(web 域,零注入面)

`GET /api/store/{table}?col=v&order=col:desc&limit=n` 只接受**声明过的可滤列**(索引列 + kind/id),
其余 400。`?distinct=col` 取某列去重取值集(前端筛选器动态取值)。加业务能力=改声明:域加表→前端立刻能查。

## 运行时设置基座(core/settings.py)

各域设置 = 本域信封表单行(如 `task_digest_settings` / `daemons_settings` / `engines_settings`)。
统一走 `SettingsStore`(get 首读播种、update 校验合并、`peek` 读方快照不落种)+ `edit_handler`
(生成按域过滤的标准订阅处理器,纯声明域组装点一行接线)。

- 读 = `/api/store/<表>`;写 = `web/settings/edit_requested`(domain+字段补丁),各设置拥有域订阅同一
  topic 过滤后经自己的 Settings 模型 `model_validate` 合并。非法字段/类型 → pydantic 抛错进死信(显式拒绝)。
- **设置项 = 自带 UI 元数据的完整声明**:字段用 `Setting(default, *, label, widget, options, secret,
  hidden, help)` 就地声明(`core/contracts.py`);`widget` ∈ text·password·select·number·toggle·
  textarea·voiceprint,`widget="auto"` 才按值类型推控件。`snapshot` 对 kind=settings 的表附上
  `settings_fields` + `settings_label`,**前端设置页纯按规格渲染,零硬编码元数据**。加/改设置项
  只动后端 Record 一处,前端自动跟随。

## 内核表

`bus_journal` / `bus_deliveries`(账本)与 `hub_logs`(日志)由内核直管(owner=kernel,不属任何域):
组装时 `attach_kernel_table` 挂接,读/快照经枢纽,写由内核组件自持连接。日志落库 = `LogStoreHandler`
(独立写线程攒批,记录路径零阻塞、零总线依赖——黑匣子录到停机最后一刻)。

## 并发 / 多进程语义

- **并发**(事件循环零阻塞):写串行(asyncio.Lock,线程中执行);读并行(WAL + 每次读独立只读连接,
  不持写锁);`snapshot()` async 同读路径。
- **多进程**(process 域落地时主库是多进程库):WAL 天然支持"多读者 + 串行写者",所有连接带
  `busy_timeout`(跨进程写锁竞争是等待而非立抛 SQLITE_BUSY)。子进程各自实例化 StorageHub 挂同一
  store.db(DDL 幂等对齐);**写窗口纪律跨进程不变**(每进程只发它有权写的域窗口)。跨进程写不经彼此
  `_notify`,故 SSE/store 观察者只见本进程写入(检查点等非实时展示数据,可接受)。

## 显式边界

实表信封漂移(列缺失/结构不符)→ 挂载即抛错,绝不自动改表(**报警自动化,动刀人工化**)。
