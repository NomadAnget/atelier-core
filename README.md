# gcp-core

**GCP 纯基础设施底座** —— 消息总线 + 存储枢纽 + 装配注册表。零业务逻辑,供多个产品仓库作平台基座复用(从 [Architecture](http://debian.lan:3257/Carnation/Architecture) 抽出,保留相关提交历史)。

## 构成

```
src/
  core/                 内核(地基,RESERVED,不是域)
    core.py             消息总线内核——目录即路由、投递语义、闭环校验
    contracts.py        总线载荷契约基类(Message)
    journal.py          持久账本——"发出去 = 落盘了"
    storage.py          存储枢纽——无感自增量、信封表、按域写权
    settings.py         运行时设置底座(单行信封设置)
    registry.py         装配注册表——头文件发现 + 声明校验
    log_store.py        内核日志落库(hub_logs 类型化表)
    gateway/            网关——远程域 ws:// 桥(client/server)
  main.py               组装入口:发现全部域 → 挂载 → 启动
  composition.py        按 KIND 装配(local / process / remote)
  supervisor.py         子进程监督器(拉起 / 看活 / 停机收编)
  config.py             基建配置加载(仅 stdlib tomllib)
  logger.py             独立日志器——黑匣子
  remote.py             process 域子进程入口
```

## 定位

这是**底座,不是完整系统**:不含任何域(`task/daemons/engines/web/...`)。消费方按"目录即路由"在 `src/` 下补自己的域目录,注册表自动发现挂载。无域时 = 空总线(可 import、可起、挂载 0 个域)。

## 依赖

轻量,无重型推理栈:`pydantic` / `python-dotenv` / `websockets` / `uvicorn`。Python 3.11。

```bash
uv sync
uv run python -m src.main   # 起空总线(单端口对外,默认 8000)
```
