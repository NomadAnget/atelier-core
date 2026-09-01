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

## 构成

```
src/
  main.py               根目录唯一文件——组装入口模板;最小 serve()(空总线可起 + --check),
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
    remote.py           process 域子进程入口(python -m src.boot.remote <域>)
```

三层分明:`main.py`(产品接线)· `core/`(内核 + logger/config)· `boot/`(域无关装配层)。

## 定位

**底座,不是完整系统**:不含任何域(studio)。变体在 `src/` 下补自己的 studio 目录,注册表按"目录即路由"自动发现挂载(`core`/`boot`/`services` 是 RESERVED,不当域);无域即空总线。**装配原语在 `boot/`(随内核走);"装配哪些域"在各变体的 `main.py::serve()`(随产品走)——组装点唯一。** 域一律 `from …core import logger`。

## 依赖

轻量,无重型推理栈:`pydantic` / `python-dotenv` / `websockets` / `uvicorn`。Python 3.11。

```bash
uv sync
uv run python -m src.main --check   # 声明自检(空总线=0 studio 亦通过)
uv run python -m src.main           # 起空总线(单端口对外,默认 8000)
```
