# 日志(core/logger.py)

黑匣子:零依赖仅 stdlib,比总线更底层(core 自身依赖它)。三个 sink:

- **控制台**(紧凑单行);
- **订阅流**(进程内扇出 asyncio.Queue → SSE/哨兵消费,慢订阅丢帧不阻塞);
- **hub_logs**(组装点挂 `LogStoreHandler` 落库,黑匣子)。

## 三种使用方式

| 方式 | API | 去向 | 用途 |
|---|---|---|---|
| **分级日志** | `logger.debug/info/warning/error/exception(msg, *a, layer=, component=, phase=, console_only=, exc_info=)` | 控制台 + 订阅流 + hub_logs | 事实/生命周期。`phase` 结构化配对起止(不靠猜文本);`console_only=True` 只上终端、不入库不入流(高频刷屏行) |
| **临时进度** | `logger.progress(msg, layer=, component=, key=, frac=, done=)` | **只订阅流(SSE)**,不落库不进控制台 | 高频 live 进度(下载%/ffmpeg%)。`key` 前端原地刷同一行,`frac`∈[0,1] 渲进度条,`done=True` 清行;无 SSE 订阅者时零开销直返 |
| **结构化中继** | 远程侧 `logger.relay(send, stop)` + 主侧 `logger.reinject(record)` | 远程进程日志 → 网关 → 主进程 replay(字段保真) | **远程域专用**,BusClient 连上即自动接线:保留 layer/component/phase 与进度 key/frac/done,比 supervisor 的 stdout 文本泵强(文本泵仅兜底第三方裸 print) |

## 规矩

- **一律走 logger,禁裸 `print`**(含 `__main__` 入口);
- `layer` / `component` 是每条日志的层/部件标签,必带(内核标 `CORE`,引擎标 `ENGINES`,子进程标 `SUBPROC`);
- engines 纯计算核沉默(调用方记录),重执行体必打生命周期日志(layer=ENGINES)。
