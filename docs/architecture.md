# 架构:三层结构 · 目录即路由 · 组装点

## 三层与仓库形态

```
atelier-core (uv 依赖,本仓)          内核:总线 + 存储枢纽 + 注册表 + 组装原语
   ▲
   │ from atelier_core.core import …   (绝对导入)
   │
变体仓 atelier-<品类>                  组装某几域+管线+前端 = 产品
  ├─ src/<域>          → atelier-<域>              (git 子模块;跨域相对导入 from ..域)
  ├─ src/pipelines/<名> → atelier-pipeline-<名>     (子模块;任务域插件)
  ├─ src/services/<x>  → 远程域宿主(如 publish-engine)
  ├─ frontend/         → atelier-frontend            (子模块或 in-repo)
  ├─ main.py           组装点(产品接线,唯一)
  └─ config.toml       基建配置
```

**域是源码子模块,不是可独立安装的 pip 包**:它们靠 `from ..engines`、`from ..task.sdk`
这类**跨域相对导入**协作,要求所有域挂在同一个 `src` 包下。因此域的依赖不会像普通 pip 依赖
那样传播——依赖声明用**各域自带 `requirements.txt` + 变体动态聚合**(见变体 pyproject 的
`dynamic=["dependencies"]` + hatch-requirements-txt)。

## 目录即路由

发现机制扫**变体的包**(`domain_root`):每个子目录 = 一个域,目录名 = 域名 = topic 首段
= 表名前缀。保留名不是域:

```python
RESERVED_DIRS = {"core", "boot", "services", "pipelines"}
#   core=内核;boot=组装层;services=远程域宿主;pipelines=变体管线插件挂载点(非域)
```

域根只放**外界能碰的东西**(header/schema/build + 白名单公开件),私有实现全部收进 `impl/`
(见 [domains.md](domains.md))。无域 = 空总线(内核自身 `--check` 即 0 域自检)。

## domain_root 参数化(内核与变体解耦的关键)

内核对"域住哪"零假设。发现/装配全部接收 `domain_root`:

| 层 | 接口 | 变体怎么传 |
|---|---|---|
| registry | `discover_headers(root)` / `discover_schemas(root)` / `load_header(d, root)` / `load_schema(d, root)` | — |
| boot.assembly | `build_hub(..., domain_root=)` / `build_store(..., domain_root=)` / `self_test(domain_root)` | `DOMAIN_ROOT = __package__` |
| boot.composition | `load_build(d, root)` / `domain_kind(d, root)` / `topic_schema_map(root)` / `run_domain_subprocess(d, root)` | — |
| boot.remote | `python -m atelier_core.boot.remote <域> [域根]` | argv |

变体的 `main.py` 顶部 `DOMAIN_ROOT = __package__`,内核据此扫本变体的域——**内核代码永不因变体而改**。

## 组装点唯一:变体的 main.py::serve()

> 老单体是"唯一 `src/main.py`";三层化后 **composition root 下沉到每个变体**,内核只提供
> `boot` 里的域无关**装配原语**。组装点仍唯一——就是**本变体的 `main.py`**。

全系统只有变体 `serve()` 里允许:创建总线/存储枢纽实例(经 `build_hub`/`build_store`)、
接线各域 handler、实例化域服务并注入依赖、`create_task` 启动自有循环、起网关/监督器。

```python
# 变体 main.py::serve() 示意(真实形态见各变体)
DOMAIN_ROOT = __package__
bus, windows       = await build_hub(remote_handlers, domain_root=DOMAIN_ROOT, ...)
store, store_wins  = await build_store(cfg.paths.store_db, domain_root=DOMAIN_ROOT)
svc["scheduler"]   = JobSchedulerService(windows["scheduler"], store, store_wins["scheduler"], ...)
tasks = [asyncio.create_task(svc["scheduler"].run_worker()), ...]
```

域自己**绝不**:new 总线/枢纽、自行 mount、在 import 时产生副作用(开连接/起任务/读文件)。
域不知道自己运行在哪个进程——这正是将来拆进程/换载体时域代码零改动的前提。

## 新增一个域的完整清单

1. 建仓 `atelier-<domain>` + `__init__.py` 一句话说明;
2. 写 `header.py`:声明发布(含 schema)与订阅;纯遥测 topic 进 EPHEMERAL;
3. (需持久数据)写 `schema.py`:信封表/类型化表 + 提升列声明;
4. (local/process 域)写 `build.py`:`KIND` + `build(ctx)`;或直接在变体 `main.py` 接线;
5. 写 `impl/service.py`:标准形态服务类,handler 全部幂等;实现随便拆,域根只留声明面;
6. 变体挂子模块 `src/<domain>` + 在 `serve()` 接线 + `requirements.txt` 列本域依赖;
7. `python -m src.main --check` 自检(声明校验自动覆盖)+ 观测台确认拓扑边如预期。

改动半径 = 本域仓 + 变体 `main.py`/`.gitmodules` + 测试。**碰到任何第三个位置,说明设计错了。**

## 基建配置(config.toml)

**边界铁律:TOML 只管"系统怎么搭起来",永远不装运行时配置。**

| | 基建配置(config.toml) | 运行时配置 |
|---|---|---|
| 内容 | 库路径、端口、总线参数、日志级别、哨兵阈值、models_dir | 声纹、监控频率、发布目标等业务旋钮 |
| 谁改 | 运维改文件,重启生效 | 用户在前端改,立即生效 |
| 存哪 | 文件(`core/config.py` 仅 stdlib 加载) | **存储枢纽,归拥有域**(域的设置表) |

判定口诀:**组装点组装时需要的进 TOML;组装完之后才有人关心的是域数据。**

⚠ 第三类:**组装期确立的运行期事实**(网关实际端口等)。它对创建者像配置、对消费者是既成事实——
把它当配置复制给消费者,就会出现"两份配置不一致 → 消费者连错"(见 [boot.md](boot.md) 引导契约的
2026-09-27 实录)。这类值**由确立方在拉起时显式传给消费方**,不靠消费方各自读配置。
内核 config 节:`paths / web / gateway / supervisor / bus / logger / logstore / sentinel / retention`
(见 `core/config.py`;`web.dist_dir` = 变体前端构建产物目录)。运行时旋钮走 `core/settings.py`
底座(见 [storage.md](storage.md) 的设置基座段)。
