# CLAUDE.md

本文件给 Claude Code 在 **atelier-core** 仓工作时的导引。

## 这是什么

**Atelier 内核** —— 消息总线 + 存储枢纽 + 装配注册表 + 组装原语。所有产品变体共享的地基,**零业务逻辑**。
变体(`atelier-<品类>`)组装若干域(`atelier-<域>` 子模块)+ 管线 + 前端成为完整产品;本仓只提供他们
建于其上的机制。

## 宪法在 `docs/`(改机制前必读)

`docs/` 是 **Atelier 体系的宪法**(架构/契约/纪律的唯一真源),不是本仓一家的说明:

- [docs/CONSTITUTION.md](docs/CONSTITUTION.md) — 纲领:铁律 + 认知隔离 + 三层结构 + 宪法同步律
- [docs/architecture.md](docs/architecture.md) — 三层、目录即路由、domain_root、组装点、新增域清单
- [docs/bus.md](docs/bus.md) — 总线:契约/闭环校验/投递语义/handler 契约/网关/执行交接
- [docs/storage.md](docs/storage.md) — 存储枢纽:两种表、写权、无感自增量、设置基座
- [docs/domains.md](docs/domains.md) — 域构建:三形态、文件结构、import 白名单、重推理闸
- [docs/boot.md](docs/boot.md) — 组装层:build_hub/store、监督器、process 域(保留·未用)
- [docs/logging.md](docs/logging.md) — 日志三用法

## 铁律(全文见 docs/CONSTITUTION.md)

1. **单一逻辑**:加实现前先搜现有的。
2. **认知隔离**:代码只放对的域;跨域走消息不直调;import 纪律机器封禁(**先改测试白名单再改代码**)。
3. **重型推理只在管线内**(静态 AST + 运行时闸双重强制)。
4. **显式失败,不静默兜底**。
5. **Linux only**。
6. **宪法同步律**:契约/规则/纪律变更,**先改 `docs/` 对应篇再写代码**。

## 命令

```bash
uv run python -m atelier_core.main --check              # 本仓自检(空总线,0 域)
uv run python -m unittest discover -s tests -p 'test_*.py'  # 内核测试套件
uv build                                                # 构建 wheel/sdist
```

**测试套件 `tests/`**(自旧 `Carnation/Architecture@hub` 抽取内核时迁入):内核数据面全覆盖——
总线持久/重投/死信(bus_durability)、网关 forward/ack/重连/鉴权/**懒进程域**(gateway)、存储枢纽
信封表/类型化表/写权/无感自增量(store_prototype/typed_store)、config(hub_config)。**改内核先跑它防回归**。
重域依赖的集成测试(bus_prototype/settings_store/supervisor 用真域当 fixture)+ log-store/housekeeping
e2e 待造假域/适配组装根后补(暂 skip;域级测试归各 `atelier-<域>/tests/`)。

## 发版(变体按版本约束消费,不再钉 commit)

内核以 **Forgejo PyPI 包**分发,**自动发版**:改动后只需 **bump `pyproject.toml` 版本 → push master**
→ `.forgejo/workflows/publish.yml` 自动 `uv build` + `uv publish` 到 Forgejo PyPI(幂等:版本已发过则跳过,
所以**必须先 bump version** 才会发新版;走 LAN debian.lan 绕 WAF;认证=用户级 secret `Ateliers`,含 package 权)。
→ 变体 `uv sync` 按 `atelier-core>=x.y` 自动升。**不要手动 `uv publish`**——bump+push 即可。
GitHub 仓**不构建、不发包**:`.github/workflows/release-link.yml` 在版本 bump push master 时只建一条 Release
`v<version>`(无包文件,正文链到 Forgejo PyPI 该版本,幂等);包的唯一真源是 Forgejo。
(应急手动:`uv build && uv publish --publish-url http://debian.lan:3257/api/packages/Carnation/pypi -u Carnation -p <package-token>`)

**禁止**未经用户明确确认改依赖钉版策略。临时产物写项目 `tmp/`,不用系统 `/tmp`。
提交信息末尾:`Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`;用户未主动要求不自动 commit。

## 边界

- 内核对"域住哪"零假设——发现/装配全按 `domain_root` 参数走,**内核代码永不因某个变体而改**。
- 内核依赖保持轻量(pydantic/dotenv/websockets/uvicorn),**不引入任何重型推理栈**(那是引擎域的事)。
- `core/` `boot/` `services/` `pipelines/` 是 RESERVED,不是域。
