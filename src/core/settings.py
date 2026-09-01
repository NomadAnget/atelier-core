"""运行时设置底座 —— "单行信封设置"的统一实现(此前 task/daemons/platform_adapters
各手抄一份,第四个消费者进场时抽取归一;单一逻辑原则)。

职责恰好三件,与各域手抄语义逐条对齐:
  · get():读单行 → ``model_validate``(演进规则:老行缺新字段走默认)→ 无行则
    **播种默认值**(加锁复查——多循环启动即并发首读,无锁会各插一行,取 daemons 版语义);
  · update(patch):经模型校验合并后落库(非法字段/类型显式抛错,进死信);
  · peek()(静态):**读方视角**——读到就校验返回,读不到返回代码默认,**不落种**
    (写权随目录,非拥有域没有写窗口;播种是拥有域的事)。

edit_handler() 生成 ``web/settings/edit_requested`` 的标准订阅处理器(按 domain 过滤),
纯声明域(如 engines,库本体无服务)在组装点一行接线即可成为设置拥有域。
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Type

from . import logger
from .contracts import SECRET_PLACEHOLDER, Record, secret_field_names
from .storage import StorageHub, StoreWindow


class SettingsStore:
    """域运行时设置的单行信封仓(拥有域持写窗口)。"""

    def __init__(self, hub: StorageHub, store_win: StoreWindow, table: str,
                 model: Type[Record], *, layer: str, component: str = "Settings") -> None:
        self._hub = hub
        self._store = store_win
        self._table = table
        self._model = model
        self._layer = layer
        self._component = component
        self._seed_lock = asyncio.Lock()

    async def get(self) -> dict[str, Any]:
        rows = await self._hub.read(self._table, kind="settings", limit=1)
        if rows:
            data = rows[0]["data"]
            # 演进容错:模型删了字段时旧行仍带被删键,extra=forbid 会砸校验——
            # 先投影到当前模型字段集(与"加字段自愈"对称)。
            known = set(self._model.model_fields)
            clean = {k: v for k, v in data.items() if k in known}
            merged = self._model.model_validate(clean)
            out = merged.model_dump()
            if set(out) - set(data) or set(data) - known:
                # 自愈回写:加了新字段(老行缺)或删了旧字段(老行多余)。前端设置页读
                # 的是原始行(通用查询,不经模型),不回写就看到的字段与模型不一致。
                await self._store.update(self._table, rows[0]["id"], merged)
                logger.info("设置行按模型对齐 表=%s +%s -%s", self._table,
                            sorted(set(out) - set(data)), sorted(set(data) - known),
                            layer=self._layer, component=self._component)
            return out
        async with self._seed_lock:
            rows = await self._hub.read(self._table, kind="settings", limit=1)  # 锁内复查
            if rows:
                known = set(self._model.model_fields)
                clean = {k: v for k, v in rows[0]["data"].items() if k in known}
                return self._model.model_validate(clean).model_dump()
            seed = self._model()
            await self._store.insert(self._table, "settings", seed)
            logger.info("运行时设置首次落种 表=%s(默认=模型代码内默认值)", self._table,
                        layer=self._layer, component=self._component)
            return seed.model_dump()

    async def update(self, patch: dict[str, Any]) -> dict[str, Any]:
        # 密钥脱敏回写:secret 字段收到占位符 = 前端未改动(它拿到的就是占位符)→ 剔除该键,保持原值不覆盖。
        secret = secret_field_names(self._model)
        patch = {k: v for k, v in patch.items()
                 if not (k in secret and v == SECRET_PLACEHOLDER)}
        cur = await self.get()
        merged = self._model.model_validate({**cur, **patch})
        rows = await self._hub.read(self._table, kind="settings", limit=1)
        await self._store.update(self._table, rows[0]["id"], merged)
        return merged.model_dump()

    @staticmethod
    async def peek(hub: StorageHub, table: str, model: Type[Record]) -> dict[str, Any]:
        """读方快照:有行经模型校验返回,无行返回代码默认;绝不写(写权在拥有域)。"""
        rows = await hub.read(table, kind="settings", limit=1)
        if rows:
            known = set(model.model_fields)
            clean = {k: v for k, v in rows[0]["data"].items() if k in known}
            return model.model_validate(clean).model_dump()
        return model().model_dump()


def edit_handler(store: SettingsStore, domain: str) -> Callable[[Any], Any]:
    """标准设置补丁处理器:web/settings/edit_requested 按 domain 过滤 → 校验合并落库。"""

    async def on_settings_edit(env: Any) -> None:
        p = env.payload
        if p.domain != domain:
            return
        await store.update(dict(p.values))
        logger.info("%s 设置已更新:%s", domain, "、".join(sorted(p.values)),
                    layer=domain.upper(), component="Settings")

    return on_settings_edit
