"""总线载荷契约基类。

所有跨域消息必须是 Message 的子类:不可变、禁未声明字段——schema 即契约。
消息定义写在**拥有域**的 header.py 里(谁发布谁定义);订阅方实现可以
import 发布方 header 取 schema,这是契约级依赖,不是实现耦合。
"""

from __future__ import annotations

import typing
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field
from pydantic_core import PydanticUndefined


def Setting(default: Any = PydanticUndefined, *, label: str, widget: str = "auto",
            options: list | None = None, secret: bool = False, hidden: bool = False,
            help: str = "", default_factory: Any = None) -> Any:
    """声明一个"自带 UI 元数据"的设置字段——中文名/控件/选项/密钥/隐藏全随字段就地声明,
    前端设置页据此渲染,元数据不再前后端分散(见 README「设置项=自带 UI 元数据的完整声明」)。

    label    中文名;
    widget   控件形式:auto(前端按值类型推)/text/password/select/number/toggle/textarea/voiceprint;
    options  枚举下拉 ``[[值, 显示名], ...]``(widget=select);
    secret   密码框遮罩(眼睛切换明文);hidden  不在通用设置页出(有专属 UI 的字段);help 辅助说明。
    default_factory 用于 list 等可变默认(与 default 二选一)。
    """
    ui = {"label": label, "widget": widget, "options": options,
          "secret": secret, "hidden": hidden, "help": help}
    if default_factory is not None:
        return Field(default_factory=default_factory, json_schema_extra={"ui": ui})
    return Field(default, json_schema_extra={"ui": ui})


def _type_name(ann: Any) -> str:
    """字段注解 → 前端可读的值类型(widget=auto 兜底选控件用)。"""
    if typing.get_origin(ann) in (list, tuple):
        return "array"
    return {bool: "boolean", int: "integer", float: "number", str: "string"}.get(ann, "string")


def settings_ui_spec(model: type[BaseModel]) -> dict[str, dict]:
    """从 settings ``Record`` 提取每字段的 UI 规格(snapshot 附给前端)。未用 ``Setting`` 声明的
    字段走默认(label=字段名、widget=auto),仍可渲染——渐进迁移不崩。"""
    out: dict[str, dict] = {}
    for name, fi in model.model_fields.items():
        extra = fi.json_schema_extra if isinstance(fi.json_schema_extra, dict) else {}
        ui = dict(extra.get("ui") or {})
        ui.setdefault("label", name)
        ui.setdefault("widget", "auto")
        ui["type"] = _type_name(fi.annotation)
        out[name] = ui
    return out


class Message(BaseModel):
    """总线消息基类:frozen + extra=forbid,发布时按声明的 schema 强校验。"""

    model_config = ConfigDict(frozen=True, extra="forbid")


class Record(BaseModel):
    """存储载荷基类:域在 schema.py 里声明"data 列的形状"。

    与 Message 同一套思想——形状即契约,写入时按声明强校验。
    演进规则同消息:只增字段且必须带默认值,老行读出走默认值,永不改列。
    """

    model_config = ConfigDict(frozen=True, extra="forbid")


class Heartbeat(Message):
    """心跳公约标准载荷。

    公约:常驻循环的域发布到 ``<域>/health/heartbeat``(声明为 EPHEMERAL),
    载荷自带间隔承诺;哨兵按 ``interval_sec × 宽限倍数`` 通用判定沉默,
    新域只要遵循公约即自动纳入监护,哨兵零改动。
    """

    interval_sec: float
    note: str = ""
