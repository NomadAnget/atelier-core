"""总线载荷契约基类。

所有跨域消息必须是 Message 的子类:不可变、禁未声明字段——schema 即契约。
消息定义写在**拥有域**的 header.py 里(谁发布谁定义);订阅方实现可以
import 发布方 header 取 schema,这是契约级依赖,不是实现耦合。
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


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
