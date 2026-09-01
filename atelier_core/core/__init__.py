"""内核包 core:所有域建于其上的地基。三块——消息总线(core/contracts/journal/
gateway,路由/投递)+ 存储枢纽(storage/settings/log_store)+ 装配注册表(registry,
目录即路由的发现/校验)。消息总线部件仍名 Bus;本包不是域(在 RESERVED_DIRS 里)。

域目录与本包平级挂在 src/ 下;组装入口见 src/main.py。
"""

from .contracts import Message  # noqa: F401
from .core import Bus, BusError, Envelope, Window, topic_matches  # noqa: F401
from .registry import BusDeclarationError, discover_headers, load_header  # noqa: F401
