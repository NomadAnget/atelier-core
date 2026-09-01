"""process 域子进程通用入口:`python -m atelier_core.boot.remote <域> [域根]`。

基础设施薄壳——任何 `KIND=process` 的域都用它出进程,不为某个域特化。
组装逻辑全在 composition.run_domain_subprocess(域只在自己 build.py 里声明构造)。
"""

from __future__ import annotations

import asyncio
import sys

from .composition import run_domain_subprocess


def main() -> None:
    if len(sys.argv) not in (2, 3):
        raise SystemExit("用法: python -m atelier_core.boot.remote <域名> [域根]")
    domain = sys.argv[1]
    root = sys.argv[2] if len(sys.argv) == 3 else None
    asyncio.run(run_domain_subprocess(domain, root))


if __name__ == "__main__":
    main()
