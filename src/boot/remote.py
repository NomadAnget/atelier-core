"""process 域子进程通用入口:`python -m src.boot.remote <域>`。

基础设施薄壳——任何 `KIND=process` 的域都用它出进程,不为某个域特化。
组装逻辑全在 composition.run_domain_subprocess(域只在自己 build.py 里声明构造)。
"""

from __future__ import annotations

import asyncio
import sys

from .composition import run_domain_subprocess


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("用法: python -m src.boot.remote <域名>")
    asyncio.run(run_domain_subprocess(sys.argv[1]))


if __name__ == "__main__":
    main()
