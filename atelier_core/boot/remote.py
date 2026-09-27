"""process 域子进程通用入口:`python -m atelier_core.boot.remote <域> [域根] [网关地址]`。

基础设施薄壳——任何 `KIND=process` 的域都用它出进程,不为某个域特化。
组装逻辑全在 composition.run_domain_subprocess(域只在自己 build.py 里声明构造)。
"""

from __future__ import annotations

import asyncio
import sys

from .composition import run_domain_subprocess


def main() -> None:
    if len(sys.argv) not in (2, 3, 4):
        raise SystemExit("用法: python -m atelier_core.boot.remote <域名> [域根] [网关地址 host:port]")
    domain = sys.argv[1]
    root = sys.argv[2] if len(sys.argv) >= 3 else None
    # 网关地址由拉起方显式给出(引导契约,见 docs/boot.md)。缺省=回落读配置,仅为兼容手工调试;
    # 正常路径下 assembly 必传,子进程不该自己去猜网关在哪。
    gateway = sys.argv[3] if len(sys.argv) == 4 else None
    asyncio.run(run_domain_subprocess(domain, root, gateway))


if __name__ == "__main__":
    main()
