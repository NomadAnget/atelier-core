"""子进程监督器(组装层)—— 哑子进程的拉起 / 看活 / 日志接管 / 停机收编。

管辖对象:非总线公民的进程(bgutil、publish-engine 的宿主进程等)。
总线公民(远程域)经网关获得监护;这里只管"不会说话"的:
  · stdout/stderr 逐行接管 → logger(layer=SUBPROC, component=进程名)→ hub_logs
    ——子进程的内部执行细节从此和主系统日志同表可查(黑箱第一层壳);
  · 退出自动重启:指数退避(0.5s×4ⁿ,cap 30s),存活满 60s 重置退避;
  · 优雅停机:SIGTERM → 宽限 10s → SIGKILL;stop() 后不再重启。

声明来自 config.toml [[supervisor.children]](name/cmd 必填,cwd/env 可选);
组装点(serve)创建并启动,除此之外无人认识它。
"""

from __future__ import annotations

import asyncio
import os
import time

from . import logger


class SupervisorError(Exception):
    """子进程声明不合法。"""


def _die_with_parent() -> None:
    """子进程 preexec:登记 PDEATHSIG——父进程死亡(含 os._exit 硬退/被杀)
    即收 SIGTERM,任何退出路径都不留孤儿占端口(Linux only,项目铁律)。"""
    import ctypes
    import signal as _signal
    PR_SET_PDEATHSIG = 1
    ctypes.CDLL("libc.so.6", use_errno=True).prctl(PR_SET_PDEATHSIG, _signal.SIGTERM)


def _validate_children(children: tuple | list) -> list[dict]:
    specs: list[dict] = []
    for i, raw in enumerate(children):
        if not isinstance(raw, dict):
            raise SupervisorError(f"children[{i}] 必须是表(name/cmd/cwd/env)")
        unknown = raw.keys() - {"name", "cmd", "cwd", "env"}
        if unknown:
            raise SupervisorError(f"children[{i}] 含未知键 {sorted(unknown)}")
        name, cmd = raw.get("name"), raw.get("cmd")
        if not name or not isinstance(name, str):
            raise SupervisorError(f"children[{i}] 缺 name")
        if (not isinstance(cmd, list) or not cmd
                or not all(isinstance(c, str) for c in cmd)):
            raise SupervisorError(f"子进程 '{name}' 的 cmd 必须是非空字符串列表")
        env = raw.get("env") or {}
        if not all(isinstance(k, str) and isinstance(v, str) for k, v in env.items()):
            raise SupervisorError(f"子进程 '{name}' 的 env 键值必须都是字符串")
        specs.append({"name": name, "cmd": list(cmd),
                      "cwd": raw.get("cwd") or None, "env": dict(env)})
    return specs


class Supervisor:
    """监督器本体:每个子进程一条守护任务(拉起→接管输出→退出重启)。"""

    def __init__(self, children: tuple | list, *, grace_sec: float = 10.0) -> None:
        self._specs = _validate_children(children)
        self._grace = grace_sec
        self._tasks: list[asyncio.Task] = []
        self._procs: dict[str, asyncio.subprocess.Process] = {}
        self._stopping = False

    async def start(self) -> None:
        for spec in self._specs:
            self._tasks.append(asyncio.create_task(
                self._run_child(spec), name=f"supervisor:{spec['name']}"))
        if self._specs:
            logger.info("监督器上岗:%s", ", ".join(s["name"] for s in self._specs),
                        layer="CORE", component="Supervisor")

    def begin_shutdown(self) -> None:
        """停机第一步(同步、幂等):立刻禁重启。必须早于停机链其余 await——否则
        子进程被 SIGINT 打死(Ctrl+C 传进程组)后,守护循环会在 stop() 到达前的窗口
        里把它重新拉起,再被 stop() 的 SIGTERM→宽限→SIGKILL 收拾,凭空拖满一个宽限期。"""
        self._stopping = True

    async def stop(self) -> None:
        """停机收编:先禁重启,再逐个 SIGTERM→宽限→SIGKILL。"""
        self._stopping = True
        for name, proc in list(self._procs.items()):
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=self._grace)
                except asyncio.TimeoutError:
                    logger.warning("子进程 %s 宽限期未退,强杀", name,
                                   layer="CORE", component="Supervisor")
                    proc.kill()
                    await proc.wait()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    # ── 守护循环 ─────────────────────────────────────────────────────────────

    async def _run_child(self, spec: dict) -> None:
        name = spec["name"]
        backoff = 0.5
        while not self._stopping:
            started = time.monotonic()
            try:
                # 子进程环境:剥掉父进程的 venv 指针——子进程各有自己的项目环境
                # (publish-engine 自带 .venv),继承 VIRTUAL_ENV 会让子进程里的 uv
                # 抱怨"env 不匹配将被忽略"(实为环境污染,非日志问题)。
                child_env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}
                proc = await asyncio.create_subprocess_exec(
                    *spec["cmd"], cwd=spec["cwd"],
                    env={**child_env, **spec["env"]},
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                    # 父死子随(PDEATHSIG):主进程无论怎么退(优雅/超时硬退/被杀),
                    # 子进程都收 SIGTERM——硬退兜底不留孤儿占端口(Linux only,项目铁律)
                    preexec_fn=_die_with_parent)
            except (OSError, FileNotFoundError) as exc:
                logger.error("子进程 %s 启动失败:%s(%.1fs 后重试)", name, exc, backoff,
                             layer="CORE", component="Supervisor")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 4, 30.0)
                continue

            self._procs[name] = proc
            # 拉起后立即复查:若停机在"while 检查通过→此刻"之间发起,新进程会漏出
            # stop() 的终止快照。这里就地收掉,关掉最后的重启竞态窗口。
            if self._stopping:
                self._procs.pop(name, None)
                proc.terminate()
                await proc.wait()
                return
            logger.info("子进程启动 %s pid=%d", name, proc.pid,
                        layer="CORE", component="Supervisor")
            assert proc.stdout is not None
            async for raw in proc.stdout:            # 输出接管:黑箱变白箱
                line = raw.decode(errors="replace").rstrip()
                if line:
                    logger.info("%s", line, layer="SUBPROC", component=name)
            rc = await proc.wait()
            self._procs.pop(name, None)
            if self._stopping:
                break
            if time.monotonic() - started > 60:      # 存活够久:重置退避
                backoff = 0.5
            logger.warning("子进程 %s 退出 rc=%s,%.1fs 后重启", name, rc, backoff,
                           layer="CORE", component="Supervisor")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 4, 30.0)
