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
from typing import Any

from ..core import logger


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
            raise SupervisorError(f"children[{i}] 必须是表(name/cmd/cwd/env/lazy/dynamic_port)")
        unknown = raw.keys() - {"name", "cmd", "cwd", "env", "lazy", "dynamic_port"}
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
        lazy = raw.get("lazy", False)
        if not isinstance(lazy, bool):
            raise SupervisorError(f"子进程 '{name}' 的 lazy 必须是布尔")
        dyn = raw.get("dynamic_port", False)
        if not isinstance(dyn, bool):
            raise SupervisorError(f"子进程 '{name}' 的 dynamic_port 必须是布尔")
        if dyn and not any("{port}" in c for c in cmd) and not any(
                "{port}" in v for v in env.values()):
            raise SupervisorError(
                f"子进程 '{name}' 声明 dynamic_port 却无 {{port}} 占位(cmd/env 至少一处)")
        specs.append({"name": name, "cmd": list(cmd),
                      "cwd": raw.get("cwd") or None, "env": dict(env),
                      "lazy": lazy, "dynamic_port": dyn})
    return specs


def _free_port() -> int:
    """抓一个当前空闲的本机 TCP 端口(bind :0 由内核分配,立即释放交子进程占用)。
    close 到子进程 bind 之间有极小 TOCTOU 窗口;边车启动即 bind,实践中无碍。"""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class Supervisor:
    """监督器本体:每个子进程一条守护任务(拉起→接管输出→退出重启)。"""

    def __init__(self, children: tuple | list, *, grace_sec: float = 10.0) -> None:
        self._specs = _validate_children(children)
        self._grace = grace_sec
        self._tasks: list[asyncio.Task] = []
        self._procs: dict[str, asyncio.subprocess.Process] = {}
        self._stopping = False
        # 懒起子进程:每名一把锁(ensure/stop 互斥)+ 预期退出标记 + 退出监听
        self._lazy_locks: dict[str, asyncio.Lock] = {}
        self._expected_exit: set[str] = set()
        self._exit_listeners: dict[str, Any] = {}
        # 动态端口:构造时一次性分配(消费方组装期即可 child_port 查),重启复用同口。
        # 同批多个 dynamic_port 边车:持锁到全分配完再释放,避免抢到同一口。
        self._ports: dict[str, int] = {}
        _held = []
        try:
            import socket
            for spec in self._specs:
                if spec["dynamic_port"]:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.bind(("127.0.0.1", 0))
                    self._ports[spec["name"]] = int(s.getsockname()[1])
                    _held.append(s)
        finally:
            for s in _held:
                s.close()

    def child_port(self, name: str) -> int | None:
        """动态分配给该子进程的端口(未声明 dynamic_port 则 None)。消费域拼地址用。"""
        return self._ports.get(name)

    def _resolve(self, spec: dict) -> tuple[list[str], dict[str, str]]:
        """把 spec 的 cmd/env 里的 {port} 占位替换成分配到的端口(无 dynamic_port 则原样)。"""
        port = self._ports.get(spec["name"])
        if port is None:
            return spec["cmd"], spec["env"]
        p = str(port)
        cmd = [c.replace("{port}", p) for c in spec["cmd"]]
        env = {k: v.replace("{port}", p) for k, v in spec["env"].items()}
        return cmd, env

    async def start(self) -> None:
        for spec in self._specs:
            if spec["lazy"]:
                continue          # 懒起子进程不随启动拉起,由需求方 ensure_child
            self._tasks.append(asyncio.create_task(
                self._run_child(spec), name=f"supervisor:{spec['name']}"))
        if self._specs:
            logger.info("监督器上岗:%s", ", ".join(
                s["name"] + ("(lazy)" if s["lazy"] else "") for s in self._specs),
                layer="CORE", component="Supervisor")

    # ── 懒起子进程(按需拉起 / 需求方停止 / 意外退出回调)────────────────────────

    def set_exit_listener(self, name: str, cb: Any) -> None:
        """登记懒起子进程的意外退出回调 cb(rc)(组装点接线;stop_child 发起的
        预期退出不触发)。cb 可为同步函数或协程函数。"""
        self._exit_listeners[name] = cb

    def _lazy_spec(self, name: str) -> dict:
        spec = next((s for s in self._specs if s["name"] == name), None)
        if spec is None or not spec["lazy"]:
            raise SupervisorError(f"'{name}' 不是已声明的懒起子进程")
        return spec

    def child_alive(self, name: str) -> bool:
        proc = self._procs.get(name)
        return proc is not None and proc.returncode is None

    async def ensure_child(self, name: str) -> None:
        """按需拉起(幂等):已在跑即返回;停机中拒绝。竞态由每名锁串行化。"""
        spec = self._lazy_spec(name)
        async with self._lazy_locks.setdefault(name, asyncio.Lock()):
            if self._stopping:
                raise SupervisorError(f"停机中,拒绝拉起 '{name}'")
            if self.child_alive(name):
                return
            self._expected_exit.discard(name)
            cmd, spec_env = self._resolve(spec)     # {port} → 分配口
            child_env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}
            proc = await asyncio.create_subprocess_exec(
                *cmd, cwd=spec["cwd"], env={**child_env, **spec_env},
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                preexec_fn=_die_with_parent)
            self._procs[name] = proc
            self._tasks.append(asyncio.create_task(
                self._watch_lazy_child(name, proc), name=f"supervisor:{name}"))
            logger.info("懒起子进程启动 %s pid=%d%s", name, proc.pid,
                        f" port={self._ports[name]}" if name in self._ports else "",
                        layer="CORE", component="Supervisor")

    async def stop_child(self, name: str) -> None:
        """需求方发起的优雅停止(幂等):SIGTERM→宽限→SIGKILL;不触发退出监听。"""
        self._lazy_spec(name)
        async with self._lazy_locks.setdefault(name, asyncio.Lock()):
            proc = self._procs.get(name)
            if proc is None or proc.returncode is not None:
                return
            self._expected_exit.add(name)
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=self._grace)
            except asyncio.TimeoutError:
                logger.warning("懒起子进程 %s 宽限期未退,强杀", name,
                               layer="CORE", component="Supervisor")
                proc.kill()
                await proc.wait()
            logger.info("懒起子进程已停 %s", name, layer="CORE", component="Supervisor")

    async def _watch_lazy_child(self, name: str, proc) -> None:
        """守护一次懒起进程的生命期:接管输出 → 等退出 → 意外退出通知监听。
        不重启——生死归需求方。"""
        assert proc.stdout is not None
        async for raw in proc.stdout:
            line = raw.decode(errors="replace").rstrip()
            if line:
                logger.info("%s", line, layer="SUBPROC", component=name)
        rc = await proc.wait()
        self._procs.pop(name, None)
        if self._stopping or name in self._expected_exit:
            self._expected_exit.discard(name)
            return
        logger.warning("懒起子进程 %s 意外退出 rc=%s(不自动重启,通知需求方)",
                       name, rc, layer="CORE", component="Supervisor")
        cb = self._exit_listeners.get(name)
        if cb is not None:
            try:
                r = cb(rc)
                if asyncio.iscoroutine(r):
                    await r
            except Exception as exc:  # noqa: BLE001 — 监听故障不反噬监督器
                logger.error("退出监听回调异常 %s err=%s", name, exc,
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
                cmd, spec_env = self._resolve(spec)     # {port} → 分配口
                child_env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}
                proc = await asyncio.create_subprocess_exec(
                    *cmd, cwd=spec["cwd"],
                    env={**child_env, **spec_env},
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
            logger.info("子进程启动 %s pid=%d%s", name, proc.pid,
                        f" port={self._ports[name]}" if name in self._ports else "",
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
