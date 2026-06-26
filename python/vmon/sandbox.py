"""Modal-style sandbox API backed by vmon templates and the guest agent."""

from __future__ import annotations

import asyncio
import json
import os
import secrets as _secrets
import shutil
import threading
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .agent import AgentClosed, AgentConn, ByteStream, ExecSession
from .image import ImageSpec, cached_template
from .pool import WarmPool
from .secret import Secret, merge_secrets
from .vmm import STATE, MicroVM, _instance_name, _validate_int_range, _validate_timeout_secs
from .volume import Volume

_POOLS: dict[str, WarmPool] = {}


def _setup_sandbox_network(
    name: str,
    *,
    ports=None,
    egress_allow=None,
    egress_allow_domains=None,
    inbound_cidr_allowlist=None,
):
    """Create the host TAP + port tunnels for a networked sandbox before launch.

    Returns a :class:`vmon.net.SandboxNetwork`; raises ``PermissionError`` without
    root/CAP_NET_ADMIN. The VM is then booted with ``--tap`` bound to this TAP and
    the guest side is configured (``net_config``) once the agent answers.
    """
    from . import net

    return net.setup_sandbox_network(
        name,
        ports=list(ports) if ports is not None else None,
        egress_allow=list(egress_allow) if egress_allow is not None else None,
        egress_allow_domains=list(egress_allow_domains)
        if egress_allow_domains is not None
        else None,
        inbound_cidr_allowlist=list(inbound_cidr_allowlist)
        if inbound_cidr_allowlist is not None
        else None,
    )


class _ProcessStdin:
    def __init__(self, session: ExecSession) -> None:
        self._session = session
        self._closed = False

    def write(self, data: bytes | bytearray | memoryview | str) -> None:
        if self._closed:
            raise ValueError("stdin is closed")
        self._session.write_stdin(data)

    def close(self) -> None:
        if not self._closed:
            self._session.close_stdin()
            self._closed = True


class Process:
    """A process running inside a Sandbox."""

    def __init__(self, session: ExecSession) -> None:
        self._session = session
        self.stdout: ByteStream = session.stdout
        self.stderr: ByteStream = session.stderr
        self.stdin = _ProcessStdin(session)

    @property
    def returncode(self) -> int | None:
        return self._session.returncode

    def write_stdin(self, data: bytes | bytearray | memoryview | str) -> None:
        self.stdin.write(data)

    def close_stdin(self) -> None:
        self.stdin.close()

    def kill(self, signal: int = 15) -> None:
        self._session.kill(signal)

    def resize(self, rows: int, cols: int) -> None:
        self._session.resize(int(rows), int(cols))

    def wait(self, timeout: float | None = None) -> int:
        return self._session.wait(timeout)


class Filesystem:
    """Filesystem RPC facade for a Sandbox."""

    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox

    def read_bytes(self, path: str) -> bytes:
        return self._sandbox.agent().fs_read(path)

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(path).decode(encoding)

    def write_bytes(
        self, path: str, data: bytes | bytearray | memoryview, mode: int = 0o644
    ) -> None:
        self._sandbox.agent().fs_write(path, data, mode=mode)

    def write_text(self, path: str, text: str, encoding: str = "utf-8", mode: int = 0o644) -> None:
        self.write_bytes(path, text.encode(encoding), mode=mode)

    def list_files(self, path: str = ".") -> list[dict[str, Any]]:
        return self._sandbox.agent().fs_list(path)

    def make_directory(self, path: str, parents: bool = True) -> None:
        self._sandbox.agent().fs_mkdir(path, parents=parents)

    def remove(self, path: str, recursive: bool = False) -> None:
        self._sandbox.agent().fs_remove(path, recursive=recursive)

    def stat(self, path: str) -> dict[str, Any]:
        return self._sandbox.agent().fs_stat(path)


class Sandbox:
    """A warm-restored microVM with Modal-compatible exec and filesystem methods."""

    def __init__(
        self,
        vm: MicroVM,
        image_spec: ImageSpec | None = None,
        workdir: str | None = None,
        env: dict[str, str] | None = None,
        network: Any = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        self.vm = vm
        self.name = vm.name
        self.image_spec = image_spec
        self.workdir = workdir or (image_spec.workdir if image_spec else None) or "/"
        self.env = dict(env or {})
        self.tags: dict[str, str] = dict(tags or {})
        self.filesystem = Filesystem(self)
        self._agent: AgentConn | None = None
        self._network = network
        self._network_policy: dict[str, Any] = {}
        self._terminated = False
        self._secret_env: dict[str, str] = {}
        self._volumes: list[Volume] = []
        self.connect_token: str | None = None
        self._timeout_secs: int | None = None
        self._watchdog_stop: threading.Event | None = None
        self._watchdog_thread: threading.Thread | None = None
        self._entry_returncode: int | None = None
        self._entry_process: Process | None = None

    @classmethod
    def create(
        cls,
        image: str | None = None,
        *,
        template: str | os.PathLike[str] | None = None,
        dockerfile: str | None = None,
        context: str = ".",
        name: str | None = None,
        cpus: int = 1,
        memory: int = 512,
        disk_mb: int = 1024,
        timeout: float = 300,
        timeout_secs: int | None = None,
        workdir: str | None = None,
        env: dict[str, str] | None = None,
        secrets: Iterable[Secret | dict[str, str]] | None = None,
        volumes: dict[str, Volume | tuple[Volume, bool]] | None = None,
        tags: dict[str, str] | None = None,
        fs_dir: str | None = None,
        block_network: bool = False,
        ports: list[int] | tuple[int, ...] | None = None,
        egress_allow: list[str] | tuple[str, ...] | None = None,
        egress_allow_domains: list[str] | tuple[str, ...] | None = None,
        inbound_cidr_allowlist: list[str] | tuple[str, ...] | None = None,
        readiness_probe: Any = None,
        pool_size: int = 0,
    ) -> Sandbox:
        if block_network and ports:
            raise ValueError("ports cannot be exposed when block_network=True")
        cpus = _validate_int_range(cpus, "cpus", maximum=64)
        memory = _validate_int_range(memory, "memory", maximum=64 * 1024, unit=" MiB")
        disk_mb = _validate_int_range(disk_mb, "disk_mb")
        if timeout_secs == 0:
            eff_timeout_secs = None
        else:
            eff_timeout_secs = (
                _validate_timeout_secs(timeout_secs) if timeout_secs is not None else 300
            )
        template_dir, image_spec, image_ref = cls._resolve_template(
            image=image,
            template=template,
            dockerfile=dockerfile,
            context=context,
            disk_mb=disk_mb,
            timeout=timeout,
            memory=memory,
            cpus=cpus,
        )
        # Modal-parity default: a 5-minute VMM-enforced wall-clock deadline unless
        # the caller sets timeout_secs (0 disables).
        acquired: list[Volume] = []
        volume_specs: list[dict[str, Any]] = []
        vm: MicroVM | None = None
        from_pool = False
        sb: Sandbox | None = None
        net_handle: Any = None
        try:
            used_tags: set[str] = set()
            for mountpoint, value in (volumes or {}).items():
                vol, read_only = cls._coerce_volume(value)
                vol.acquire()
                acquired.append(vol)
                host_dir = vol.ensure()
                tag = cls._unique_volume_tag(vol.tag, used_tags)
                volume_specs.append(
                    {
                        "mountpoint": str(mountpoint),
                        "volume": vol,
                        "tag": tag,
                        "host_dir": str(host_dir),
                        "read_only": read_only,
                    }
                )

            vol_tuples = [
                (spec["tag"], spec["host_dir"], bool(spec["read_only"])) for spec in volume_specs
            ]
            pool: WarmPool | None = None
            # The warm pool only serves auto-named sandboxes: a pooled clone keeps
            # its pre-assigned name, so a caller-requested `name` (and SDK-level
            # from_id / named uniqueness) must take the cold-restore path instead.
            if (
                pool_size > 0
                and block_network
                and not volume_specs
                and name is None
                and fs_dir is None
            ):
                key = str(template_dir)
                pool = _POOLS.get(key)
                if pool is None or pool.size != int(pool_size):
                    pool = WarmPool(template_dir, int(pool_size))
                    _POOLS[key] = pool
                vm = pool.claim()
                from_pool = vm is not None
            if vm is None and block_network:
                vm = MicroVM.restore(
                    template_dir,
                    name=name,
                    agent=True,
                    fs_dir=fs_dir,
                    mem=memory,
                    cpus=cpus,
                    volumes=vol_tuples,
                    timeout_secs=eff_timeout_secs,
                )
            elif vm is None:
                # vmon restore cannot synthesize a NIC the template snapshot
                # lacks, so a networked sandbox fresh-boots from the template disk
                # (as a CoW overlay) with a virtio-net device on a host TAP.
                name = name or _instance_name(Path(str(template_dir)).name or "sandbox")
                base_disk = Path(template_dir) / "rootfs.img"
                if not base_disk.is_file():
                    raise RuntimeError(
                        f"template {template_dir} has no rootfs.img; networked "
                        "sandboxes require a disk-backed template"
                    )
                net_handle = _setup_sandbox_network(
                    name,
                    ports=ports,
                    egress_allow=egress_allow,
                    egress_allow_domains=egress_allow_domains,
                    inbound_cidr_allowlist=inbound_cidr_allowlist,
                )
                vm = MicroVM.boot_rootfs(
                    base_disk,
                    name=name,
                    mem=memory,
                    cpus=cpus,
                    overlay=True,
                    agent=True,
                    tap=str(net_handle.guest_config["tap"]),
                    fs_dir=fs_dir,
                    volumes=vol_tuples,
                    timeout_secs=eff_timeout_secs,
                    snapshot_root=STATE / "snapshots",
                )

            sb = cls(vm, image_spec=image_spec, workdir=workdir, env=env, tags=tags)
            sb._network = net_handle
            sb._secret_env = merge_secrets(secrets)
            sb._volumes = list(acquired)
            sb._timeout_secs = eff_timeout_secs
            if sb._timeout_secs is not None:
                sb._start_timeout_watchdog(sb._timeout_secs)
            sb.agent(connect_timeout=timeout).ping(timeout=timeout)
            if from_pool and sb._timeout_secs is not None:
                # Pool clones launch without --timeout-secs; arm the VMM-enforced
                # deadline via the extend control op (the host watchdog is a backstop).
                try:
                    sb.extend(sb._timeout_secs)
                except Exception:
                    pass
            for spec in volume_specs:
                sb.agent().mount(spec["tag"], spec["mountpoint"], ro=bool(spec["read_only"]))
            if net_handle is not None:
                gc = net_handle.guest_config
                sb.agent().net_config(gc["ip"], int(gc["prefix"]), gc["gw"], gc.get("dns") or [])
                sb._network_policy = {
                    "egress_allow": list(egress_allow) if egress_allow is not None else None,
                    "egress_allow_domains": list(egress_allow_domains)
                    if egress_allow_domains is not None
                    else None,
                    "inbound_cidr_allowlist": list(inbound_cidr_allowlist)
                    if inbound_cidr_allowlist is not None
                    else None,
                }
            volumes_meta = {
                spec["mountpoint"]: {
                    "name": spec["volume"].name,
                    "tag": spec["tag"],
                    "read_only": bool(spec["read_only"]),
                }
                for spec in volume_specs
            }
            vm._save_meta(
                sandbox=True,
                image=image_ref,
                template=str(template_dir),
                workdir=sb.workdir,
                env_names=sorted(sb.env),
                secret_names=sorted(sb._secret_env),
                tags=sb.tags,
                volumes=volumes_meta,
                block_network=block_network,
                timeout_secs=sb._timeout_secs,
            )
            if readiness_probe is not None:
                sb.wait_until_ready(readiness_probe, timeout=timeout)
            return sb
        except BaseException:
            if sb is not None:
                sb.terminate(wait=True)
            else:
                if net_handle is not None:
                    try:
                        net_handle.teardown()
                    except Exception:
                        pass
                if vm is not None:
                    try:
                        vm.stop()
                    finally:
                        for vol in acquired:
                            vol.release()
                else:
                    for vol in acquired:
                        vol.release()
            raise

    @classmethod
    def from_snapshot(
        cls,
        name: str | os.PathLike[str],
        *,
        fork: bool = False,
        sandbox_name: str | None = None,
        timeout: float = 60,
        workdir: str | None = None,
        env: dict[str, str] | None = None,
        block_network: bool = False,
        ports: list[int] | tuple[int, ...] | None = None,
        egress_allow: list[str] | tuple[str, ...] | None = None,
    ) -> Sandbox:
        if block_network and ports:
            raise ValueError("ports cannot be exposed when block_network=True")
        net_handle = None
        sb: Sandbox | None = None
        try:
            if block_network:
                vm = (
                    MicroVM.fork_snapshot(name, name=sandbox_name, agent=True)
                    if fork
                    else MicroVM.restore(name, name=sandbox_name, agent=True)
                )
                sb = cls(vm, workdir=workdir, env=env)
                sb.agent(connect_timeout=timeout).ping(timeout=timeout)
            else:
                if fork:
                    raise ValueError(
                        "networked from_snapshot fork is unsupported; use "
                        "block_network=True or Sandbox.create()"
                    )
                snap_dir = MicroVM._snapshot_dir(name)
                base_disk = snap_dir / "rootfs.img"
                if not base_disk.is_file():
                    raise RuntimeError(f"snapshot {name} has no rootfs.img for a networked boot")
                sandbox_name = sandbox_name or _instance_name(Path(str(name)).name or "snapshot")
                net_handle = _setup_sandbox_network(
                    sandbox_name, ports=ports, egress_allow=egress_allow
                )
                vm = MicroVM.boot_rootfs(
                    base_disk,
                    name=sandbox_name,
                    overlay=True,
                    agent=True,
                    tap=str(net_handle.guest_config["tap"]),
                    snapshot_root=STATE / "snapshots",
                )
                sb = cls(vm, workdir=workdir, env=env)
                sb._network = net_handle
                sb.agent(connect_timeout=timeout).ping(timeout=timeout)
                gc = net_handle.guest_config
                sb.agent().net_config(gc["ip"], int(gc["prefix"]), gc["gw"], gc.get("dns") or [])
            vm._save_meta(
                sandbox=True,
                restored_snapshot=str(name),
                workdir=sb.workdir,
                env_names=sorted(sb.env),
                tags=sb.tags,
                secret_names=[],
                volumes={},
                block_network=block_network,
            )
            return sb
        except BaseException:
            if sb is not None:
                sb.terminate(wait=True)
            elif net_handle is not None:
                try:
                    net_handle.teardown()
                except Exception:
                    pass
            raise

    @classmethod
    def from_id(cls, id: str) -> Sandbox:
        return cls.attach(id)

    @classmethod
    def attach(cls, name: str) -> Sandbox:
        vm = MicroVM(name)
        meta = vm.meta
        return cls(
            vm, workdir=meta.get("workdir"), env=meta.get("env") or {}, tags=meta.get("tags") or {}
        )

    @staticmethod
    def _resolve_template(
        *,
        image: str | None,
        template: str | os.PathLike[str] | None,
        dockerfile: str | None,
        context: str,
        disk_mb: int,
        timeout: float,
        memory: int,
        cpus: int,
    ) -> tuple[Path, ImageSpec | None, str | None]:
        if template is not None:
            path = Path(template)
            template_dir = (
                path if path.exists() or path.is_absolute() else STATE / "templates" / str(template)
            )
            return template_dir, None, str(template)
        cached = cached_template(
            image=image,
            dockerfile=dockerfile,
            context=context,
            disk_mb=disk_mb,
            timeout=timeout,
            memory=memory,
            cpus=cpus,
        )
        return cached.snapshot_dir, cached.spec, cached.spec.reference

    @staticmethod
    def _coerce_volume(value: Volume | tuple[Volume, bool]) -> tuple[Volume, bool]:
        if isinstance(value, tuple):
            vol, read_only = value
            if not isinstance(vol, Volume):
                raise TypeError("volume tuple must contain a Volume")
            return vol, bool(read_only)
        if not isinstance(value, Volume):
            raise TypeError("volumes values must be Volume instances")
        return value, False

    @staticmethod
    def _unique_volume_tag(base: str, used: set[str]) -> str:
        stem = "".join(
            ch if ("a" <= ch <= "z" or "0" <= ch <= "9" or ch == "_") else "_"
            for ch in base.lower()
        )
        stem = (stem or "vol")[:32]
        candidate = stem
        suffix = 2
        while candidate in used:
            tail = f"_{suffix}"
            candidate = f"{stem[: 32 - len(tail)]}{tail}"
            suffix += 1
        used.add(candidate)
        return candidate

    def _start_timeout_watchdog(self, secs: int) -> None:
        stop = threading.Event()
        self._watchdog_stop = stop

        def watch() -> None:
            if not stop.wait(max(1, int(secs))):
                self.terminate(wait=False)

        thread = threading.Thread(target=watch, name=f"vmon-timeout-{self.name}", daemon=True)
        self._watchdog_thread = thread
        thread.start()

    def _stop_timeout_watchdog(self) -> None:
        stop = self._watchdog_stop
        thread = self._watchdog_thread
        self._watchdog_stop = None
        self._watchdog_thread = None
        if stop is not None:
            stop.set()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=0.1)

    def agent(self, connect_timeout: float | None = None) -> AgentConn:
        if self._terminated:
            raise AgentClosed("sandbox has been terminated")
        if self._agent is None or self._agent.closed:
            self._agent = self.vm.agent(connect_timeout=connect_timeout)
        return self._agent

    def exec(
        self,
        *cmd: str | Iterable[str],
        workdir: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
        pty: bool = False,
        tty: bool = False,
        _track_entry: bool = True,
    ) -> Process:
        if len(cmd) == 1 and not isinstance(cmd[0], str):
            argv = [str(c) for c in cmd[0]]
        else:
            argv = [str(c) for c in cmd]
        if not argv:
            raise ValueError("exec command must not be empty")
        merged_env = {**self.env, **self._secret_env}
        if env:
            merged_env.update({str(k): str(v) for k, v in env.items()})
        session = self.agent().exec(
            argv, cwd=workdir or self.workdir, env=merged_env, timeout=timeout, tty=bool(pty or tty)
        )
        proc = Process(session)
        if _track_entry and self._entry_process is None:
            self._entry_process = (
                proc  # first user exec is the sandbox "entry" for poll()/returncode
            )
        return proc

    def wait_until_ready(self, probe: Any = None, timeout: float = 300) -> None:
        deadline = time.time() + timeout
        last: BaseException | None = None
        if probe is None:
            self.agent(connect_timeout=timeout).ping(timeout=timeout)
            return
        while time.time() < deadline:
            remaining = max(0.0, deadline - time.time())
            try:
                if isinstance(probe, int):
                    if self.agent().tcp_probe(int(probe)):
                        return
                elif isinstance(probe, dict) and "port" in probe:
                    host = str(probe.get("host") or "127.0.0.1")
                    if self.agent().tcp_probe(
                        int(probe["port"]), host=host, timeout=min(1.0, remaining)
                    ):
                        return
                else:
                    argv = (
                        ["sh", "-lc", probe] if isinstance(probe, str) else [str(p) for p in probe]
                    )
                    proc = self.exec(argv, timeout=min(5.0, remaining), _track_entry=False)
                    try:
                        if proc.wait(timeout=min(5.0, remaining)) == 0:
                            return
                    except TimeoutError:
                        proc.kill()
                time.sleep(0.1)
            except BaseException as exc:
                last = exc
                time.sleep(0.1)
        raise TimeoutError(f"sandbox readiness probe timed out after {timeout}s") from last

    def _status(self) -> dict[str, Any] | None:
        paths = [Path(self.vm.control_sock).parent / "status.json", self.vm.dir / "status.json"]
        seen: set[Path] = set()
        for path in paths:
            if path in seen:
                continue
            seen.add(path)
            try:
                data = json.loads(path.read_text())
            except (FileNotFoundError, json.JSONDecodeError, OSError):
                continue
            return data if isinstance(data, dict) else None
        return None

    def poll(self) -> int | None:
        if self._entry_returncode is None and self._entry_process is not None:
            rc = self._entry_process.returncode
            if rc is not None:
                self._entry_returncode = rc
        if self._entry_returncode is not None:
            return self._entry_returncode
        status = self._status()
        if status is None:
            return None
        code = status.get("vmm_returncode")
        if isinstance(code, int):
            return code
        reason = status.get("reason")
        if isinstance(reason, str):
            return {"timeout": 124, "quit": 137, "killed": 137, "shutdown": 0}.get(reason)
        return None

    @property
    def returncode(self) -> int | None:
        return self.poll()

    def snapshot(self, name: str | None = None) -> str:
        if self._terminated:
            raise RuntimeError("sandbox has been terminated")
        snap_name = name or f"{self.name}-{int(time.time() * 1000)}"
        self.vm.snapshot(snap_name, keep_running=True, disk_src=self.vm.rootfs_img)
        return snap_name

    def snapshot_filesystem(self, name: str | None = None, ttl: int | None = 30 * 24 * 3600) -> str:
        if self._terminated:
            raise RuntimeError("sandbox has been terminated")
        snap_name = name or f"{self.name}-img-{int(time.time())}"
        # vmon writes snapshot state under the VM's launch --snapshot-root, so
        # capture there (default root) and then relocate the complete snapshot
        # (state + rootfs.img) into the template store, where
        # Sandbox.create(template=...) resolves it by name.
        src_dir = self.vm.snapshot(snap_name, keep_running=True, disk_src=self.vm.rootfs_img)
        dst_dir = STATE / "templates" / snap_name
        dst_dir.parent.mkdir(parents=True, exist_ok=True)
        if src_dir.resolve() != dst_dir.resolve():
            if dst_dir.exists():
                shutil.rmtree(dst_dir)
            os.replace(src_dir, dst_dir)
        meta = {"created_unix": int(time.time()), "ttl": ttl, "image": snap_name}
        (dst_dir / "image.json").write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
        return snap_name

    def terminate(self, wait: bool = True) -> None:
        if self._terminated:
            return
        self._terminated = True
        self._stop_timeout_watchdog()
        try:
            if self._agent is not None:
                self._agent.close()
            self._teardown_network()
            self.vm.stop()
        finally:
            for vol in self._volumes:
                vol.release()
            self._volumes.clear()

    stop = terminate

    def tunnels(self) -> dict[int, tuple[str, int]]:
        if self._network is not None and hasattr(self._network, "tunnels"):
            return dict(self._network.tunnels())
        return {}

    def create_connect_token(self) -> str:
        if self.connect_token is None:
            self.connect_token = _secrets.token_urlsafe(32)
        return self.connect_token

    def extend(self, secs: int) -> dict[str, Any]:
        control = self.vm.control
        extend = getattr(control, "extend", None)
        if callable(extend):
            result = extend(int(secs))
        else:
            request = getattr(control, "_request", None)
            if not callable(request):
                raise RuntimeError("control client does not support extend")
            result = request("extend", {"secs": int(secs)})
        # Realign the host watchdog backstop with the new VMM deadline so it does
        # not terminate the sandbox at the original (now-stale) deadline.
        self._timeout_secs = int(secs)
        self._stop_timeout_watchdog()
        self._start_timeout_watchdog(int(secs))
        return result if isinstance(result, dict) else {}

    def set_network_policy(
        self,
        block_network: bool | None = None,
        cidr_allow: list[str] | tuple[str, ...] | None = None,
        domain_allow: list[str] | tuple[str, ...] | None = None,
    ) -> None:
        if self._network is None:
            return
        try:
            from . import net
        except ImportError:
            return
        cfg = getattr(self._network, "guest_config", None) or getattr(self._network, "config", None)
        if not hasattr(net, "setup_tap"):
            return
        if isinstance(cfg, dict):
            tap = str(cfg.get("tap") or getattr(getattr(self._network, "tap", None), "name", ""))
            guest_ip = str(cfg.get("guest_ip") or cfg.get("ip") or "")
            host_ip = str(cfg.get("host_ip") or cfg.get("gw") or "")
            prefix = int(cfg.get("prefix", 30))
        else:
            tap = str(getattr(self._network, "name", ""))
            guest_ip = str(getattr(self._network, "guest_ip", ""))
            host_ip = str(getattr(self._network, "host_ip", ""))
            prefix = int(getattr(self._network, "prefix", 30))
        if block_network is True:
            allow_list: list[str] | None = []
        elif cidr_allow is not None:
            allow_list = list(cidr_allow)
        else:
            allow_list = self._network_policy.get("egress_allow")
        domain_list = (
            list(domain_allow)
            if domain_allow is not None
            else self._network_policy.get("egress_allow_domains")
        )
        net.setup_tap(
            tap,
            guest_ip,
            host_ip,
            prefix,
            egress_allow=allow_list,
            egress_allow_domains=domain_list,
        )
        self._network_policy.update(
            {
                "block_network": bool(block_network) if block_network is not None else False,
                "egress_allow": allow_list,
                "egress_allow_domains": domain_list,
            }
        )

    @property
    def aio(self) -> _AsyncSandbox:
        return _AsyncSandbox(self)

    def __enter__(self) -> Sandbox:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.terminate()

    def _teardown_network(self) -> None:
        network = self._network
        self._network = None
        if network is None:
            return
        if hasattr(network, "teardown"):
            network.teardown()
        elif callable(network):
            network()


class _AsyncSandbox:
    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox

    async def exec(self, *cmd: str | Iterable[str], **kwargs: Any) -> Process:
        return await asyncio.to_thread(self._sandbox.exec, *cmd, **kwargs)

    async def terminate(self, *args: Any, **kwargs: Any) -> None:
        return await asyncio.to_thread(self._sandbox.terminate, *args, **kwargs)

    async def wait_until_ready(self, *args: Any, **kwargs: Any) -> None:
        return await asyncio.to_thread(self._sandbox.wait_until_ready, *args, **kwargs)

    async def snapshot_filesystem(self, *args: Any, **kwargs: Any) -> str:
        return await asyncio.to_thread(self._sandbox.snapshot_filesystem, *args, **kwargs)

    async def poll(self) -> int | None:
        return await asyncio.to_thread(self._sandbox.poll)

    async def set_network_policy(self, *args: Any, **kwargs: Any) -> None:
        return await asyncio.to_thread(self._sandbox.set_network_policy, *args, **kwargs)


def default_command(spec: ImageSpec | None, override: list[str] | None = None) -> list[str]:
    if spec is None:
        return list(override or ["/bin/sh"])
    argv = spec.argv(override)
    return argv or ["/bin/sh"]
