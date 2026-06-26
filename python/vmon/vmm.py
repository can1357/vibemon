"""The MicroVM SDK: boot containers, suspend/resume, snapshot/restore, fork."""

from __future__ import annotations

import json
import os
import platform
import re
import shutil
import signal
import subprocess
import threading
import time
from collections.abc import Iterable
from pathlib import Path
from typing import cast

from .agent_client import AgentClosed, AgentConn
from .control import Control

STATE = Path(os.environ.get("VMON_HOME", str(Path.home() / ".vmon")))
_MAX_CPUS = 64
_MAX_MEM_MIB = 64 * 1024
_MAX_TIMEOUT_SECS = 86_400


def _validate_int_range(
    value, flag: str, *, minimum: int = 1, maximum: int | None = None, unit: str = ""
) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{flag} must be an integer")
    try:
        ivalue = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{flag} must be an integer") from exc
    if isinstance(value, float) and ivalue != value:
        raise ValueError(f"{flag} must be an integer")
    if ivalue < minimum or (maximum is not None and ivalue > maximum):
        upper = f" and {maximum}{unit}" if maximum is not None else ""
        raise ValueError(f"{flag} must be between {minimum}{unit}{upper} (got {ivalue})")
    return ivalue


def _validate_timeout_secs(value: int | None) -> int | None:
    if value is None:
        return None
    return _validate_int_range(value, "timeout_secs", maximum=_MAX_TIMEOUT_SECS)


def _arch() -> str:
    m = platform.machine()
    return "aarch64" if m in ("aarch64", "arm64") else "x86_64"


def _cargo_target_dir(repo: Path) -> Path | None:
    """The cargo target directory for *repo* (``$CARGO_TARGET_DIR`` or ``cargo metadata``).

    Honors a custom ``build.target-dir`` set in cargo config, which a bare
    ``$CARGO_TARGET_DIR`` env lookup misses.
    """
    if env := os.environ.get("CARGO_TARGET_DIR"):
        return Path(env)
    try:
        out = subprocess.run(
            ["cargo", "metadata", "--no-deps", "--format-version", "1"],
            cwd=repo,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        return Path(json.loads(out)["target_directory"])
    except OSError, subprocess.CalledProcessError, json.JSONDecodeError, KeyError:
        return None


def find_binary() -> str:
    """Locate the vmm VMM binary (env override, cargo target dirs, or PATH).

    Searches ``$VMON_BIN``, then the repo's cargo target directory across native
    and cross ``release``/``debug`` layouts, then ``PATH``. On macOS a plain
    ``cargo build`` of the repo produces a binary ad-hoc codesigned with
    ``com.apple.security.hypervisor``, which this resolver finds without any flags.
    """
    if env := os.environ.get("VMON_BIN"):
        return env
    arch = _arch()
    repo = Path(__file__).resolve().parents[2]
    triples = [
        "",
        f"{arch}-unknown-linux-gnu",
        f"{arch}-unknown-linux-musl",
        f"{arch}-apple-darwin",
    ]

    def probe(root: Path) -> str | None:
        for triple in triples:
            for profile in ("release", "debug"):
                c = root / triple / profile / "vmm"
                if c.is_file() and os.access(c, os.X_OK):
                    return str(c)
        return None

    if found := probe(repo / "target"):
        return found
    if (target_dir := _cargo_target_dir(repo)) is not None and (found := probe(target_dir)):
        return found
    if found := shutil.which("vmm"):
        return found
    raise RuntimeError("vmm binary not found. Build it (cargo build) or set VMON_BIN.")


def default_kernel() -> str:
    """Resolve a guest kernel for the host arch.

    Order: ``$VMON_KERNEL`` override, a matching host kernel under ``/boot``
    (Linux), then a pinned kernel auto-downloaded into ``$VMON_HOME/assets`` on
    first use (so macOS/HVF hosts need no manual setup).
    """
    if env := os.environ.get("VMON_KERNEL"):
        return env
    k = Path(f"/boot/vmlinuz-{platform.release()}")
    if k.is_file():
        return str(k)
    from .assets import ensure_kernel

    return str(ensure_kernel())


def _cmdline() -> str:
    base = "console=ttyS0 reboot=t panic=-1 rdinit=/init"
    if _arch() == "aarch64":
        base += " earlycon=uart8250,mmio,0x9000000"
    return base


def _agent_cmdline() -> str:
    base = "console=ttyS0 reboot=t panic=-1 root=/dev/vda init=/.vmon/agent vmon.agent=serve"
    if _arch() == "aarch64":
        base += " earlycon=uart8250,mmio,0x9000000"
    return base


def _valid_snapshot_name(name: str) -> str:
    if not name or name in {".", ".."} or name.startswith("."):
        raise ValueError("snapshot name must be a non-hidden relative name")
    if "\x00" in name or "/" in name or "\\" in name or ".." in name:
        raise ValueError("snapshot name must not contain path separators or '..'")
    return name


def _snapshot_state_present(snap_dir: Path) -> bool:
    """True if *snap_dir* holds restorable snapshot state.

    Mirrors vmon's ``selected_layout``: a snapshot is a manifest-published
    generation (``current-generation`` + ``vmstate.<gen>.bin``). The writer only
    emits generation files, so the manifest's presence is the gate.
    """
    return (snap_dir / "current-generation").is_file()


def copy_reflink(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> Path:
    """Copy ``src`` to ``dst`` using CoW reflinks when the host supports them."""
    src_p = Path(src)
    dst_p = Path(dst)
    dst_p.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst_p.with_name(dst_p.name + f".tmp-{os.getpid()}")
    tmp.unlink(missing_ok=True)
    try:
        subprocess.run(
            ["cp", "--reflink=auto", str(src_p), str(tmp)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError, subprocess.CalledProcessError:
        shutil.copy2(src_p, tmp)
    os.replace(tmp, dst_p)
    return dst_p


def _volume_args(volumes: Iterable[tuple[str, str | os.PathLike[str], bool]] | None) -> list[str]:
    """Format ``(tag, host_dir, read_only)`` tuples as repeatable ``--volume`` CLI args."""
    args: list[str] = []
    for tag, host_dir, read_only in volumes or []:
        tag_s = str(tag)
        if not re.fullmatch(r"[a-z0-9_]{1,32}", tag_s):
            raise ValueError(f"volume tag {tag_s!r} must match [a-z0-9_]{{1,32}}")
        host_s = os.fspath(host_dir)
        if not host_s:
            raise ValueError("volume host directory must not be empty")
        if "\0" in host_s:
            raise ValueError("volume host directory must not contain NUL bytes")
        if not read_only and host_s.endswith(":ro"):
            raise ValueError("read-write volume host directory must not end with ':ro'")
        args.extend(["--volume", f"{tag_s}:{host_s}" + (":ro" if read_only else "")])
    return args


class MicroVM:
    """A microVM instance, addressed by a stable ``name``."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.dir = STATE / "vms" / name
        self.sock = self.dir / "api.sock"
        self.log = self.dir / "console.log"
        self.meta_path = self.dir / "meta.json"
        self._agent_conn: AgentConn | None = None
        self._agent_lock = threading.Lock()

    @property
    def meta(self) -> dict[str, object]:
        try:
            return json.loads(self.meta_path.read_text())
        except FileNotFoundError:
            return {}

    def _save_meta(self, **kw: object) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        m = self.meta
        m.update(kw)
        self.meta_path.write_text(json.dumps(m, indent=2, sort_keys=True))

    @staticmethod
    def _meta_optional_path(value: object) -> Path | None:
        if not value:
            return None
        if isinstance(value, str):
            return Path(value)
        if isinstance(value, os.PathLike):
            return Path(cast(os.PathLike[str], value))
        raise TypeError("metadata path must be str or os.PathLike")

    @classmethod
    def _meta_path(cls, value: object, fallback: str | os.PathLike[str]) -> Path:
        return cls._meta_optional_path(value) or Path(fallback)

    @staticmethod
    def _meta_pid(value: object) -> int | None:
        if not value:
            return None
        if isinstance(value, int):
            return value
        raise TypeError("metadata pid must be int")

    @property
    def control_sock(self) -> Path:
        m = self.meta
        if (jail_root := self._meta_optional_path(m.get("jail_root"))) is not None:
            return jail_root / "root" / "run" / "vmon" / "control.sock"
        return self._meta_path(m.get("sock"), self.sock)

    @property
    def agent_sock(self) -> Path:
        m = self.meta
        if (jail_root := self._meta_optional_path(m.get("jail_root"))) is not None:
            return jail_root / "root" / "run" / "vmon" / "agent.sock"
        return self._meta_path(m.get("agent_sock"), self.dir / "agent.sock")

    @property
    def rootfs_img(self) -> Path:
        return self._meta_path(self.meta.get("rootfs"), self.dir / "rootfs.img")

    @property
    def control(self) -> Control:
        return Control(str(self.control_sock))

    def agent(self, connect_timeout: float | None = None) -> AgentConn:
        """Return a cached :class:`AgentConn`, reconnecting if it closed or the socket moved."""
        if self.meta.get("agent_sock") is None and "agent_sock" in self.meta:
            raise AgentClosed(f"VM '{self.name}' was launched without a guest agent")
        sock = str(self.agent_sock)
        with self._agent_lock:
            cached = self._agent_conn
            if cached is not None and cached.sock_path == sock and not cached.closed:
                return cached
        # Connect outside the lock so a slow/retrying connect never stalls other callers.
        conn = self._connect_agent(sock, connect_timeout)
        with self._agent_lock:
            current = self._agent_conn
            if current is not None and current.sock_path == sock and not current.closed:
                self._close_conn(conn)  # lost the race; reuse the connection another caller cached
                return current
            if current is not None and current is not conn:
                self._close_conn(current)  # drop a stale conn (closed or pointing at an old socket)
            self._agent_conn = conn
            return conn

    def wait_for_agent(self, timeout: float = 300.0) -> AgentConn:
        """Wait for the guest agent and fail early when the transport cannot appear."""
        deadline = time.monotonic() + max(0.0, float(timeout))
        last: BaseException | None = None
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(self._agent_timeout_message(timeout, last)) from last
            slice_timeout = min(1.0, max(0.001, remaining))
            try:
                conn = self.agent(connect_timeout=slice_timeout)
                conn.ping(timeout=slice_timeout)
                return conn
            except (AgentClosed, OSError, TimeoutError) as exc:
                last = exc
                self._close_agent()
                if message := self._agent_channel_failure():
                    raise TimeoutError(message) from exc
                time.sleep(min(0.05, max(0.0, remaining)))

    def _agent_channel_failure(self) -> str | None:
        log = self.logs()
        if "Run /.vmon/agent as init process" not in log:
            return None
        if "virtio_console" in log or "hvc0" in log:
            return None
        started = self.meta.get("started")
        try:
            started_at = (
                float(started) if started and isinstance(started, (int, float, str)) else 0.0
            )
        except ValueError:
            started_at = 0.0
        age = time.time() - started_at
        if age < 3.0:
            return None
        tail = self._console_tail(log)
        return (
            "guest agent channel did not come up: the kernel reached /.vmon/agent, "
            "but the console log shows no virtio-console/hvc0 device. Use a guest "
            "kernel with CONFIG_VIRTIO_CONSOLE or set VMON_KERNEL to one that "
            f"provides it; see {self.log}.\n{tail}"
        )

    def _agent_timeout_message(self, timeout: float, last: BaseException | None) -> str:
        if message := self._agent_channel_failure():
            return message
        detail = f": {last}" if last is not None else ""
        return f"agent did not answer within {timeout:g}s{detail}; see {self.log}"

    @staticmethod
    def _console_tail(log: str, lines: int = 12) -> str:
        tail = "\n".join(log.splitlines()[-lines:])
        return f"last console lines:\n{tail}" if tail else "console log is empty"

    def _connect_agent(self, sock: str, connect_timeout: float | None) -> AgentConn:
        deadline = None if connect_timeout is None else time.monotonic() + max(0.0, connect_timeout)
        last: BaseException | None = None
        while True:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            try:
                connect_slice = min(remaining if remaining is not None else 1.0, 1.0)
                return AgentConn(sock, connect_timeout=connect_slice)
            except (FileNotFoundError, ConnectionRefusedError, OSError) as e:
                last = e
                if deadline is not None and time.monotonic() >= deadline:
                    raise TimeoutError(f"agent socket {sock} not ready: {last}") from e
                time.sleep(min(0.01, remaining if remaining is not None else 0.01))

    def _close_agent(self) -> None:
        with self._agent_lock:
            conn, self._agent_conn = self._agent_conn, None
        self._close_conn(conn)

    @staticmethod
    def _close_conn(conn: AgentConn | None) -> None:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def is_running(self) -> bool:
        pid = self._meta_pid(self.meta.get("pid"))
        if not pid:
            return False
        proc_stat = Path(f"/proc/{pid}/stat")
        if proc_stat.exists():
            try:
                state = proc_stat.read_text().split(") ", 1)[1].split(" ", 1)[0]
            except FileNotFoundError, ProcessLookupError, IndexError:
                return False
            if state in ("Z", "X", "x"):
                try:
                    os.waitpid(pid, os.WNOHANG)
                except OSError:
                    pass
                return False
            return True
        # No /proc (e.g. macOS/HVF): a dead child VMM lingers as a zombie that
        # still answers kill(0), so reap it first when it is our child. A reaped
        # pid means it exited; ChildProcessError means it is not our child (a
        # rehydrated VMM after a daemon restart), so fall back to a liveness probe.
        try:
            reaped, _ = os.waitpid(pid, os.WNOHANG)
            return reaped != pid
        except OSError:
            pass  # not our child (rehydrated VMM) or unwaitable -> probe below
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def logs(self) -> str:
        try:
            return self.log.read_text(errors="replace")
        except FileNotFoundError:
            return ""

    @classmethod
    def boot_rootfs(
        cls,
        rootfs: str | os.PathLike[str],
        *,
        name: str | None = None,
        mem: int = 512,
        cpus: int = 1,
        snapshot_root: str | os.PathLike[str] | None = None,
        image: str | None = None,
        read_only: bool = False,
        agent: bool = True,
        overlay: bool = False,
        tap: str | None = None,
        fs_dir: str | os.PathLike[str] | None = None,
        volumes: list[tuple[str, str | os.PathLike[str], bool]] | None = None,
        timeout_secs: int | None = None,
    ) -> MicroVM:
        """Fresh direct-kernel boot from an ext4 rootfs.

        With ``overlay`` the rootfs is exposed as a copy-on-write overlay so the
        backing image is never mutated. ``tap`` attaches a virtio-net device bound
        to a pre-created host TAP (the restore path cannot synthesize a NIC the
        snapshot lacks, so networked sandboxes fresh-boot through here).
        ``fs_dir``/``volumes`` add virtio-fs shares and ``timeout_secs`` arms the
        VMM wall-clock deadline.
        """
        mem = _validate_int_range(mem, "mem", maximum=_MAX_MEM_MIB, unit=" MiB")
        cpus = _validate_int_range(cpus, "cpus", maximum=_MAX_CPUS)
        timeout_secs = _validate_timeout_secs(timeout_secs)
        name = name or _instance_name("vm")
        vm = cls(name)
        vm.dir.mkdir(parents=True, exist_ok=True)
        rootfs_p = Path(rootfs)
        args = [
            "--kernel",
            default_kernel(),
            "--mem",
            str(mem),
            "--cpus",
            str(cpus),
            "--api-sock",
            str(vm.sock),
            "--cmdline",
            _agent_cmdline() if agent else _cmdline(),
        ]
        if overlay:
            disk = vm.dir / "rootfs.img"
            args.extend(["--disk-overlay-of", str(rootfs_p), "--rootfs", str(disk)])
        else:
            disk = rootfs_p
            args.extend(["--rootfs", str(rootfs_p)])
            if read_only:
                args.append("--rootfs-ro")
        agent_sock = vm.dir / "agent.sock" if agent else None
        if agent_sock is not None:
            args.extend(["--agent-sock", str(agent_sock)])
        if tap is not None:
            args.extend(["--tap", str(tap)])
        if fs_dir is not None:
            args.extend(["--fs-tag", "host", "--fs-dir", str(fs_dir)])
        vols = list(volumes or [])
        args.extend(_volume_args(vols))
        if timeout_secs is not None:
            args.extend(["--timeout-secs", str(timeout_secs)])
        if snapshot_root is not None:
            args.extend(["--snapshot-root", str(snapshot_root)])
        vm._launch(
            args,
            control_sock=vm.sock,
            agent_sock=agent_sock,
            image=image,
            mem=mem,
            cpus=cpus,
            rootfs=str(disk),
            tap=tap,
            volumes=[{"tag": t, "dir": str(d), "ro": bool(ro)} for t, d, ro in vols],
            snapshot_root=str(snapshot_root) if snapshot_root else None,
        )
        return vm

    @classmethod
    def restore(
        cls,
        snapshot: str | os.PathLike[str],
        name: str | None = None,
        *,
        agent: bool = False,
        fs_dir: str | os.PathLike[str] | None = None,
        mem: int | None = None,
        cpus: int | None = None,
        volumes: list[tuple[str, str | os.PathLike[str], bool]] | None = None,
        timeout_secs: int | None = None,
    ) -> MicroVM:
        """Warm-boot a microVM from a snapshot directory or named snapshot."""
        snap_dir = cls._snapshot_dir(snapshot)
        if not _snapshot_state_present(snap_dir):
            raise FileNotFoundError(f"no snapshot {snapshot!r} at {snap_dir}")
        base_name = Path(snapshot).name if not isinstance(snapshot, Path) else snapshot.name
        name = name or _instance_name(base_name or "restore")
        vm = cls(name)
        vm.dir.mkdir(parents=True, exist_ok=True)
        args = ["--restore", str(snap_dir), "--api-sock", str(vm.sock)]
        if mem is not None:
            mem = _validate_int_range(mem, "mem", maximum=_MAX_MEM_MIB, unit=" MiB")
            args.extend(["--mem", str(mem)])
        if cpus is not None:
            cpus = _validate_int_range(cpus, "cpus", maximum=_MAX_CPUS)
            args.extend(["--cpus", str(cpus)])
        rootfs = None
        snap_rootfs = snap_dir / "rootfs.img"
        if snap_rootfs.is_file():
            rootfs = vm.dir / "rootfs.img"
            args.extend(["--disk-overlay-of", str(snap_rootfs), "--rootfs", str(rootfs)])
        agent_sock = vm.dir / "agent.sock" if agent else None
        if agent_sock is not None:
            args.extend(["--agent-sock", str(agent_sock)])
        if fs_dir is not None:
            args.extend(["--fs-tag", "host", "--fs-dir", str(fs_dir)])
        vols = list(volumes or [])
        args.extend(_volume_args(vols))
        timeout_secs = _validate_timeout_secs(timeout_secs)
        if timeout_secs is not None:
            args.extend(["--timeout-secs", str(timeout_secs)])
        args.extend(["--snapshot-root", str(STATE / "snapshots")])
        t0 = time.time()
        vm._launch(
            args,
            control_sock=vm.sock,
            agent_sock=agent_sock,
            restored_from=str(snapshot),
            rootfs=str(rootfs) if rootfs else None,
            snapshot_root=str(STATE / "snapshots"),
            volumes=[{"tag": t, "dir": str(d), "ro": bool(ro)} for t, d, ro in vols],
        )
        vm._record_reconstruct(t0)
        return vm

    @classmethod
    def fork_snapshot(
        cls,
        snapshot: str | os.PathLike[str],
        name: str | None = None,
        *,
        agent: bool = False,
        volumes: list[tuple[str, str | os.PathLike[str], bool]] | None = None,
        timeout_secs: int | None = None,
    ) -> MicroVM:
        snap_dir = cls._snapshot_dir(snapshot)
        if not _snapshot_state_present(snap_dir):
            raise FileNotFoundError(f"no snapshot {snapshot!r} at {snap_dir}")
        base_name = Path(snapshot).name if not isinstance(snapshot, Path) else snapshot.name
        name = name or _instance_name(f"{base_name or 'snapshot'}-fork")
        vm = cls(name)
        vm.dir.mkdir(parents=True, exist_ok=True)
        args = ["--fork-from", str(snap_dir), "--api-sock", str(vm.sock)]
        rootfs = None
        snap_rootfs = snap_dir / "rootfs.img"
        if snap_rootfs.is_file():
            rootfs = vm.dir / "rootfs.img"
            args.extend(["--disk-overlay-of", str(snap_rootfs), "--rootfs", str(rootfs)])
        agent_sock = vm.dir / "agent.sock" if agent else None
        if agent_sock is not None:
            args.extend(["--agent-sock", str(agent_sock)])
        vols = list(volumes or [])
        args.extend(_volume_args(vols))
        timeout_secs = _validate_timeout_secs(timeout_secs)
        if timeout_secs is not None:
            args.extend(["--timeout-secs", str(timeout_secs)])
        args.extend(["--snapshot-root", str(STATE / "snapshots")])
        t0 = time.time()
        vm._launch(
            args,
            control_sock=vm.sock,
            agent_sock=agent_sock,
            forked_from=str(snapshot),
            rootfs=str(rootfs) if rootfs else None,
            snapshot_root=str(STATE / "snapshots"),
            volumes=[{"tag": t, "dir": str(d), "ro": bool(ro)} for t, d, ro in vols],
        )
        vm._record_reconstruct(t0)
        return vm

    def fork(
        self, snapshot: str | os.PathLike[str], count: int = 1, names: list[str] | None = None
    ) -> list[MicroVM]:
        clones = []
        for i in range(count):
            cname = names[i] if names and i < len(names) else None
            clones.append(
                self.fork_snapshot(snapshot, name=cname, agent=bool(self.meta.get("agent_sock")))
            )
        return clones

    def _launch(
        self,
        extra_args: list[str],
        *,
        control_sock: Path | None = None,
        agent_sock: Path | None = None,
        launch_timeout: float = 15.0,
        **meta,
    ) -> None:
        control_sock = control_sock or self.sock
        snapshot_root = Path(meta.pop("snapshot_root", None) or (STATE / "snapshots"))
        snapshot_root.mkdir(parents=True, exist_ok=True)
        for sock in (control_sock, agent_sock):
            if sock is not None:
                Path(sock).unlink(missing_ok=True)
        binary = find_binary()
        self.dir.mkdir(parents=True, exist_ok=True)
        # vmon rejects a control/agent socket whose immediate parent dir is
        # group- or world-accessible; the process umask can leave it 0775, so
        # force each socket parent to 0700 before launch.
        for sock in (control_sock, agent_sock):
            if sock is not None:
                Path(sock).parent.chmod(0o700)
        launch_args = list(extra_args)
        if "--snapshot-root" not in launch_args:
            launch_args.extend(["--snapshot-root", str(snapshot_root)])
        # Local-dev escape hatch: disable vmon's Stage-B process filters
        # (seccomp/Landlock). Use only on a trusted host (e.g. a dev VM); never
        # for untrusted guests. See vmon's `--no-sandbox`.
        if (
            os.environ.get("VMON_NO_SANDBOX") not in (None, "", "0")
            and "--no-sandbox" not in launch_args
        ):
            launch_args.append("--no-sandbox")
        with self.log.open("wb") as logf:
            proc = subprocess.Popen(
                [binary, *launch_args],
                stdout=logf,
                stderr=logf,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
        self._save_meta(
            pid=proc.pid,
            sock=str(control_sock),
            agent_sock=str(agent_sock) if agent_sock else None,
            binary=binary,
            snapshot_root=str(snapshot_root),
            started=time.time(),
            status="running",
            **meta,
        )
        try:
            deadline = time.monotonic() + launch_timeout
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    tail = "\n".join(self.logs().splitlines()[-15:])
                    raise RuntimeError(
                        f"vmon exited (code {proc.returncode}) before the VM came up:\n{tail}"
                    )
                try:
                    self.control.wait_ready(timeout=0.05)
                    return
                except TimeoutError, OSError, RuntimeError:
                    time.sleep(0.002)
            raise TimeoutError(f"VM '{self.name}' control socket never came up; see {self.log}")
        except BaseException:
            try:
                self._cleanup_failed_launch(proc, control_sock, agent_sock)
            except Exception:
                pass
            raise

    def _cleanup_failed_launch(self, proc: subprocess.Popen, *socks: Path | None) -> None:
        if proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except OSError:
                try:
                    proc.terminate()
                except OSError:
                    pass
            try:
                proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except OSError:
                    try:
                        proc.kill()
                    except OSError:
                        pass
                try:
                    proc.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    pass
        for sock in socks:
            if sock is not None:
                Path(sock).unlink(missing_ok=True)
        self._save_meta(status="failed", returncode=proc.poll())

    def _record_reconstruct(self, started: float) -> None:
        reconstruct_ms = None
        m = re.search(r"reconstruct took ([0-9.]+)ms", self.logs())
        if m:
            reconstruct_ms = float(m.group(1))
        self._save_meta(
            restore_ms=round((time.time() - started) * 1000, 1), reconstruct_ms=reconstruct_ms
        )

    def pause(self):
        return self.control.pause()

    def resume(self):
        return self.control.resume()

    def snapshot(
        self,
        snapshot: str,
        keep_running: bool = True,
        *,
        disk_src: str | os.PathLike[str] | None = None,
        snapshot_root: str | os.PathLike[str] | None = None,
        base: str | None = None,
    ) -> Path:
        """Pause, optionally capture rootfs.img, snapshot VM state, then resume or stop.

        When ``base`` names a sibling snapshot (same launch ``--snapshot-root``), the
        VMM stores only the guest-RAM pages that differ from it, producing a delta
        (incremental) snapshot. Restore reconstructs the base+delta chain.

        ``snapshot_root`` only redirects the host-side ``rootfs.img`` copy and the
        returned directory; the VMM always writes state under its launch
        ``--snapshot-root`` joined with ``name``, so pass a root that matches it.
        """
        name = _valid_snapshot_name(str(snapshot))
        root = (
            Path(snapshot_root)
            if snapshot_root
            else self._meta_path(self.meta.get("snapshot_root"), STATE / "snapshots")
        )
        snap_dir = self._snapshot_dir(name, root=root)
        paused = False
        try:
            self.control.pause()
            paused = True
            if disk_src is not None:
                copy_reflink(disk_src, snap_dir / "rootfs.img")
            self.control.snapshot(name, base=base)
        except BaseException:
            if paused and keep_running:
                try:
                    self.control.resume()
                except BaseException:
                    pass
            raise
        if keep_running:
            self.control.resume()
        else:
            self.stop()
        return snap_dir

    def stop(self, wait: bool = True) -> None:
        self._close_agent()
        pid = self._meta_pid(self.meta.get("pid"))
        try:
            self.control.quit()
        except OSError, RuntimeError:
            if pid:
                try:
                    os.kill(pid, signal.SIGTERM)
                except OSError:
                    pass
        if wait and pid and not self._wait_pid_exit(pid, timeout=2.0):
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                pass
            if not self._wait_pid_exit(pid, timeout=1.0):
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
                self._wait_pid_exit(pid, timeout=1.0)
        self._save_meta(status="stopped")

    @staticmethod
    def _wait_pid_exit(pid: int, timeout: float) -> bool:
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            try:
                reaped, _ = os.waitpid(pid, os.WNOHANG)
                if reaped == pid:
                    return True
            except ChildProcessError:
                try:
                    os.kill(pid, 0)
                except OSError:
                    return True
            except OSError:
                try:
                    os.kill(pid, 0)
                except OSError:
                    return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.02)

    def remove(self) -> None:
        self._close_agent()
        if self.is_running():
            self.stop()
        shutil.rmtree(self.dir, ignore_errors=True)

    @staticmethod
    def _snapshot_dir(
        snapshot: str | os.PathLike[str], root: str | os.PathLike[str] | None = None
    ) -> Path:
        p = Path(snapshot)
        if p.is_absolute() or len(p.parts) > 1:
            return p
        return Path(root or (STATE / "snapshots")) / str(snapshot)

    @classmethod
    def list(cls) -> list[MicroVM]:
        vms_dir = STATE / "vms"
        if not vms_dir.is_dir():
            return []
        return [cls(p.name) for p in sorted(vms_dir.iterdir()) if p.is_dir()]


def _instance_name(prefix: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9_.-]", "-", prefix).strip(".-") or "vm"
    return f"{clean}-{int(time.time() * 1000) % 1_000_000:06d}"
