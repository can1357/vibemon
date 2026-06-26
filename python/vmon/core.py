"""Dependency-free engine: the single owner of the microVM registry.

``Engine`` owns ``~/.vmon`` (the registry over :data:`vmon.vmm.STATE`) and *all*
VM lifecycle logic — run, exec, logs, snapshot, restore, fork, stop, remove —
plus the Modal-style sandbox registry the web gateway exposes. It is consumed by
two transports:

* the stdlib Unix-socket daemon (:mod:`vmon.daemon`), reached by the thin ``vmon``
  CLI (:mod:`vmon.client`), and
* the FastAPI web gateway (:mod:`vmon.server`), whose ``Supervisor`` is a thin
  async adapter that maps :class:`EngineError` to HTTP status codes.

Because both transports share one ``Engine`` instance, a VM created over the
socket is visible over HTTP and vice versa. The engine imports nothing from
fastapi/uvicorn/starlette/pydantic so ``import vmon.core`` stays dependency-free.

Methods are synchronous and raise plain :class:`EngineError` subclasses; the
adapters translate them. Streaming methods take optional ``on_output(stream,
data: bytes)`` callbacks and a ``cancel`` :class:`threading.Event`; when
``on_output`` is ``None`` the call is non-streaming.
"""

from __future__ import annotations

import builtins
import os
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .vmm import MicroVM, _snapshot_state_present
from .volume import Volume

OnOutput = Callable[[str, bytes], None]

_RETURNCODE_UNSET = object()

#: Default image for a fresh ``vmon shell`` with no ref/snapshot/template.
DEFAULT_SHELL_IMAGE = os.environ.get("VMON_SHELL_IMAGE", "debian:stable-slim")

#: Default interactive shell command: prefer ``bash``, fall back to ``sh``.
#: ``exec`` replaces the launcher so the shell owns the PTY and its exit code.
_DEFAULT_SHELL_ARGV = [
    "/bin/sh",
    "-c",
    "command -v bash >/dev/null 2>&1 && exec bash -i || exec sh -i",
]


class EngineError(Exception):
    """Base engine failure; ``code`` lets adapters map to a transport error.

    The daemon copies ``code``/``message`` into its error frame; the FastAPI
    adapter maps ``code`` to an HTTP status (see :class:`vmon.server.Supervisor`).
    """

    code = "internal"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code


class NotFound(EngineError):
    """No VM with the requested name exists in the registry."""

    code = "not_found"


class NotRunning(EngineError):
    """The VM exists but is not in the ``running`` state."""

    code = "not_running"


class Busy(EngineError):
    """A required process-bound resource (e.g. a volume lock) is held elsewhere."""

    code = "busy"


class Invalid(EngineError):
    """The request is malformed (e.g. an empty exec command)."""

    code = "invalid"


class Unsupported(EngineError):
    """The backing sandbox does not implement the requested capability."""

    code = "unsupported"


def state_dir() -> Path:
    """The Vibemon home directory (``$VMON_HOME`` or ``~/.vmon``).

    Equivalent to :data:`vmon.vmm.STATE`; centralized here so the daemon and
    client derive socket/lock paths from a single source.
    """

    return Path(os.environ.get("VMON_HOME", str(Path.home() / ".vmon")))


@dataclass
class VMRecord:
    """One registry entry; the single record type shared by both transports.

    ``sandbox`` is the live :class:`vmon.sandbox.Sandbox` when the VM was created
    or attached this process lifetime (``None`` after a daemon restart until an
    operation re-attaches). ``volumes`` holds single-writer locks the engine
    re-acquired during :meth:`Engine.rehydrate` so :meth:`Engine.stop` can
    release them.
    """

    id: str
    name: str
    status: str
    sandbox: Any = None
    pid: int | None = None
    source: str | None = None
    created_at: float = field(default_factory=time.time)
    timeout: float | None = None
    detail: dict[str, Any] = field(default_factory=dict)
    tags: dict[str, str] = field(default_factory=dict)
    last_active: float = field(default_factory=time.time)
    terminated_at: float | None = None
    error: str | None = None
    volumes: list[Volume] = field(default_factory=list)

    @property
    def expires_at(self) -> float | None:
        if self.timeout is None or self.timeout <= 0:
            return None
        return self.last_active + self.timeout

    def touch(self) -> None:
        self.last_active = time.time()

    def view(self) -> dict[str, Any]:
        """The rich dict the HTTP gateway returns for a sandbox.

        ``detail`` (raw ``meta`` for daemon/rehydrated records, or curated detail
        for web-created ones) is spread first so the canonical fields below win;
        ``returncode`` prefers a terminate-captured exit code over the live probe.
        """
        return {
            **self.detail,
            "id": self.id,
            "name": self.name,
            "status": self.status,
            "created_at": self.created_at,
            "last_active": self.last_active,
            "expires_at": self.expires_at,
            "terminated_at": self.terminated_at,
            "error": self.error,
            "tags": dict(self.tags),
            "returncode": self.detail.get("returncode", getattr(self.sandbox, "returncode", None)),
        }


def _coerce_create_params(params: dict[str, Any]) -> dict[str, Any]:
    """Inflate JSON ``secrets``/``volumes`` into SDK objects for ``Sandbox.create``."""
    if "secrets" in params:
        from .secret import Secret

        params["secrets"] = [Secret.from_dict(s) for s in params.get("secrets") or []]
    if "volumes" in params:
        params["volumes"] = {
            mountpoint: Volume(name) for mountpoint, name in (params.get("volumes") or {}).items()
        }
    return params


class Engine:
    """Single owner of the microVM registry and all VM lifecycle logic."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._records: dict[str, VMRecord] = {}
        self.rehydrate()

    # -- backend injection (overridden in tests) ---------------------------

    @staticmethod
    def _sandbox_class() -> type[Any]:
        """The Sandbox class used for create/attach; patched by tests to a fake."""
        try:
            from .sandbox import Sandbox
        except ModuleNotFoundError as exc:
            if exc.name in {"vmon.sandbox", "sandbox"}:
                raise EngineError(
                    "vmon requires the Sandbox SDK; install a build that includes "
                    "python/vmon/sandbox.py"
                ) from exc
            raise
        return Sandbox

    # -- rehydration --------------------------------------------------------

    def rehydrate(self) -> None:
        """Rebuild the registry from ``~/.vmon/vms`` on (re)start.

        Disk is the source of truth: each ``vms/<name>/meta.json`` reconstructs a
        record (status from a pid-liveness check). For every *running* VM the
        engine re-asserts process-bound resources — volume single-writer locks
        are re-acquired (best-effort). Port tunnels are asyncio servers bound to
        the old process and cannot be restored, so networked records are flagged
        ``tunnels_lost``; the host TAP/iptables are host-persistent and need no
        action until stop/rm.
        """
        records: dict[str, VMRecord] = {}
        for vm in MicroVM.list():
            meta = vm.meta
            running = vm.is_running()
            saved_status = str(meta.get("status") or "")
            status = (
                "running"
                if running
                else (saved_status if saved_status in {"stopped", "terminated"} else "stopped")
            )
            record = VMRecord(
                id=vm.name,
                name=vm.name,
                status=status,
                pid=self._pid_of(meta),
                source=self._source_of(meta),
                detail=dict(meta),
                tags=self._tags_of(meta),
                timeout=self._timeout_of(meta),
            )
            if running:
                for vol_name in self._volume_names(meta):
                    try:
                        volume = Volume(vol_name)
                        volume.acquire()
                        record.volumes.append(volume)
                    except RuntimeError, ValueError:
                        # Lock held by a live VM, or an invalid name: leave it be.
                        continue
                if meta.get("tap"):
                    record.detail["tunnels_lost"] = True
            elif saved_status != status:
                self._persist_record_status(record, status)
            records[vm.name] = record
        with self._lock:
            self._records = records

    @staticmethod
    def _pid_of(meta: Mapping[str, object]) -> int | None:
        pid = meta.get("pid")
        return pid if isinstance(pid, int) and not isinstance(pid, bool) else None

    @staticmethod
    def _timeout_of(meta: Mapping[str, object]) -> float | None:
        timeout = meta.get("timeout_secs")
        if isinstance(timeout, bool):
            return None
        if isinstance(timeout, (int, float)):
            return float(timeout)
        return None

    @staticmethod
    def _tags_of(meta: Mapping[str, object]) -> dict[str, str]:
        tags = meta.get("tags")
        if not isinstance(tags, dict):
            return {}
        return {
            key: value
            for key, value in tags.items()
            if isinstance(key, str) and isinstance(value, str)
        }

    @staticmethod
    def _source_of(meta: Mapping[str, object]) -> str | None:
        for key in ("image", "restored_from", "forked_from", "restored_snapshot"):
            source = meta.get(key)
            if isinstance(source, str) and source:
                return source
        return None

    @staticmethod
    def _volume_names(meta: Mapping[str, object]) -> builtins.list[str]:
        """Volume names from a sandbox meta dict (``{mp: {name,...}}``).

        ``Sandbox.create`` records volumes keyed by mountpoint with a ``name``;
        direct ``MicroVM.restore/fork`` records a nameless ``[{tag,dir,ro}]`` list
        that carries no lock to re-acquire, so list-form entries are skipped.
        """
        vols = meta.get("volumes")
        if not isinstance(vols, dict):
            return []
        names: builtins.list[str] = []
        for info in vols.values():
            if isinstance(info, dict) and info.get("name"):
                names.append(str(info["name"]))
        return names

    # -- registry access ----------------------------------------------------

    def get(self, name: str, *, require_running: bool = False) -> VMRecord:
        with self._lock:
            record = self._records.get(name)
        if record is None:
            raise NotFound(f"unknown sandbox {name!r}")
        # Refresh a real VM's liveness (its VMM may have died/timed out) before
        # trusting the cached status. Records without a pid (e.g. fakes) stay
        # status-based. The pid-check runs outside the lock (disk/proc read).
        if require_running:
            self._refresh_record_status(record)
            if record.status != "running":
                raise NotRunning(f"sandbox {name!r} is not running")
        record.touch()
        return record

    def list(self, tags: dict[str, str] | None = None) -> builtins.list[VMRecord]:
        with self._lock:
            records = list(self._records.values())
        if not tags:
            return records
        out: builtins.list[VMRecord] = []
        for record in records:
            available = dict(record.detail.get("tags") or record.tags or {})
            sandbox_tags = getattr(record.sandbox, "tags", None)
            if isinstance(sandbox_tags, dict):
                available.update(sandbox_tags)
            if all(available.get(key) == value for key, value in tags.items()):
                out.append(record)
        return out

    def ps(self) -> builtins.list[dict[str, Any]]:
        """``[{name, status, pid, source}]`` with live records refreshed."""
        rows: builtins.list[dict[str, Any]] = []
        for record in self.list():
            vm = MicroVM(record.name)
            meta = vm.meta
            status = self._refresh_record_status(record, meta=meta)
            rows.append(
                {
                    "name": record.name,
                    "status": status,
                    "pid": meta.get("pid") or record.pid,
                    "source": record.source or self._source_of(meta) or "-",
                }
            )
        return rows

    def _refresh_record_status(self, record: VMRecord, meta: dict[str, Any] | None = None) -> str:
        """Refresh liveness without overwriting terminal registry states."""
        if record.status != "running":
            return record.status
        meta = MicroVM(record.name).meta if meta is None else meta
        pid = meta.get("pid") or record.pid
        if pid is None:
            return record.status
        if not MicroVM(record.name).is_running():
            self._persist_record_status(record, "stopped")
        return record.status

    def _persist_record_status(
        self,
        record: VMRecord,
        status: str,
        *,
        returncode: Any = _RETURNCODE_UNSET,
        terminated_at: float | None = None,
    ) -> None:
        """Update the in-memory record and persist terminal state best-effort."""
        record.status = status
        record.detail["status"] = status
        if terminated_at is not None:
            record.terminated_at = terminated_at
            record.detail["terminated_at"] = terminated_at
        meta_update: dict[str, Any] = {"status": status}
        if returncode is not _RETURNCODE_UNSET and returncode is not None:
            record.detail["returncode"] = int(returncode)
            meta_update["returncode"] = int(returncode)
        if terminated_at is not None:
            meta_update["terminated_at"] = terminated_at
        try:
            MicroVM(record.name)._save_meta(**meta_update)
        except Exception:
            pass

    # -- sandbox resolution -------------------------------------------------

    def _sandbox_for(self, name: str) -> Any:
        """The live sandbox for *name*, attaching a fresh one when none is held."""
        with self._lock:
            record = self._records.get(name)
        if record is not None and record.sandbox is not None:
            return record.sandbox
        return self._sandbox_class().attach(name)

    @staticmethod
    def _create_sandbox(sandbox_cls: type[Any], **params: Any) -> Any:
        """Create a sandbox and surface backend failures as engine errors."""
        try:
            return sandbox_cls.create(**params)
        except EngineError:
            raise
        except Exception as exc:
            raise EngineError(f"sandbox create failed: {exc}") from exc

    def _record_vm(self, vm: MicroVM, *, source: str | None, sandbox: Any = None) -> VMRecord:
        meta = vm.meta
        record = VMRecord(
            id=vm.name,
            name=vm.name,
            status="running",
            sandbox=sandbox,
            pid=self._pid_of(meta),
            source=source or self._source_of(meta),
            detail=dict(meta),
            tags=self._tags_of(meta),
            timeout=self._timeout_of(meta),
        )
        with self._lock:
            self._records[vm.name] = record
        return record

    # -- run / restore / fork ----------------------------------------------

    def run(
        self,
        params: dict[str, Any],
        on_output: OnOutput | None = None,
        cancel: threading.Event | None = None,
    ) -> dict[str, Any]:
        """Boot a container image (agent sandbox by default), Docker ``run`` style.

        Agent mode creates a :class:`Sandbox` and execs the image entrypoint;
        foreground streams stdout/stderr and ``run --rm``-terminates on exit or
        client cancel. ``no_agent`` uses the legacy console path. ``detach``
        returns immediately, leaving the VM running.
        """
        from .sandbox import default_command

        detach = bool(params.get("detach"))
        cmd = params.get("cmd") or None
        if params.get("no_agent"):
            vm = MicroVM.run(
                image=params.get("image"),
                dockerfile=params.get("dockerfile"),
                context=params.get("context") or ".",
                name=params.get("name"),
                mem=int(params.get("mem", 512)),
                cpus=int(params.get("cpus", 1)),
                cmd=cmd,
            )
            record = self._record_vm(vm, source=self._source_of(vm.meta))
            result = {"name": vm.name, "pid": vm.meta.get("pid"), "image": vm.meta.get("image")}
            if detach:
                return {**result, "detached": True}
            console_rc = self._stream_console(vm, on_output, cancel, terminate_on_cancel=True)
            self._persist_record_status(record, "stopped", returncode=console_rc)
            return {**result, "returncode": console_rc}

        sandbox_cls = self._sandbox_class()
        sandbox = self._create_sandbox(
            sandbox_cls,
            image=params.get("image"),
            dockerfile=params.get("dockerfile"),
            context=params.get("context") or ".",
            name=params.get("name"),
            cpus=int(params.get("cpus", 1)),
            memory=int(params.get("mem", 512)),
            disk_mb=int(params.get("disk_mb", 1024)),
            timeout=float(params.get("timeout", 300.0)),
        )
        record = self._record_vm(
            sandbox.vm,
            source=self._source_of(sandbox.vm.meta),
            sandbox=sandbox,
        )
        argv = default_command(sandbox.image_spec, cmd)
        proc = sandbox.exec(argv)
        # `vmon run` has no stdin channel (the client never forwards it), so give
        # the payload EOF like `docker run` without -i; otherwise an image whose
        # command reads stdin (e.g. the default /bin/sh) blocks forever.
        proc.close_stdin()
        result = {
            "name": sandbox.name,
            "pid": sandbox.vm.meta.get("pid"),
            "image": sandbox.vm.meta.get("image"),
        }
        if detach:
            return {**result, "detached": True}
        rc: int | None = None
        try:
            rc = self.pump_process(proc, on_output, cancel)
        finally:
            teardown_rc = self._teardown(record)
            self._persist_record_status(
                record, "stopped", returncode=rc if rc is not None else teardown_rc
            )
        return {**result, "returncode": int(rc if rc is not None else -1)}

    def restore(
        self,
        params: dict[str, Any],
        on_output: OnOutput | None = None,
        cancel: threading.Event | None = None,
    ) -> dict[str, Any]:
        """Warm-boot a VM from a snapshot; stream its console unless detached."""
        snapshot = params.get("snapshot")
        if not snapshot:
            raise Invalid("restore requires a snapshot name")
        vm = MicroVM.restore(snapshot, name=params.get("name"), agent=bool(params.get("agent")))
        self._record_vm(vm, source=f"restore:{snapshot}")
        meta = vm.meta
        result = {
            "name": vm.name,
            "reconstruct_ms": meta.get("reconstruct_ms"),
            "restore_ms": meta.get("restore_ms"),
        }
        if params.get("detach"):
            return {**result, "detached": True}
        rc = self._stream_console(vm, on_output, cancel, terminate_on_cancel=True)
        record = self.get(vm.name)
        self._persist_record_status(record, "stopped", returncode=rc)
        return {**result, "returncode": rc}

    def fork(self, params: dict[str, Any]) -> builtins.list[dict[str, Any]]:
        """Fork ``count`` copy-on-write clones from a snapshot."""
        snapshot = params.get("snapshot")
        if not snapshot:
            raise Invalid("fork requires a snapshot name")
        clones = MicroVM("_forker").fork(snapshot, count=int(params.get("count", 2)))
        out: builtins.list[dict[str, Any]] = []
        for clone in clones:
            self._record_vm(clone, source=f"fork:{snapshot}")
            out.append(
                {
                    "name": clone.name,
                    "pid": clone.meta.get("pid"),
                    "reconstruct_ms": clone.meta.get("reconstruct_ms"),
                }
            )
        return out

    def start_shell(self, params: dict[str, Any]) -> tuple[Any, str, Callable[[], None]]:
        """Resolve a shell target and start an interactive process inside it.

        ``ref`` resolves, in order, to: a running VM (attach — instant, *not*
        removed on exit), a ``vmon snapshot`` template (warm-boot an ephemeral
        VM), a filesystem template, or otherwise an image (cold-boot an ephemeral
        VM). ``image`` forces the image path. Returns ``(proc, name, cleanup)``;
        the caller pumps ``proc`` and MUST invoke ``cleanup`` exactly once —
        ``cleanup`` tears down ephemeral VMs and is a no-op for the attach case.
        """
        ref = params.get("ref")
        image = params.get("image")
        cmd = [str(c) for c in (params.get("cmd") or [])] or None
        env = params.get("env")
        tty = bool(params.get("tty", True))
        argv = cmd or list(_DEFAULT_SHELL_ARGV)

        if ref:
            with self._lock:
                exists = ref in self._records
            if exists:
                try:
                    self.get(ref, require_running=True)
                except NotRunning:
                    pass  # a stopped record: fall through to snapshot/template/image
                else:
                    proc = self._sandbox_for(ref).exec(*argv, env=env, tty=tty, _track_entry=False)
                    return proc, ref, (lambda: None)

        create = {
            "cpus": int(params.get("cpus", 1)),
            "memory": int(params.get("mem", 512)),
            "disk_mb": int(params.get("disk_mb", 1024)),
            "timeout": float(params.get("timeout", 300.0)),
            "block_network": True,
        }
        name: str | None = None
        sandbox: Any = None
        try:
            if ref and self._snapshot_exists(ref):
                vm = MicroVM.restore(ref, agent=True)
                name = vm.name
                # A freshly restored VM's agent socket may not be up yet: attach a
                # sandbox locally to block on readiness, then exec. Record it
                # *sandbox-less* — an attached sandbox carries no network handle, so
                # `remove()` must fall back to `vm.stop()` + host TAP/lease teardown
                # (Engine._teardown / _teardown_network) rather than a no-op terminate.
                attached = self._sandbox_class().attach(name)
                attached.wait_until_ready(timeout=float(params.get("timeout", 300.0)))
                self._record_vm(vm, source=f"shell:{ref}")
                proc = attached.exec(*argv, env=env, tty=tty, _track_entry=False)
            else:
                sandbox_cls = self._sandbox_class()
                if ref and self._template_exists(ref):
                    sandbox = self._create_sandbox(sandbox_cls, template=ref, **create)
                else:
                    sandbox = self._create_sandbox(
                        sandbox_cls, image=image or ref or DEFAULT_SHELL_IMAGE, **create
                    )
                name = sandbox.name
                self._record_vm(
                    sandbox.vm, source=self._source_of(sandbox.vm.meta) or "shell", sandbox=sandbox
                )
                proc = sandbox.exec(*argv, env=env, tty=tty, _track_entry=False)
        except BaseException:
            if name is not None:
                try:
                    self.remove(name)
                except Exception:
                    pass
            if sandbox is not None:
                try:
                    sandbox.terminate()
                except Exception:
                    pass
            raise

        if name is None:
            raise EngineError("shell setup did not return a VM name")

        def cleanup() -> None:
            self.remove(name)

        return proc, name, cleanup

    @staticmethod
    def _snapshot_exists(ref: str) -> bool:
        """True if ``ref`` names a restorable ``vmon snapshot`` template."""
        try:
            return _snapshot_state_present(MicroVM._snapshot_dir(ref))
        except Exception:
            return False

    @staticmethod
    def _template_exists(ref: str) -> bool:
        """True if ``ref`` names a filesystem template under ``$VMON_HOME/templates``."""
        return (state_dir() / "templates" / ref).exists()

    # -- exec / logs / cp ---------------------------------------------------

    def start_exec(
        self,
        name: str,
        cmd: builtins.list[str],
        *,
        env: dict[str, str] | None = None,
        workdir: str | None = None,
        tty: bool = False,
        timeout: float | None = None,
    ) -> Any:
        """Start an exec session inside a running VM; returns the live ``Process``.

        The caller (daemon or HTTP adapter) pumps ``proc.stdout``/``stderr``,
        forwards stdin, and waits on ``proc.wait()``.
        """
        if not cmd:
            raise Invalid("exec cmd must not be empty")
        self.get(name, require_running=True)
        sandbox = self._sandbox_for(name)
        return sandbox.exec(*cmd, workdir=workdir, env=env, tty=tty, timeout=timeout)

    def logs(
        self,
        name: str,
        *,
        follow: bool = False,
        on_line: OnOutput | None = None,
        cancel: threading.Event | None = None,
    ) -> str | None:
        """Return or stream a VM's console. Read-only: never affects the VM."""
        self.get(name)
        vm = MicroVM(name)
        if on_line is None:
            return vm.logs()
        if not follow:
            data = vm.logs()
            if data:
                on_line("console", data.encode("utf-8", "replace"))
            return None
        self._stream_console(vm, on_line, cancel, terminate_on_cancel=False)
        return None

    def cp_read(self, name: str, path: str) -> bytes:
        """Read *path* from the guest filesystem."""
        data = self._sandbox_for(name).filesystem.read_bytes(path)
        return data if isinstance(data, bytes) else bytes(data)

    def cp_write(self, name: str, path: str, data: bytes) -> None:
        """Write *data* to *path* in the guest filesystem."""
        self._sandbox_for(name).filesystem.write_bytes(path, data)

    # filesystem methods reused by the HTTP gateway (live sandbox or attach)
    read_file = cp_read

    def write_file(self, name: str, path: str, data: bytes) -> None:
        self.cp_write(name, path, data)

    def delete_file(self, name: str, path: str, recursive: bool = False) -> None:
        self._sandbox_for(name).filesystem.remove(path, recursive=recursive)

    def list_files(self, name: str, path: str) -> builtins.list[dict[str, Any]]:
        return list(self._sandbox_for(name).filesystem.list_files(path))

    def stat_file(self, name: str, path: str) -> dict[str, Any]:
        return dict(self._sandbox_for(name).filesystem.stat(path))

    # -- lifecycle control --------------------------------------------------

    def pause(self, name: str) -> dict[str, Any]:
        self.get(name, require_running=True)
        return dict(MicroVM(name).pause() or {})

    def resume(self, name: str) -> dict[str, Any]:
        self.get(name, require_running=True)
        return dict(MicroVM(name).resume() or {})

    def snapshot_template(self, name: str, snapshot: str, stop: bool = False) -> str:
        """Snapshot guest machine state into a named template (distinct from a
        filesystem image, see :meth:`snapshot_filesystem`)."""
        record = self.get(name, require_running=True)
        vm = MicroVM(name)
        snap_dir = vm.snapshot(
            snapshot,
            keep_running=not stop,
            disk_src=vm.rootfs_img if vm.rootfs_img.exists() else None,
        )
        if stop:
            # Route the stop through engine teardown so a sandbox VM's TAP/lease
            # and rehydrated volume locks are released, not just the VMM process.
            rc = self._teardown(record)
            self._persist_record_status(record, "stopped", returncode=rc)
        return str(snap_dir)

    def stop(self, name: str) -> dict[str, Any]:
        """Stop a VM and release its process-bound resources (idempotent)."""
        record = self.get(name)
        if record.status == "terminated":
            return {"name": name, "status": record.status}
        rc = self._teardown(record)
        self._persist_record_status(record, "stopped", returncode=rc)
        return {"name": name, "status": "stopped"}

    def remove(self, name: str) -> dict[str, Any]:
        """Stop (if needed) then delete a VM and drop its registry entry."""
        record = self.get(name)
        self._teardown(record)
        MicroVM(name).remove()
        with self._lock:
            self._records.pop(name, None)
        return {"name": name, "removed": True}

    def _teardown(self, record: VMRecord) -> int | None:
        """Release every host resource a VM holds (idempotent, best-effort).

        A live sandbox owns the full teardown (agent, network, volumes, VM); a
        rehydrated/attached record falls back to stopping the VM, tearing down
        network state from the on-disk lease, and releasing re-acquired volumes.
        The best return code observed before or after teardown is returned so
        callers can preserve final status in the registry.
        """
        name = record.name
        sandbox = record.sandbox
        returncode = self._sandbox_returncode(sandbox)
        if sandbox is not None:
            try:
                term = getattr(sandbox, "terminate", None) or getattr(sandbox, "stop", None)
                if term is not None:
                    term()
            except Exception:
                pass
            finally:
                if returncode is None:
                    returncode = self._sandbox_returncode(sandbox)
                record.sandbox = None
            return returncode
        vm = MicroVM(name)
        try:
            if vm.is_running():
                vm.stop()
        finally:
            self._teardown_network(name)
            for volume in record.volumes:
                try:
                    volume.release()
                except Exception:
                    pass
            record.volumes = []
        return returncode

    @staticmethod
    def _sandbox_returncode(sandbox: Any) -> int | None:
        if sandbox is None:
            return None
        try:
            rc = getattr(sandbox, "returncode", None)
        except Exception:
            return None
        return int(rc) if rc is not None else None

    @staticmethod
    def _teardown_network(name: str) -> None:
        """Best-effort host-network teardown from the persisted lease.

        Removes the TAP and base NAT/FORWARD rules and drops the lease. Exact
        per-CIDR/domain egress rules are not reconstructed here; they reference a
        TAP that is deleted, so they go inert. No-ops when the VM had no network.
        """
        try:
            from . import net
        except Exception:
            return
        lease = net.lease_for(name)
        if lease is None:
            return
        try:
            tap = lease.get("tap")
            guest_ip_raw = lease.get("guest_ip")
            host_ip_raw = lease.get("host_ip")
            prefix_raw = lease.get("prefix")
            guest_ip = guest_ip_raw if isinstance(guest_ip_raw, str) else None
            host_ip = host_ip_raw if isinstance(host_ip_raw, str) else None
            if isinstance(prefix_raw, int) and not isinstance(prefix_raw, bool):
                prefix = prefix_raw
            elif isinstance(prefix_raw, str):
                prefix = int(prefix_raw)
            else:
                prefix = 30
            if not isinstance(tap, str):
                return
            net.teardown_tap(tap, guest_ip, host_ip, prefix)
        except Exception:
            pass
        try:
            net.release_guest_config(name)
        except Exception:
            pass

    # -- HTTP-gateway registry methods (sync bodies) ------------------------

    def create(self, params: dict[str, Any]) -> VMRecord:
        """Create a Modal-style sandbox and register it (used by the web gateway)."""
        request_time = time.time()
        params = _coerce_create_params(dict(params))
        sid = str(params.get("name") or f"sb-{uuid.uuid4().hex[:12]}")
        params["name"] = sid
        sandbox_cls = self._sandbox_class()
        sandbox = self._create_sandbox(sandbox_cls, **params)
        now = time.time()
        tags = dict(params.get("tags") or getattr(sandbox, "tags", {}) or {})
        detail = {
            "image": params.get("image"),
            "template": params.get("template"),
            "tags": tags,
            "cpus": params.get("cpus"),
            "memory": params.get("memory"),
            "disk_mb": params.get("disk_mb"),
            "block_network": params.get("block_network"),
            "egress_allow": params.get("egress_allow"),
            "egress_allow_domains": params.get("egress_allow_domains"),
            "inbound_cidr_allowlist": params.get("inbound_cidr_allowlist"),
            "pool_size": params.get("pool_size"),
            "create_latency_ms": (now - request_time) * 1000.0,
        }
        record = VMRecord(
            id=sid,
            name=str(params["name"]),
            status="running",
            sandbox=sandbox,
            pid=getattr(getattr(sandbox, "vm", None), "meta", {}).get("pid")
            if hasattr(sandbox, "vm")
            else None,
            source=params.get("image") or params.get("template"),
            created_at=now,
            timeout=params.get("timeout"),
            tags=tags,
            detail=detail,
        )
        with self._lock:
            self._records[sid] = record
        return record

    def terminate(self, name: str, *, reason: str = "api") -> VMRecord:
        """Terminate a running sandbox (web semantics: status -> ``terminated``)."""
        record = self.get(name, require_running=True)
        returncode = self._teardown(record)
        if self._detect_oom(name):
            reason = "oom"
            record.detail["oom"] = True
        terminated_at = time.time()
        self._persist_record_status(
            record, "terminated", returncode=returncode, terminated_at=terminated_at
        )
        record.detail["terminated_reason"] = reason
        try:
            MicroVM(name)._save_meta(terminated_reason=reason, oom=bool(record.detail.get("oom")))
        except Exception:
            pass
        return record

    def snapshot_filesystem(self, name: str, snapshot: str | None = None) -> str:
        sandbox = self._sandbox_for(name)
        fn = getattr(sandbox, "snapshot_filesystem", None) or getattr(sandbox, "snapshot", None)
        if fn is None:
            raise Unsupported("sandbox snapshot API unavailable")
        snap = fn(snapshot)
        return str(snap if snap is not None else snapshot)

    def extend(self, name: str, secs: int) -> dict[str, Any]:
        sandbox = self._sandbox_for(name)
        fn = getattr(sandbox, "extend", None)
        if fn is None:
            raise Unsupported("sandbox extend API unavailable")
        return dict(fn(secs) or {})

    def set_network_policy(
        self,
        name: str,
        *,
        block_network: bool | None = None,
        cidr_allow: builtins.list[str] | None = None,
        domain_allow: builtins.list[str] | None = None,
    ) -> dict[str, Any]:
        sandbox = self._sandbox_for(name)
        setter = getattr(sandbox, "set_network_policy", None)
        if setter is None:
            raise Unsupported("sandbox network policy API unavailable")
        result = setter(
            block_network=block_network, cidr_allow=cidr_allow, domain_allow=domain_allow
        )
        return dict(result or {})

    def tunnels(self, name: str) -> dict[int, tuple[str, int]]:
        sandbox = self._sandbox_for(name)
        tunnels = getattr(sandbox, "tunnels", None)
        if tunnels is None:
            raise Unsupported("sandbox tunnel API unavailable")
        result = tunnels() if callable(tunnels) else tunnels
        normalized: dict[int, tuple[str, int]] = {}
        for port, target in dict(result or {}).items():
            host, host_port = target
            normalized[int(port)] = (str(host), int(host_port))
        return normalized

    @staticmethod
    def _detect_oom(name: str) -> bool:
        """True if a VM's cgroup recorded an OOM kill (best-effort)."""
        meta = MicroVM(name).meta
        if not isinstance(meta, dict):
            return False
        candidates: builtins.list[Path] = []
        for key in ("memory_events", "memory_events_path"):
            if meta.get(key):
                candidates.append(Path(str(meta[key])))
        jail_root = Path(str(meta["jail_root"])) if meta.get("jail_root") else None
        for key in ("cgroup_path", "cgroup", "cgroup_dir"):
            if meta.get(key):
                cgroup = Path(str(meta[key]))
                candidates.append(cgroup / "memory.events")
                if jail_root is not None and not cgroup.is_absolute():
                    candidates.append(jail_root / cgroup / "memory.events")
        candidates.append(Path("/sys/fs/cgroup") / "vmon" / name / "memory.events")
        for path in candidates:
            try:
                for line in path.read_text().splitlines():
                    key, _, value = line.partition(" ")
                    if key == "oom_kill" and int(value or "0") > 0:
                        return True
            except Exception:
                continue
        return False

    # -- streaming helpers --------------------------------------------------

    def _stream_console(
        self,
        vm: MicroVM,
        on_output: OnOutput | None,
        cancel: threading.Event | None,
        terminate_on_cancel: bool,
    ) -> int:
        """Tail a VM's console until it exits. ``cancel`` stops streaming; when
        ``terminate_on_cancel`` it also stops the VM (foreground ``run``)."""
        printed = 0
        while True:
            data = vm.logs()
            if len(data) > printed:
                if on_output is not None:
                    on_output("console", data[printed:].encode("utf-8", "replace"))
                printed = len(data)
            if not vm.is_running():
                data = vm.logs()
                if len(data) > printed and on_output is not None:
                    on_output("console", data[printed:].encode("utf-8", "replace"))
                return 0
            if cancel is not None and cancel.is_set():
                if terminate_on_cancel:
                    try:
                        vm.stop()
                    except Exception:
                        pass
                return -1
            time.sleep(0.1)

    def pump_process(
        self, proc: Any, on_output: OnOutput | None = None, cancel: threading.Event | None = None
    ) -> int:
        """Stream a process's stdout/stderr via ``on_output`` and return its code.

        Reader threads drain each stream; the wait is cancel-aware so a client
        disconnect kills the entry exec (the foreground ``run`` is bound to the
        connection).
        """
        threads: builtins.list[threading.Thread] = []
        for stream_name in ("stdout", "stderr"):
            stream = getattr(proc, stream_name, None)
            if stream is None:
                continue
            thread = threading.Thread(
                target=self._drain_stream, args=(stream_name, stream, on_output), daemon=True
            )
            thread.start()
            threads.append(thread)
        try:
            rc = self._wait_cancellable(proc, cancel)
        finally:
            self._close_streams(proc)
        for thread in threads:
            thread.join(timeout=5.0)
        return rc

    @staticmethod
    def _drain_stream(stream_name: str, stream: Any, on_output: OnOutput | None) -> None:
        try:
            for chunk in stream:
                if chunk and on_output is not None:
                    on_output(stream_name, chunk if isinstance(chunk, bytes) else bytes(chunk))
        except Exception:
            pass

    @staticmethod
    def _close_streams(proc: Any) -> None:
        for stream_name in ("stdout", "stderr"):
            stream = getattr(proc, stream_name, None)
            close = getattr(stream, "close", None)
            if close is not None:
                try:
                    close()
                except Exception:
                    pass

    @staticmethod
    def _wait_cancellable(proc: Any, cancel: threading.Event | None) -> int:
        if cancel is None:
            try:
                return int(proc.wait())
            except Exception:
                return -1
        done = threading.Event()
        box: dict[str, int] = {}

        def waiter() -> None:
            try:
                box["rc"] = int(proc.wait())
            except Exception:
                box["rc"] = -1
            finally:
                done.set()

        threading.Thread(target=waiter, daemon=True).start()
        while not done.wait(0.1):
            if cancel.is_set():
                try:
                    proc.kill()
                except Exception:
                    pass
                done.wait(5.0)
                break
        return box.get("rc", -1)
