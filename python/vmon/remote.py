"""SDK client selector and gateway-backed sandbox facade."""

from __future__ import annotations

import base64
import contextlib
import os
import queue
import secrets
import threading
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, BinaryIO, cast, overload

from .client import DaemonError, LocalTransport, MeshTransport, Transport
from .context import LOCAL, Context, ContextStore
from .secret import Secret

if TYPE_CHECKING:
    from .sandbox import RemoteFunction

__all__ = [
    "RemoteFilesystem",
    "RemoteProcess",
    "RemoteSandbox",
    "SandboxCollection",
    "VmonClient",
    "connect",
]

_DEFAULT_CREATE_CMD = ["sh", "-c", "sleep 2147483647"]
_STDIN_EOF = object()
_HOST_VOLUME_KEYS = frozenset({"host_path", "host_dir", "host", "source", "src"})


def connect(
    context: str | None = None,
    *,
    endpoints: list[str] | None = None,
    token: str | None = None,
) -> VmonClient:
    """Connect the Python SDK to local ``vmond`` or to a named mesh context.

    Resolution matches the CLI transport selector: explicit endpoints win, then
    an explicit context name, then ``VMON_CONTEXT`` / the persisted current
    context, and finally the local daemon when nothing remote is selected.
    """

    if endpoints is not None:
        return VmonClient(
            MeshTransport(list(endpoints), _effective_token(token)),
            context_name=context,
            local=False,
        )

    store = ContextStore()
    store.load()
    selected = context if context is not None else store.current_name()
    if not selected or selected == LOCAL:
        return VmonClient(LocalTransport(), context_name=LOCAL, local=True)

    ctx = store.get(selected)
    if ctx is None:
        raise DaemonError(f"no such context {selected!r}", code="invalid")
    if not ctx.endpoints:
        raise DaemonError(f"context {selected!r} has no endpoints", code="invalid")

    def persist_roster(roster: list[str]) -> None:
        current = store.get(ctx.name)
        if current is None:
            return
        store.put(
            Context(
                current.name,
                list(roster),
                token=current.token,
                region=current.region,
                updated=time.time(),
            )
        )

    return VmonClient(
        MeshTransport(
            list(ctx.endpoints),
            _effective_token(token, ctx, store),
            on_roster=persist_roster,
        ),
        context_name=ctx.name,
        local=False,
    )


def _effective_token(
    explicit: str | None,
    ctx: Context | None = None,
    store: ContextStore | None = None,
) -> str | None:
    """Resolve the bearer token: explicit arg, env, in-memory context, saved file."""
    if explicit is not None:
        return explicit
    env_token = os.environ.get("VMON_API_TOKEN")
    if env_token:
        return env_token
    if ctx is not None and ctx.token:
        return ctx.token
    if ctx is not None and store is not None:
        return store.load_token(ctx.name)
    return None


class VmonClient:
    """High-level SDK client bound to one local or mesh transport."""

    def __init__(
        self,
        transport: Transport,
        *,
        context_name: str | None = None,
        local: bool = False,
    ) -> None:
        self.transport = transport
        self.context_name = context_name
        self._local = bool(local)
        self.sandboxes = SandboxCollection(self)

    @overload
    def function(self, fn: Callable[..., Any]) -> RemoteFunction: ...

    @overload
    def function(
        self, fn: None = None, **sandbox_kwargs: Any
    ) -> Callable[[Callable[..., Any]], RemoteFunction]: ...

    def function(
        self,
        fn: Callable[..., Any] | None = None,
        **sandbox_kwargs: Any,
    ) -> RemoteFunction | Callable[[Callable[..., Any]], RemoteFunction]:
        """Decorate a source-available function to run through this client's sandbox plane."""

        from .sandbox import RemoteFunction

        def decorate(func: Callable[..., Any]) -> RemoteFunction:
            return RemoteFunction(func, sandbox_kwargs, sandbox_factory=self.sandboxes.create)

        if fn is not None:
            return decorate(fn)
        return decorate


class SandboxCollection:
    """Create, list, and attach sandboxes through a :class:`VmonClient`."""

    def __init__(self, client: VmonClient) -> None:
        self._client = client

    def create(self, image: str | None = None, **kwargs: Any) -> Any:
        if self._client._local:
            from .sandbox import Sandbox

            local_kwargs = dict(kwargs)
            if "mem" in local_kwargs and "memory" not in local_kwargs:
                local_kwargs["memory"] = local_kwargs.pop("mem")
            local_kwargs.pop("cmd", None)
            local_kwargs["context"] = local_kwargs.get("context") or "."
            return Sandbox.create(image=image, **local_kwargs)
        return RemoteSandbox.create(self._client.transport, image=image, **kwargs)

    def list(self, *, tags: Mapping[str, str] | None = None) -> list[dict[str, Any]]:
        result = self._client.transport.call("ps")
        rows = result.get("sandboxes") or result.get("vms") or []
        if not isinstance(rows, list):
            return []
        normalized = [dict(row) for row in rows if isinstance(row, Mapping)]
        if tags:
            wanted = {str(key): str(value) for key, value in tags.items()}
            normalized = [
                row
                for row in normalized
                if all(
                    str((row.get("tags") or {}).get(key)) == value
                    for key, value in wanted.items()
                )
            ]
        return normalized

    def get(self, sandbox_id: str) -> Any:
        if self._client._local:
            from .sandbox import Sandbox

            return Sandbox.attach(str(sandbox_id))
        return RemoteSandbox.from_id(self._client.transport, str(sandbox_id))


class RemoteSandbox:
    """Transport-backed sandbox facade for a gateway-owned sandbox."""

    def __init__(
        self,
        transport: Transport,
        view: Mapping[str, Any],
        *,
        env: Mapping[str, str] | None = None,
        secret_env: Mapping[str, str] | None = None,
        workdir: str | None = None,
    ) -> None:
        self.transport = transport
        self._view = dict(view)
        sandbox_id = self._view.get("id") or self._view.get("name")
        if not sandbox_id:
            raise DaemonError("gateway did not return a sandbox id", code="invalid")
        self.sandbox_id = str(sandbox_id)
        self.name = str(self._view.get("name") or self.sandbox_id)
        self.workdir = workdir or _optional_str(self._view.get("workdir")) or "/"
        self.env = {str(k): str(v) for k, v in (env or {}).items()}
        self._secret_env = {str(k): str(v) for k, v in (secret_env or {}).items()}
        self.tags = _string_mapping(self._view.get("tags"))
        self.filesystem = RemoteFilesystem(self)
        self._entry_process: RemoteProcess | None = None
        self._entry_returncode: int | None = None
        self._terminated = False

    @classmethod
    def create(
        cls,
        transport: Transport,
        image: str | None = None,
        *,
        template: str | os.PathLike[str] | None = None,
        dockerfile: str | None = None,
        name: str | None = None,
        cpus: int = 1,
        mem: int | None = None,
        memory: int | None = None,
        disk_mb: int = 1024,
        timeout: float = 300,
        timeout_secs: int | None = None,
        workdir: str | None = None,
        env: Mapping[str, str] | None = None,
        secrets: Iterable[Secret | Mapping[str, object]] | None = None,
        volumes: Mapping[str, Any] | None = None,
        tags: Mapping[str, str] | None = None,
        fs_dir: str | None = None,
        block_network: bool = False,
        ports: Sequence[int] | None = None,
        egress_allow: Sequence[str] | None = None,
        egress_allow_domains: Sequence[str] | None = None,
        inbound_cidr_allowlist: Sequence[str] | None = None,
        readiness_probe: Any = None,
        pool_size: int = 0,
        detach: bool = True,
        cmd: Sequence[str] | None = None,
        idempotency_key: str | None = None,
        **extra: Any,
    ) -> RemoteSandbox:
        _reject_local_only(dockerfile=dockerfile, fs_dir=fs_dir, volumes=volumes)
        if readiness_probe is not None:
            raise DaemonError(
                "readiness_probe is local-only in the SDK remote facade", code="unsupported"
            )
        if pool_size:
            raise DaemonError(
                "pool_size is local-only in the SDK remote facade", code="unsupported"
            )

        secret_payload, secret_env = _serialize_secrets(secrets)
        volume_payload = _serialize_volumes(volumes)
        effective_mem = int(mem if mem is not None else (memory if memory is not None else 512))
        payload: dict[str, Any] = {
            "image": image,
            "template": str(template) if template is not None else None,
            "name": name,
            "cpus": int(cpus),
            "mem": effective_mem,
            "memory": effective_mem,
            "disk_mb": int(disk_mb),
            "timeout": float(timeout),
            "timeout_secs": timeout_secs,
            "workdir": workdir,
            "env": _string_mapping(env),
            "secrets": secret_payload,
            "volumes": volume_payload,
            "tags": _string_mapping(tags),
            "block_network": bool(block_network),
            "ports": list(ports) if ports is not None else None,
            "egress_allow": list(egress_allow) if egress_allow is not None else None,
            "egress_allow_domains": list(egress_allow_domains)
            if egress_allow_domains is not None
            else None,
            "inbound_cidr_allowlist": list(inbound_cidr_allowlist)
            if inbound_cidr_allowlist is not None
            else None,
            "detach": bool(detach),
            "cmd": list(cmd) if cmd is not None else list(_DEFAULT_CREATE_CMD),
            "idempotency_key": idempotency_key or _new_idempotency_key(),
        }
        payload.update(extra)
        clean = {key: value for key, value in payload.items() if value is not None}
        view = transport.call("run", **clean)
        return cls(
            transport,
            view,
            env=_string_mapping(env),
            secret_env=secret_env,
            workdir=workdir,
        )

    @classmethod
    def from_id(cls, transport: Transport, sandbox_id: str) -> RemoteSandbox:
        return cls(transport, transport.call("inspect", name=sandbox_id))

    def exec(
        self,
        *cmd: str | Iterable[str],
        workdir: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        pty: bool = False,
        tty: bool = False,
        _track_entry: bool = True,
    ) -> RemoteProcess:
        if len(cmd) == 1 and not isinstance(cmd[0], str):
            argv = [str(part) for part in cmd[0]]
        else:
            argv = [str(part) for part in cmd]
        if not argv:
            raise ValueError("exec command must not be empty")
        merged_env = {**self.env, **self._secret_env}
        if env:
            merged_env.update({str(k): str(v) for k, v in env.items()})
        proc = RemoteProcess(
            self.transport,
            self.sandbox_id,
            argv,
            workdir=workdir or self.workdir,
            env=merged_env or None,
            timeout=timeout,
            tty=bool(pty or tty),
        )
        if _track_entry and self._entry_process is None:
            self._entry_process = proc
        return proc

    def logs(self, *, follow: bool = False, encoding: str = "utf-8") -> str:
        chunks: list[bytes] = []

        def on_event(_stream: str, data: bytes) -> None:
            chunks.append(data)

        self.transport.stream("logs", on_event, name=self.sandbox_id, follow=follow)
        return b"".join(chunks).decode(encoding, "replace")

    def inspect(self) -> dict[str, Any]:
        self._view = self.transport.call("inspect", name=self.sandbox_id)
        return dict(self._view)

    def poll(self) -> int | None:
        if self._entry_returncode is None and self._entry_process is not None:
            rc = self._entry_process.returncode
            if rc is not None:
                self._entry_returncode = rc
        if self._entry_returncode is not None:
            return self._entry_returncode
        view = self.inspect()
        rc = view.get("returncode")
        if isinstance(rc, int):
            return rc
        status = view.get("status")
        if isinstance(status, str) and status.lower() in {"stopped", "terminated", "exited"}:
            return 0
        return None

    @property
    def returncode(self) -> int | None:
        return self.poll()

    def wait(self, timeout: float | None = None) -> int:
        deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
        while True:
            rc = self.poll()
            if rc is not None:
                return rc
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"sandbox {self.sandbox_id} did not exit within {timeout}s")
            time.sleep(0.1)

    def terminate(self, wait: bool = True) -> None:
        if self._terminated:
            return
        self._terminated = True
        with contextlib.suppress(DaemonError):
            self.transport.call("stop", name=self.sandbox_id)
        with contextlib.suppress(DaemonError):
            self.transport.call("rm", name=self.sandbox_id)
        if wait:
            self._entry_returncode = 0

    def extend(self, secs: int) -> dict[str, Any]:
        return self.transport.call("extend", name=self.sandbox_id, secs=int(secs))

    def metrics(self) -> dict[str, Any]:
        return self.transport.call("metrics", name=self.sandbox_id)

    def snapshot(self, name: str | None = None) -> str:
        snapshot = name or f"{self.name}-{int(time.time() * 1000)}"
        result = self.transport.call(
            "snapshot", name=self.sandbox_id, snapshot=snapshot, stop=False
        )
        return str(result.get("snapshot") or result.get("image") or snapshot)

    def snapshot_filesystem(self, name: str | None = None, ttl: int | None = None) -> str:
        del ttl
        return self.snapshot(name)


class RemoteFilesystem:
    """Filesystem facade backed by gateway file/list/stat routes."""

    def __init__(self, sandbox: RemoteSandbox) -> None:
        self._sandbox = sandbox

    def read_bytes(self, path: str) -> bytes:
        result = self._sandbox.transport.call(
            "cp_read", name=self._sandbox.sandbox_id, path=str(path)
        )
        data = result.get("data")
        if not isinstance(data, str):
            raise DaemonError("gateway returned malformed file data", code="invalid")
        return base64.b64decode(data)

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(path).decode(encoding)

    def write_bytes(
        self, path: str, data: bytes | bytearray | memoryview, mode: int = 0o644
    ) -> None:
        encoded = base64.b64encode(bytes(data)).decode("ascii")
        self._sandbox.transport.call(
            "cp_write", name=self._sandbox.sandbox_id, path=str(path), data=encoded, mode=int(mode)
        )

    def write_text(self, path: str, text: str, encoding: str = "utf-8", mode: int = 0o644) -> None:
        self.write_bytes(path, text.encode(encoding), mode=mode)

    def list_files(self, path: str = ".") -> list[dict[str, Any]]:
        result = self._sandbox.transport.call("fs_list", name=self._sandbox.sandbox_id, path=path)
        entries = result.get("entries") or result.get("files") or []
        return [dict(entry) for entry in entries if isinstance(entry, Mapping)]

    def list(self, path: str = ".") -> list[dict[str, Any]]:
        return self.list_files(path)

    def stat(self, path: str) -> dict[str, Any]:
        return self._sandbox.transport.call("fs_stat", name=self._sandbox.sandbox_id, path=path)

    def make_directory(self, path: str, parents: bool = True) -> None:
        argv = ["mkdir", "-p", path] if parents else ["mkdir", path]
        self._run_filesystem_command(argv)

    def mkdir(self, path: str, parents: bool = True) -> None:
        self.make_directory(path, parents=parents)

    def remove(self, path: str, recursive: bool = False) -> None:
        argv = ["rm", "-rf", path] if recursive else ["rm", "-f", path]
        self._run_filesystem_command(argv)

    def _run_filesystem_command(self, argv: Sequence[str]) -> None:
        proc = self._sandbox.exec(argv, _track_entry=False)
        rc = proc.wait()
        if rc != 0:
            stderr = proc.stderr.read().decode("utf-8", "replace").strip()
            detail = stderr or f"filesystem command exited with {rc}"
            raise DaemonError(detail, code="failed")


class RemoteProcess:
    """Process-like wrapper around a transport streaming ``exec`` request."""

    def __init__(
        self,
        transport: Transport,
        sandbox_id: str,
        cmd: Sequence[str],
        *,
        workdir: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        tty: bool = False,
    ) -> None:
        from .agent_client import ByteStream

        self._transport = transport
        self._sandbox_id = sandbox_id
        self._cmd = list(cmd)
        self._workdir = workdir
        self._env = dict(env or {})
        self._timeout = timeout
        self._tty = bool(tty)
        self._stdin_pipe = _InputPipe()
        self.stdin = _RemoteProcessStdin(self._stdin_pipe)
        self.stdout = ByteStream()
        self.stderr = ByteStream()
        self._done = threading.Event()
        self._returncode: int | None = None
        self._error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, name="vmon-remote-exec", daemon=True)
        self._thread.start()

    @property
    def returncode(self) -> int | None:
        if not self._done.is_set():
            return None
        return self._returncode

    def write_stdin(self, data: bytes | bytearray | memoryview | str) -> None:
        self.stdin.write(data)

    def close_stdin(self) -> None:
        self.stdin.close()

    def kill(self, signal: int = 15) -> None:
        del signal
        raise DaemonError("killing an in-flight remote exec is unsupported", code="unsupported")

    def resize(self, rows: int, cols: int) -> None:
        del rows, cols
        raise DaemonError("resizing an in-flight remote exec is unsupported", code="unsupported")

    def wait(self, timeout: float | None = None) -> int:
        if not self._done.wait(timeout):
            raise TimeoutError("remote exec did not finish before timeout")
        self.close_stdin()
        if self._error is not None:
            raise self._error
        return int(self._returncode if self._returncode is not None else -1)

    def _run(self) -> None:
        def on_event(stream: str, data: bytes) -> None:
            if stream == "stderr":
                self.stderr.feed(data)
            else:
                self.stdout.feed(data)

        try:
            params = {
                "name": self._sandbox_id,
                "cmd": self._cmd,
                "workdir": self._workdir,
                "env": self._env or None,
                "timeout": self._timeout,
            }
            clean_params = {k: v for k, v in params.items() if v is not None}
            if self._tty:
                interactive = cast(Any, self._transport.interactive)
                result = interactive(
                    "exec",
                    on_event,
                    tty=True,
                    **clean_params,
                )
            else:
                result = self._transport.stream(
                    "exec",
                    on_event,
                    stdin=cast(BinaryIO, self._stdin_pipe),
                    **clean_params,
                )
            rc = result.get("returncode")
            self._returncode = int(rc) if rc is not None else 0
        except BaseException as exc:
            self._error = exc
            self._returncode = -1
        finally:
            self.stdout.close()
            self.stderr.close()
            self._done.set()


class _InputPipe:
    def __init__(self) -> None:
        self._q: queue.Queue[bytes | object] = queue.Queue()
        self._buf = bytearray()
        self._closed = False

    def write(self, data: bytes | bytearray | memoryview | str) -> None:
        if self._closed:
            raise ValueError("stdin is closed")
        if isinstance(data, str):
            data = data.encode()
        chunk = bytes(data)
        if chunk:
            self._q.put(chunk)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._q.put(_STDIN_EOF)

    def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b""
        if size is not None and size > 0:
            while len(self._buf) < size:
                item = self._q.get()
                if item is _STDIN_EOF:
                    break
                self._buf.extend(item)  # type: ignore[arg-type]
            out = bytes(self._buf[:size])
            del self._buf[:size]
            return out
        chunks: list[bytes] = []
        if self._buf:
            chunks.append(bytes(self._buf))
            self._buf.clear()
        while True:
            item = self._q.get()
            if item is _STDIN_EOF:
                break
            chunks.append(item)  # type: ignore[arg-type]
        return b"".join(chunks)


class _RemoteProcessStdin:
    def __init__(self, pipe: _InputPipe) -> None:
        self._pipe = pipe
        self._closed = False

    def write(self, data: bytes | bytearray | memoryview | str) -> None:
        if self._closed:
            raise ValueError("stdin is closed")
        self._pipe.write(data)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._pipe.close()


def _reject_local_only(
    *,
    dockerfile: str | None,
    fs_dir: str | None,
    volumes: Mapping[str, Any] | None,
) -> None:
    if dockerfile is not None:
        raise DaemonError(
            "dockerfile builds are local-only in the SDK remote facade", code="unsupported"
        )
    if fs_dir is not None:
        raise DaemonError(
            "fs_dir host mounts are local-only in the SDK remote facade", code="unsupported"
        )
    for mount, spec in (volumes or {}).items():
        value = spec[0] if isinstance(spec, tuple) and spec else spec
        if not isinstance(value, (str, Mapping)):
            raise DaemonError(
                f"volume {mount!r} uses a local host Volume object; pass a gateway volume name",
                code="unsupported",
            )
        if isinstance(value, Mapping) and _HOST_VOLUME_KEYS.intersection(value):
            raise DaemonError(
                f"volume {mount!r} contains a host path; host-path volumes are local-only",
                code="unsupported",
            )


def _serialize_secrets(
    values: Iterable[Secret | Mapping[str, object]] | None,
) -> tuple[list[dict[str, str]] | None, dict[str, str]]:
    payload: list[dict[str, str]] = []
    merged: dict[str, str] = {}
    for item in values or ():
        if isinstance(item, Secret):
            data = item.as_env()
        elif isinstance(item, Mapping):
            data = Secret.from_dict(item).as_env()
        else:
            raise TypeError(f"expected Secret or mapping, got {type(item).__name__}")
        payload.append(data)
        merged.update(data)
    return (payload or None), merged


def _serialize_volumes(values: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not values:
        return None
    payload: dict[str, Any] = {}
    for mount, spec in values.items():
        read_only: bool | None = None
        value = spec
        if isinstance(spec, tuple):
            value = spec[0]
            if len(spec) > 1:
                read_only = bool(spec[1])
        if isinstance(value, str):
            payload[str(mount)] = {"name": value, "read_only": read_only} if read_only else value
        elif isinstance(value, Mapping):
            item = {str(k): v for k, v in value.items()}
            if read_only is not None:
                item["read_only"] = read_only
            payload[str(mount)] = item
    return payload


def _string_mapping(values: Mapping[Any, Any] | None | object) -> dict[str, str]:
    if not isinstance(values, Mapping):
        return {}
    return {str(key): str(value) for key, value in values.items()}


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _new_idempotency_key() -> str:
    return secrets.token_urlsafe(18)


