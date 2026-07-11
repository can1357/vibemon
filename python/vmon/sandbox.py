"""Thin Python SDK facade for the Rust v1 vmon API."""

from __future__ import annotations

import ast
import asyncio
import base64
import builtins
import contextlib
import functools
import inspect
import json
import queue
import socket
import symtable
import sys
import textwrap
import threading
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, overload

import httpx

from ._transport import (
    DaemonError,
    ResponseStream,
    VmonTransport,
    WebSocketConnection,
    get_transport,
    path_segment,
    split_create_context,
)
from .secret import Secret, merge_secrets
from .volume import Volume

_EOF = object()


@dataclass(frozen=True, slots=True)
class ExecResult:
    """Captured output and exit status from a non-streaming sandbox command."""

    returncode: int
    stdout: bytes
    stderr: bytes


class ByteStream(Iterable[bytes]):
    """Thread-safe byte iterator used for stdout/stderr streams."""

    def __init__(self) -> None:
        self._q: queue.Queue[bytes | object] = queue.Queue()
        self._buf = bytearray()
        self._closed = False
        self._eof = False

    def feed(self, data: bytes) -> None:
        if data:
            self._q.put(bytes(data))

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._q.put(_EOF)

    def __iter__(self) -> ByteStream:
        return self

    def __next__(self) -> bytes:
        if self._buf:
            data = bytes(self._buf)
            self._buf.clear()
            return data
        if self._eof:
            raise StopIteration
        item = self._q.get()
        if item is _EOF:
            self._eof = True
            raise StopIteration
        return item  # type: ignore[return-value]

    def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b""
        if size is not None and size > 0:
            while len(self._buf) < size and not self._eof:
                item = self._q.get()
                if item is _EOF:
                    self._eof = True
                    break
                self._buf.extend(item)  # type: ignore[arg-type]
            out = bytes(self._buf[:size])
            del self._buf[:size]
            return out

        chunks: list[bytes] = []
        if self._buf:
            chunks.append(bytes(self._buf))
            self._buf.clear()
        while not self._eof:
            item = self._q.get()
            if item is _EOF:
                self._eof = True
                break
            chunks.append(item)  # type: ignore[arg-type]
        return b"".join(chunks)


class _ExecSession:
    """A live v1 exec or shell WebSocket session."""

    def __init__(
        self,
        transport: VmonTransport,
        target: str,
        request: dict[str, Any],
        *,
        endpoint: str | None = None,
        timeout: float | None = None,
        tty: bool = False,
        close_transport: bool = False,
    ) -> None:
        self.stdout = ByteStream()
        self.stderr = ByteStream()
        self._transport = transport
        self._close_transport = close_transport
        self._timeout = timeout
        self._tty = tty
        self._exit = threading.Event()
        self._returncode: int | None = None
        self._signal: int | None = None
        self._ready_name: str | None = None
        self._error: BaseException | None = None
        self._send_lock = threading.Lock()
        self._stdin_closed = False
        self._closing = False
        path = endpoint or f"/v1/sandboxes/{path_segment(target)}/exec"
        websocket: WebSocketConnection | None = None
        try:
            websocket = transport.websocket(path)
            websocket.send_json(request)
        except BaseException:
            if websocket is not None:
                websocket.close()
            if close_transport:
                transport.close()
            raise
        self._ws = websocket
        self._reader = threading.Thread(
            target=self._read_loop, name=f"vmon-stream-{target}", daemon=True
        )
        self._reader.start()

    @property
    def returncode(self) -> int | None:
        return self._returncode

    @property
    def signal(self) -> int | None:
        return self._signal

    @property
    def ready_name(self) -> str | None:
        return self._ready_name

    def write_stdin(self, data: bytes | bytearray | memoryview | str) -> None:
        raw = data.encode() if isinstance(data, str) else bytes(data)
        if not raw:
            return
        with self._send_lock:
            if self._stdin_closed:
                raise DaemonError("exec stdin is closed", code="closed")
            self._ws.send_json({"stdin_b64": base64.b64encode(raw).decode("ascii")})

    def close_stdin(self) -> None:
        with self._send_lock:
            if self._stdin_closed:
                return
            self._stdin_closed = True
            self._ws.send_json({"eof": True})

    def resize(self, rows: int, cols: int) -> None:
        self._ws.send_json({"resize": [int(rows), int(cols)]})

    def kill(self, signal: int = 15) -> None:
        self._closing = True
        self._signal = int(signal)
        self._ws.close()

    def wait(self, timeout: float | None = None) -> int:
        if not self._tty and not self._stdin_closed:
            with contextlib.suppress(Exception):
                self.close_stdin()
        effective = self._timeout if timeout is None else timeout
        if not self._exit.wait(effective):
            self.kill()
            raise TimeoutError("exec timed out")
        if self._error is not None:
            raise self._error
        return int(self._returncode if self._returncode is not None else -1)

    def _read_loop(self) -> None:
        try:
            while True:
                frame = self._ws.recv_json()
                if frame is None:
                    if self._returncode is None:
                        self._returncode = -1
                    return
                error = frame.get("error")
                if isinstance(error, Mapping):
                    code = str(error.get("code") or "internal")
                    message = str(error.get("message") or "exec failed")
                    self._error = DaemonError(message, code=code)
                    self._returncode = -1
                    return
                ready = frame.get("ready")
                if isinstance(ready, str):
                    self._ready_name = ready
                    continue
                stream = frame.get("stream")
                if stream in {"stdout", "stderr", "console"}:
                    encoded = frame.get("b64")
                    if not isinstance(encoded, str):
                        self._error = DaemonError("invalid exec stream frame", code="protocol")
                        self._returncode = -1
                        return
                    try:
                        data = base64.b64decode(encoded, validate=True)
                    except ValueError, TypeError:
                        self._error = DaemonError("invalid exec stream payload", code="protocol")
                        self._returncode = -1
                        return
                    (self.stderr if stream == "stderr" else self.stdout).feed(data)
                    continue
                if "exit" in frame:
                    raw_exit = frame.get("exit")
                    try:
                        self._returncode = int(raw_exit) if raw_exit is not None else -1
                    except TypeError, ValueError:
                        self._returncode = -1
                    raw_signal = frame.get("signal")
                    try:
                        self._signal = int(raw_signal) if raw_signal is not None else None
                    except TypeError, ValueError:
                        self._signal = None
                    return
        except BaseException as exc:
            if not self._closing:
                self._error = exc
            self._returncode = -1
        finally:
            self.stdout.close()
            self.stderr.close()
            self._exit.set()
            self._ws.close()
            if self._close_transport:
                self._transport.close()


class _ProcessStdin:
    def __init__(self, session: _ExecSession) -> None:
        self._session = session
        self._closed = False

    def write(self, data: bytes | bytearray | memoryview | str) -> None:
        self._session.write_stdin(data)

    def close(self) -> None:
        if not self._closed:
            try:
                self._session.close_stdin()
            finally:
                self._closed = True


class Process:
    """A streaming process running inside a sandbox."""

    def __init__(self, session: _ExecSession) -> None:
        self._session = session
        self.stdout = session.stdout
        self.stderr = session.stderr
        self.stdin = _ProcessStdin(session)

    @property
    def returncode(self) -> int | None:
        """Return the exit status once the process has completed."""
        return self._session.returncode

    @property
    def sandbox_name(self) -> str | None:
        """Return the shell-created sandbox name once announced by the server."""
        return self._session.ready_name

    def write_stdin(self, data: bytes | bytearray | memoryview | str) -> None:
        """Write bytes or text to the process standard input."""
        self.stdin.write(data)

    def close_stdin(self) -> None:
        """Close process standard input."""
        self.stdin.close()

    def kill(self, signal: int = 15) -> None:
        """Disconnect the stream, causing the daemon to terminate the process."""
        self._session.kill(signal)

    def resize(self, rows: int, cols: int) -> None:
        """Resize the process pseudo-terminal."""
        self._session.resize(rows, cols)

    def wait(self, timeout: float | None = None) -> int:
        """Wait for completion and return the process exit status."""
        try:
            return self._session.wait(timeout)
        finally:
            self._close_streams()

    def close(self) -> None:
        """Close this process stream, terminating a still-running command."""
        if self.returncode is None:
            self.kill()
        self._close_streams()

    def _close_streams(self) -> None:
        with contextlib.suppress(Exception):
            self.stdout.close()
        with contextlib.suppress(Exception):
            self.stderr.close()

    def __enter__(self) -> Process:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class ConsoleStream(Iterable[bytes]):
    """Closeable byte stream from a sandbox console attachment."""

    def __init__(self, websocket: WebSocketConnection) -> None:
        self._ws = websocket
        self._stream = ByteStream()
        self._done = threading.Event()
        self._error: BaseException | None = None
        self._closing = False
        self._reader = threading.Thread(
            target=self._read_loop, name="vmon-console-attach", daemon=True
        )
        self._reader.start()

    def __iter__(self) -> ConsoleStream:
        return self

    def __next__(self) -> bytes:
        try:
            return next(self._stream)
        except StopIteration:
            self.wait()
            raise

    def read(self, size: int = -1) -> bytes:
        """Read console bytes, blocking until enough data or stream completion."""
        data = self._stream.read(size)
        if size is None or size < 0:
            self.wait()
        return data

    def wait(self, timeout: float | None = None) -> None:
        """Wait for attachment completion and raise any structured stream error."""
        if not self._done.wait(timeout):
            raise TimeoutError("console attachment timed out")
        if self._error is not None:
            raise self._error

    def close(self) -> None:
        """Close the console attachment idempotently."""
        self._closing = True
        self._ws.close()
        self._stream.close()

    def _read_loop(self) -> None:
        try:
            while True:
                frame = self._ws.recv_json()
                if frame is None:
                    return
                error = frame.get("error")
                if isinstance(error, Mapping):
                    self._error = DaemonError(
                        str(error.get("message") or "console attachment failed"),
                        code=str(error.get("code") or "internal"),
                    )
                    return
                if frame.get("stream") != "console":
                    continue
                encoded = frame.get("b64")
                if not isinstance(encoded, str):
                    self._error = DaemonError("invalid console stream frame", code="protocol")
                    return
                try:
                    self._stream.feed(base64.b64decode(encoded, validate=True))
                except ValueError, TypeError:
                    self._error = DaemonError("invalid console stream payload", code="protocol")
                    return
        except BaseException as exc:
            if not self._closing:
                self._error = exc
        finally:
            self._stream.close()
            self._done.set()
            self._ws.close()

    def __enter__(self) -> ConsoleStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class EventStream(Iterable[dict[str, Any]]):
    """Closeable iterator over JSON objects from an SSE endpoint."""

    def __init__(
        self, response: ResponseStream, *, owned_transport: VmonTransport | None = None
    ) -> None:
        self._response = response
        self._owned_transport = owned_transport
        self._closed = False

    def __iter__(self) -> Iterator[dict[str, Any]]:
        data_lines: list[str] = []
        try:
            for line in self._response.iter_lines():
                if line == "":
                    if data_lines:
                        yield self._decode("\n".join(data_lines))
                        data_lines.clear()
                    continue
                if line.startswith("data:"):
                    data_lines.append(line[5:].lstrip(" "))
            if data_lines:
                yield self._decode("\n".join(data_lines))
        finally:
            self.close()

    @staticmethod
    def _decode(payload: str) -> dict[str, Any]:
        try:
            value = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise DaemonError("vmon event stream returned invalid JSON", code="protocol") from exc
        if not isinstance(value, dict):
            raise DaemonError("vmon event stream returned a non-object event", code="protocol")
        return value

    def close(self) -> None:
        """Close the event response and any transport owned by the stream."""
        if self._closed:
            return
        self._closed = True
        try:
            self._response.close()
        finally:
            if self._owned_transport is not None:
                self._owned_transport.close()

    def __enter__(self) -> EventStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class LogStream(Iterable[str]):
    """Closeable iterator over decoded sandbox console log chunks."""

    def __init__(self, events: EventStream) -> None:
        self._events = events

    def __iter__(self) -> Iterator[str]:
        for event in self._events:
            data = event.get("data")
            if isinstance(data, str):
                yield data
                continue
            encoded = event.get("b64")
            if isinstance(encoded, str):
                try:
                    yield base64.b64decode(encoded, validate=True).decode("utf-8", errors="replace")
                except ValueError, TypeError:
                    raise DaemonError(
                        "invalid sandbox log stream payload", code="protocol"
                    ) from None

    def close(self) -> None:
        """Close the underlying event stream."""
        self._events.close()

    def __enter__(self) -> LogStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class Filesystem:
    """Filesystem RPC facade for a Sandbox."""

    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox

    def read_bytes(self, path: str) -> bytes:
        return self._sandbox._transport.bytes(
            "GET", f"/v1/sandboxes/{path_segment(self._sandbox.name)}/files", params={"path": path}
        )

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(path).decode(encoding)

    def write_bytes(
        self, path: str, data: bytes | bytearray | memoryview, mode: int = 0o644
    ) -> None:
        del mode
        self._sandbox._transport.json(
            "PUT",
            f"/v1/sandboxes/{path_segment(self._sandbox.name)}/files",
            params={"path": path},
            content=bytes(data),
            headers={"Content-Type": "application/octet-stream"},
        )

    def write_text(self, path: str, text: str, encoding: str = "utf-8", mode: int = 0o644) -> None:
        self.write_bytes(path, text.encode(encoding), mode=mode)

    def list_files(self, path: str = ".") -> list[dict[str, Any]]:
        value = self._sandbox._transport.json(
            "GET",
            f"/v1/sandboxes/{path_segment(self._sandbox.name)}/files/list",
            params={"path": path},
        )
        if isinstance(value, dict):
            entries = value.get("entries")
            if isinstance(entries, list):
                return [entry for entry in entries if isinstance(entry, dict)]
        return value if isinstance(value, list) else []

    def make_directory(self, path: str, parents: bool = True) -> None:
        cmd = ["mkdir"]
        if parents:
            cmd.append("-p")
        cmd.append(path)
        proc = self._sandbox.exec(cmd, _track_entry=False)
        rc = proc.wait(timeout=30)
        if rc != 0:
            raise DaemonError(
                proc.stderr.read().decode("utf-8", "replace") or "mkdir failed", code="failed"
            )

    def remove(self, path: str, recursive: bool = False) -> None:
        self._sandbox._transport.json(
            "DELETE",
            f"/v1/sandboxes/{path_segment(self._sandbox.name)}/files",
            params={"path": path, "recursive": bool(recursive)},
        )

    def stat(self, path: str) -> dict[str, Any]:
        value = self._sandbox._transport.json(
            "GET",
            f"/v1/sandboxes/{path_segment(self._sandbox.name)}/files/stat",
            params={"path": path},
        )
        return dict(value) if isinstance(value, dict) else {}


def health(*, context: str | None = None) -> dict[str, Any]:
    """Return the selected daemon's ``/healthz`` response."""
    with get_transport(context) as transport:
        value = transport.json("GET", "/healthz")
    if not isinstance(value, Mapping) or not isinstance(value.get("ok"), bool):
        raise DaemonError("health check returned a malformed response", code="protocol")
    return dict(value)


def server_info(*, context: str | None = None) -> dict[str, Any]:
    """Return server capabilities and version information."""
    with get_transport(context) as transport:
        value = transport.json("GET", "/v1/info")
    if not isinstance(value, Mapping):
        raise DaemonError("server info returned a non-object response", code="protocol")
    return dict(value)


def daemon_metrics(*, context: str | None = None) -> str:
    """Return the daemon's Prometheus metrics document."""
    with get_transport(context) as transport:
        return transport.text("GET", "/metrics")


def openapi_schema(*, context: str | None = None) -> dict[str, Any]:
    """Return the OpenAPI document advertised by the selected daemon."""
    with get_transport(context) as transport:
        value = transport.json("GET", "/v1/openapi.json")
    if not isinstance(value, Mapping):
        raise DaemonError("OpenAPI endpoint returned a non-object response", code="protocol")
    return dict(value)


def events(*, context: str | None = None) -> EventStream:
    """Open the daemon's JSON server-sent event stream."""
    transport = get_transport(context)
    try:
        response = transport.stream("GET", "/v1/events")
    except BaseException:
        transport.close()
        raise
    return EventStream(response, owned_transport=transport)


def list_snapshots(*, context: str | None = None) -> list[str]:
    """Return snapshot names known to the selected daemon."""
    with get_transport(context) as transport:
        value = transport.json("GET", "/v1/snapshots")
    snapshots = value.get("snapshots") if isinstance(value, Mapping) else None
    if not isinstance(snapshots, list) or not all(isinstance(name, str) for name in snapshots):
        raise DaemonError("snapshot list returned a malformed response", code="protocol")
    return list(snapshots)


def shell(
    *cmd: str | Iterable[str],
    ref: str | None = None,
    image: str | None = None,
    context: str | None = None,
    cpus: int = 1,
    memory: int = 512,
    disk_mb: int = 1024,
    timeout: float | None = 300,
) -> Process:
    """Start a streaming interactive shell, cleaning up ephemeral sandboxes on close."""
    argv = _coerce_cmd(cmd) if cmd else ["/bin/sh"]
    body = _drop_none(
        {
            "ref": ref,
            "image": image,
            "cmd": argv,
            "cpus": int(cpus),
            "mem": int(memory),
            "disk_mb": int(disk_mb),
            "timeout": timeout,
            "tty": True,
        }
    )
    transport = get_transport(context)
    return Process(
        _ExecSession(
            transport,
            "shell",
            body,
            endpoint="/v1/shell",
            timeout=timeout,
            tty=True,
            close_transport=True,
        )
    )


class WarmPoolHandle:
    """Closeable client-side handle for a server-owned warm pool."""

    def __init__(
        self,
        reference: str,
        size: int,
        transport: VmonTransport,
        stats: Mapping[str, Any] | None = None,
    ) -> None:
        self.reference = reference
        self.size = int(size)
        self._transport = transport
        self._stats = dict(stats or {})
        self._closed = False

    def stats(self) -> dict[str, Any]:
        """Refresh and return this pool's server statistics."""
        if self._closed:
            return dict(self._stats)
        pools = self._transport.json("GET", "/v1/pools")
        if isinstance(pools, dict):
            value = pools.get(self.reference)
            if isinstance(value, dict):
                self._stats = dict(value)
        return dict(self._stats)

    def shutdown(self) -> None:
        """Delete this pool and close its transport."""
        if self._closed:
            return
        try:
            self._transport.json("DELETE", f"/v1/pools/{path_segment(self.reference)}")
        finally:
            self._closed = True
            self._transport.close()

    close = shutdown

    def __enter__(self) -> WarmPoolHandle:
        return self

    def __exit__(self, *exc: object) -> None:
        self.shutdown()


def pool_inventory(*, context: str | None = None) -> dict[str, int]:
    """Return ready warm-pool counts keyed by portable image/template reference."""
    with get_transport(context) as transport:
        pools = transport.json("GET", "/v1/pools")
    if not isinstance(pools, dict):
        return {}
    out: dict[str, int] = {}
    for ref, stats in pools.items():
        if isinstance(ref, str) and isinstance(stats, Mapping):
            ready = stats.get("ready", stats.get("ready_count", 0))
            with contextlib.suppress(TypeError, ValueError):
                out[ref] = int(ready)
    return out


def prewarm(ref: str, *, count: int, **template_kwargs: Any) -> WarmPoolHandle:
    """Set a server-owned warm pool size for ``ref``."""
    count = int(count)
    if count < 0:
        raise ValueError("pool size must be >= 0")
    transport = get_transport()
    body = {"size": count, **{k: v for k, v in template_kwargs.items() if v is not None}}
    try:
        stats = transport.json("PUT", f"/v1/pools/{path_segment(ref)}", json_body=body)
    except BaseException:
        transport.close()
        raise
    return WarmPoolHandle(ref, count, transport, stats if isinstance(stats, Mapping) else None)


def shutdown_prewarms(pools: Iterable[WarmPoolHandle]) -> None:
    """Delete each supplied warm pool."""
    for pool in pools:
        pool.shutdown()


def shutdown_all_pools(*, context: str | None = None) -> None:
    """Delete every warm pool in the selected API context."""
    with get_transport(context) as transport:
        pools = transport.json("GET", "/v1/pools")
        if isinstance(pools, dict):
            for ref in list(pools):
                transport.json("DELETE", f"/v1/pools/{path_segment(str(ref))}")


def _coerce_cmd(cmd: tuple[str | Iterable[str], ...]) -> list[str]:
    if len(cmd) == 1 and not isinstance(cmd[0], str):
        return [str(part) for part in cmd[0]]
    return [str(part) for part in cmd]


def _drop_none(data: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in data.items() if value is not None}


def _path_tail(value: str) -> str:
    return "/".join(path_segment(part) for part in value.lstrip("/").split("/"))


def _secret_wire(
    secrets: Iterable[Secret | Mapping[str, object]] | None,
) -> list[dict[str, Any]] | None:
    if secrets is None:
        return None
    out: list[dict[str, Any]] = []
    for item in secrets:
        if isinstance(item, Secret):
            out.append(item.to_wire())
        elif isinstance(item, Mapping):
            out.append(Secret.from_dict(item).to_wire())
        else:
            raise TypeError(f"expected Secret or mapping, got {type(item).__name__}")
    return out


def _volume_name(value: Any) -> str:
    if isinstance(value, Volume):
        return value.name
    if isinstance(value, str):
        return Volume(value).name
    raise TypeError("volume spec must be a Volume, name string, tuple, or mapping")


def _volume_wire(volumes: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if volumes is None:
        return None
    out: dict[str, Any] = {}
    for mount, value in volumes.items():
        if isinstance(value, tuple):
            if len(value) != 2:
                raise TypeError("volume tuple must be (Volume, read_only)")
            name = _volume_name(value[0])
            read_only = bool(value[1])
        elif isinstance(value, Mapping):
            name_value = value.get("name")
            if not isinstance(name_value, str):
                raise TypeError("volume mapping requires a string 'name'")
            name = Volume(name_value).name
            read_only = bool(value.get("read_only", value.get("ro", False)))
        else:
            name = _volume_name(value)
            read_only = False
        out[str(mount)] = {"name": name, "read_only": True} if read_only else name
    return out


def _clone_create_extra(kwargs: Mapping[str, Any]) -> dict[str, Any]:
    allowed = {
        "image",
        "template",
        "dockerfile",
        "context",
        "cpus",
        "memory",
        "disk_mb",
        "timeout",
        "timeout_secs",
        "workdir",
        "env",
        "secrets",
        "volumes",
        "tags",
        "fs_dir",
        "block_network",
        "ports",
        "egress_allow",
        "egress_allow_domains",
        "inbound_cidr_allowlist",
        "readiness_probe",
        "pool_size",
        "ha",
        "arch",
        "idempotency_key",
        "command",
    }
    unknown = sorted(set(kwargs) - allowed)
    if unknown:
        names = ", ".join(unknown)
        raise TypeError(f"unsupported sandbox option(s): {names}")
    return {key: value for key, value in kwargs.items() if value is not None}


class Sandbox:
    """A v1 API-backed microVM sandbox."""

    def __init__(
        self,
        name: str | Mapping[str, Any],
        *,
        transport: VmonTransport | None = None,
        view: Mapping[str, Any] | None = None,
        workdir: str | None = None,
        env: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        secrets: Iterable[Secret | Mapping[str, object]] | None = None,
        _owns_transport: bool = False,
    ) -> None:
        if isinstance(name, Mapping):
            view = name
            resolved = str(view.get("name") or view.get("id") or "")
        else:
            resolved = str(name)
        if not resolved:
            raise ValueError("sandbox name is required")
        self.name = resolved
        self.sandbox_id = resolved
        if transport is None:
            self._transport = get_transport()
            self._owns_transport = True
        else:
            self._transport = transport
            self._owns_transport = _owns_transport
        self._transport_closed = False
        self._view: dict[str, Any] = dict(view or {"id": resolved, "name": resolved})
        self.workdir = workdir or self._string_detail("workdir")
        self.env: dict[str, str] = {str(k): str(v) for k, v in (env or {}).items()}
        self.tags: dict[str, str] = {
            str(k): str(v) for k, v in (tags or self._view.get("tags") or {}).items()
        }
        self.filesystem = Filesystem(self)
        self._secret_env = merge_secrets(secrets)
        self.connect_token: str | None = None
        self._terminated = False

    @classmethod
    def create(
        cls,
        *,
        image: str | None = None,
        template: str | None = None,
        dockerfile: str | None = None,
        context: str | None = None,
        name: str | None = None,
        cpus: int = 1,
        memory: int = 512,
        disk_mb: int = 1024,
        timeout: float | None = 300,
        timeout_secs: int | None = None,
        workdir: str | None = None,
        env: dict[str, str] | None = None,
        secrets: Iterable[Secret | Mapping[str, object]] | None = None,
        volumes: Mapping[str, Any] | None = None,
        tags: dict[str, str] | None = None,
        fs_dir: str | None = None,
        block_network: bool = False,
        ports: Sequence[int] | None = None,
        egress_allow: Sequence[str] | None = None,
        egress_allow_domains: Sequence[str] | None = None,
        inbound_cidr_allowlist: Sequence[str] | None = None,
        readiness_probe: Any = None,
        pool_size: int = 0,
        ha: str | None = None,
        arch: str | None = None,
        idempotency_key: str | None = None,
        command: Sequence[str] | None = None,
    ) -> Sandbox:
        """Create a sandbox using the stable ``SandboxCreate`` request fields."""
        transport_context, api_context = split_create_context(context)
        body = _drop_none(
            {
                "image": image,
                "template": template,
                "dockerfile": dockerfile,
                "context": api_context,
                "name": name,
                "cpus": int(cpus),
                "memory": int(memory),
                "disk_mb": int(disk_mb),
                "timeout": timeout,
                "timeout_secs": timeout_secs,
                "workdir": workdir,
                "env": {str(k): str(v) for k, v in (env or {}).items()}
                if env is not None
                else None,
                "secrets": _secret_wire(secrets),
                "volumes": _volume_wire(volumes),
                "tags": {str(k): str(v) for k, v in (tags or {}).items()}
                if tags is not None
                else None,
                "fs_dir": fs_dir,
                "block_network": bool(block_network),
                "ports": [int(port) for port in ports] if ports is not None else None,
                "egress_allow": list(egress_allow) if egress_allow is not None else None,
                "egress_allow_domains": list(egress_allow_domains)
                if egress_allow_domains is not None
                else None,
                "inbound_cidr_allowlist": list(inbound_cidr_allowlist)
                if inbound_cidr_allowlist is not None
                else None,
                "readiness_probe": readiness_probe,
                "pool_size": int(pool_size),
                "ha": ha,
                "arch": arch,
                "idempotency_key": idempotency_key,
                "command": [str(part) for part in command] if command is not None else None,
            }
        )
        transport = get_transport(transport_context)
        try:
            view = transport.json("POST", "/v1/sandboxes", json_body=body)
            if not isinstance(view, Mapping):
                raise DaemonError("create returned a non-object response", code="protocol")
        except BaseException:
            transport.close()
            raise
        try:
            return cls(
                view,
                transport=transport,
                workdir=workdir,
                env=env,
                tags=tags,
                secrets=secrets,
                _owns_transport=True,
            )
        except BaseException:
            transport.close()
            raise

    @classmethod
    def from_snapshot(
        cls,
        snapshot: str,
        *,
        name: str | None = None,
        sandbox_name: str | None = None,
        agent: bool = True,
        fork: bool = False,
        count: int = 1,
        context: str | None = None,
        **kwargs: Any,
    ) -> Sandbox:
        """Restore one sandbox from a snapshot, optionally using copy-on-write fork."""
        if fork:
            if int(count) != 1:
                raise ValueError("from_snapshot(fork=True) creates one clone; use Sandbox.fork")
            return cls.fork(snapshot, count=1, context=context, **kwargs)[0]

        transport_context, api_context = split_create_context(context)
        extra = _clone_create_extra(kwargs)
        if api_context is not None:
            extra["context"] = api_context
        if "volumes" in extra:
            extra["volumes"] = _volume_wire(extra["volumes"])
        if "secrets" in extra:
            extra["secrets"] = _secret_wire(extra["secrets"])
        body = {"name": sandbox_name or name, "agent": bool(agent), **extra}
        transport = get_transport(transport_context)
        try:
            view = transport.json(
                "POST",
                f"/v1/snapshots/{path_segment(snapshot)}/restore",
                json_body=_drop_none(body),
            )
            if not isinstance(view, Mapping):
                raise DaemonError("restore returned a non-object response", code="protocol")
        except BaseException:
            transport.close()
            raise
        try:
            return cls(
                view,
                transport=transport,
                workdir=kwargs.get("workdir"),
                env=kwargs.get("env"),
                tags=kwargs.get("tags"),
                secrets=kwargs.get("secrets"),
                _owns_transport=True,
            )
        except BaseException:
            transport.close()
            raise

    @classmethod
    def fork(
        cls,
        snapshot: str,
        *,
        count: int = 1,
        context: str | None = None,
        **kwargs: Any,
    ) -> list[Sandbox]:
        """Create and return all copy-on-write clones from a snapshot."""
        count = int(count)
        if count < 1:
            raise ValueError("fork count must be >= 1")
        transport_context, api_context = split_create_context(context)
        extra = _clone_create_extra(kwargs)
        if api_context is not None:
            extra["context"] = api_context
        if "volumes" in extra:
            extra["volumes"] = _volume_wire(extra["volumes"])
        if "secrets" in extra:
            extra["secrets"] = _secret_wire(extra["secrets"])
        transport = get_transport(transport_context)
        clones: list[Sandbox] = []
        try:
            value = transport.json(
                "POST",
                f"/v1/snapshots/{path_segment(snapshot)}/fork",
                json_body={"count": count, **extra},
            )
            rows = value.get("clones") if isinstance(value, Mapping) else None
            if not isinstance(rows, list) or len(rows) != count:
                raise DaemonError("fork returned an unexpected clone count", code="protocol")
            for row in rows:
                if not isinstance(row, Mapping) or not isinstance(row.get("name"), str):
                    raise DaemonError("fork returned a malformed clone", code="protocol")
                clones.append(
                    cls(
                        row,
                        transport=transport.clone(),
                        workdir=kwargs.get("workdir"),
                        env=kwargs.get("env"),
                        tags=kwargs.get("tags"),
                        secrets=kwargs.get("secrets"),
                        _owns_transport=True,
                    )
                )
        except BaseException:
            for clone in clones:
                with contextlib.suppress(Exception):
                    clone.terminate()
            raise
        finally:
            transport.close()
        return clones

    @classmethod
    def from_id(cls, sandbox_id: str, *, context: str | None = None) -> Sandbox:
        """Attach a user object to an existing sandbox id."""
        return cls.attach(sandbox_id, context=context)

    @classmethod
    def attach(
        cls,
        name: str,
        *,
        context: str | None = None,
        transport: VmonTransport | None = None,
    ) -> Sandbox:
        """Fetch an existing sandbox and return its user object."""
        if context is not None and transport is not None:
            raise ValueError("context and transport are mutually exclusive")
        owns_transport = transport is None
        resolved_transport = transport or get_transport(context)
        try:
            view = resolved_transport.json("GET", f"/v1/sandboxes/{path_segment(name)}")
            if not isinstance(view, Mapping):
                raise DaemonError("inspect returned a non-object response", code="protocol")
        except BaseException:
            if owns_transport:
                resolved_transport.close()
            raise
        try:
            return cls(
                view,
                transport=resolved_transport,
                _owns_transport=owns_transport,
            )
        except BaseException:
            if owns_transport:
                resolved_transport.close()
            raise

    @classmethod
    def list(
        cls,
        *,
        tag: Mapping[str, str] | None = None,
        context: str | None = None,
    ) -> list[Sandbox]:
        """List sandboxes, optionally filtering by exact tag values."""
        params = [("tag", f"{key}={value}") for key, value in sorted(tag.items())] if tag else None
        with get_transport(context) as transport:
            value = transport.json("GET", "/v1/sandboxes", params=params)
            rows = value.get("sandboxes") if isinstance(value, Mapping) else None
            if not isinstance(rows, list) or not all(
                isinstance(row, Mapping) and isinstance(row.get("name") or row.get("id"), str)
                for row in rows
            ):
                raise DaemonError("sandbox list returned a malformed response", code="protocol")
            sandboxes: list[Sandbox] = []
            try:
                for row in rows:
                    sandboxes.append(cls(row, transport=transport.clone(), _owns_transport=True))
            except BaseException:
                for sandbox in sandboxes:
                    sandbox.close()
                raise
            return sandboxes

    def __repr__(self) -> str:
        return f"Sandbox(name={self.name!r})"

    def __str__(self) -> str:
        return self.name

    @property
    def aio(self) -> _AsyncSandbox:
        return _AsyncSandbox(self)

    @property
    def view(self) -> dict[str, Any]:
        return dict(self._view)

    @property
    def returncode(self) -> int | None:
        value = self._view.get("returncode")
        return int(value) if isinstance(value, int) else None

    def _string_detail(self, key: str) -> str | None:
        value = self._view.get(key)
        return value if isinstance(value, str) else None

    def refresh(self) -> dict[str, Any]:
        value = self._transport.json("GET", f"/v1/sandboxes/{path_segment(self.name)}")
        if isinstance(value, Mapping):
            self._view = dict(value)
        return dict(self._view)

    def exists(self) -> bool:
        try:
            self.refresh()
            return True
        except DaemonError as exc:
            if exc.code == "not_found" or exc.status == 404:
                return False
            raise

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
        del _track_entry
        argv = _coerce_cmd(cmd)
        if not argv:
            raise ValueError("exec command must not be empty")
        merged_env = {**self.env, **self._secret_env}
        if env:
            merged_env.update({str(k): str(v) for k, v in env.items()})
        body = _drop_none(
            {
                "cmd": argv,
                "workdir": workdir or self.workdir,
                "env": merged_env or None,
                "timeout": timeout,
                "tty": bool(pty or tty),
            }
        )
        session = _ExecSession(
            self._transport,
            self.name,
            body,
            timeout=timeout,
            tty=bool(pty or tty),
        )
        return Process(session)

    def exec_capture(
        self,
        *cmd: str | Iterable[str],
        workdir: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        tty: bool = False,
    ) -> ExecResult:
        """Run a non-streaming command and return its captured output."""
        argv = _coerce_cmd(cmd)
        if not argv:
            raise ValueError("exec command must not be empty")
        merged_env = {**self.env, **self._secret_env}
        if env:
            merged_env.update({str(key): str(value) for key, value in env.items()})
        value = self._transport.json(
            "POST",
            f"/v1/sandboxes/{path_segment(self.name)}/exec",
            json_body=_drop_none(
                {
                    "cmd": argv,
                    "workdir": workdir or self.workdir,
                    "env": merged_env or None,
                    "timeout": timeout,
                    "tty": bool(tty),
                }
            ),
        )
        if not isinstance(value, Mapping):
            raise DaemonError("captured exec returned a non-object response", code="protocol")
        raw_exit = value.get("exit")
        stdout_b64 = value.get("stdout_b64")
        stderr_b64 = value.get("stderr_b64")
        if (
            not isinstance(raw_exit, int)
            or isinstance(raw_exit, bool)
            or not isinstance(stdout_b64, str)
            or not isinstance(stderr_b64, str)
        ):
            raise DaemonError("captured exec returned a malformed response", code="protocol")
        try:
            stdout = base64.b64decode(stdout_b64, validate=True)
            stderr = base64.b64decode(stderr_b64, validate=True)
        except (ValueError, TypeError) as exc:
            raise DaemonError("captured exec returned invalid base64", code="protocol") from exc
        return ExecResult(returncode=raw_exit, stdout=stdout, stderr=stderr)

    def attach_console(self) -> ConsoleStream:
        """Open a live, read-only stream of sandbox console output."""
        websocket = self._transport.websocket(f"/v1/sandboxes/{path_segment(self.name)}/attach")
        return ConsoleStream(websocket)

    def wait_until_ready(self, probe: Any = None, timeout: float = 300) -> None:
        """Wait until a tunnel port accepts TCP or a guest command succeeds."""
        if probe is None:
            return
        deadline = time.monotonic() + timeout
        last: Exception | None = None
        while time.monotonic() < deadline:
            try:
                if isinstance(probe, int) or (isinstance(probe, Mapping) and "port" in probe):
                    port = int(probe if isinstance(probe, int) else probe["port"])
                    target = self.tunnels().get(port)
                    if target is not None:
                        with socket.create_connection(target, timeout=min(1.0, timeout)):
                            return
                else:
                    proc = self.exec(
                        ["sh", "-lc", str(probe)],
                        timeout=min(5.0, timeout),
                        _track_entry=False,
                    )
                    if proc.wait(timeout=min(5.0, timeout)) == 0:
                        return
            except Exception as exc:
                last = exc
            time.sleep(0.1)
        raise TimeoutError(f"sandbox readiness probe timed out after {timeout:.0f}s") from last

    def poll(self) -> int | None:
        try:
            self.refresh()
        except DaemonError as exc:
            if exc.code == "not_found" or exc.status == 404:
                return self.returncode
            raise
        status = str(self._view.get("status") or "")
        if status in {"stopped", "terminated", "failed"}:
            return self.returncode if self.returncode is not None else 0
        return None

    def stop(self, wait: bool = True) -> None:
        """Stop the sandbox while retaining its server-side record."""
        del wait
        value = self._transport.json("POST", f"/v1/sandboxes/{path_segment(self.name)}/stop")
        if isinstance(value, Mapping):
            self._view = dict(value)

    def terminate(self, wait: bool = True) -> None:
        """Terminate the sandbox idempotently and release owned HTTP resources."""
        del wait
        if self._terminated:
            self.close()
            return
        value = self._transport.json("POST", f"/v1/sandboxes/{path_segment(self.name)}/terminate")
        if isinstance(value, Mapping):
            self._view = dict(value)
        self._terminated = True
        self.close()

    def remove(self) -> None:
        """Delete the sandbox record and release owned HTTP resources."""
        value = self._transport.json("DELETE", f"/v1/sandboxes/{path_segment(self.name)}")
        if isinstance(value, Mapping):
            self._view = dict(value)
        self._terminated = True
        self.close()

    def pause(self) -> dict[str, Any]:
        """Pause sandbox virtual CPUs and return the updated view."""
        value = self._transport.json("POST", f"/v1/sandboxes/{path_segment(self.name)}/pause")
        if isinstance(value, Mapping):
            self._view = dict(value)
        return dict(self._view)

    def resume(self) -> dict[str, Any]:
        """Resume a paused sandbox and return the updated view."""
        value = self._transport.json("POST", f"/v1/sandboxes/{path_segment(self.name)}/resume")
        if isinstance(value, Mapping):
            self._view = dict(value)
        return dict(self._view)

    def snapshot(self, name: str | None = None, *, stop: bool = False) -> str:
        """Create a full VM snapshot and return its server-assigned name."""
        value = self._transport.json(
            "POST",
            f"/v1/sandboxes/{path_segment(self.name)}/snapshots",
            json_body={"name": name, "stop": bool(stop)},
        )
        snapshot = value.get("snapshot") if isinstance(value, Mapping) else None
        if not isinstance(snapshot, str) or not snapshot:
            raise DaemonError("snapshot returned a malformed response", code="protocol")
        return snapshot

    def snapshot_filesystem(self, name: str | None = None) -> str:
        """Snapshot the guest filesystem and return its template image name."""
        value = self._transport.json(
            "POST",
            f"/v1/sandboxes/{path_segment(self.name)}/snapshots/fs",
            json_body={"name": name},
        )
        image = value.get("image") if isinstance(value, Mapping) else None
        if not isinstance(image, str) or not image:
            raise DaemonError("filesystem snapshot returned a malformed response", code="protocol")
        return image

    def extend(self, secs: int) -> dict[str, Any]:
        """Extend the sandbox deadline by a non-negative number of seconds."""
        secs = int(secs)
        if secs < 0:
            raise ValueError("extension seconds must be >= 0")
        value = self._transport.json(
            "POST", f"/v1/sandboxes/{path_segment(self.name)}/extend", json_body={"secs": secs}
        )
        if not isinstance(value, Mapping):
            raise DaemonError("extend returned a non-object response", code="protocol")
        return dict(value)

    def metrics(self) -> dict[str, Any]:
        """Return this sandbox's runtime metrics."""
        value = self._transport.json("GET", f"/v1/sandboxes/{path_segment(self.name)}/metrics")
        if not isinstance(value, Mapping):
            raise DaemonError("sandbox metrics returned a non-object response", code="protocol")
        return dict(value)

    @overload
    def logs(self, follow: Literal[False] = False) -> str: ...

    @overload
    def logs(self, follow: Literal[True]) -> LogStream: ...

    def logs(self, follow: bool = False) -> str | LogStream:
        """Return buffered logs or open a closeable live log stream."""
        path = f"/v1/sandboxes/{path_segment(self.name)}/logs"
        if not follow:
            return self._transport.text("GET", path, params={"follow": False})
        response = self._transport.stream("GET", path, params={"follow": True})
        return LogStream(EventStream(response))

    def tunnels(self) -> dict[int, tuple[str, int]]:
        """Return exposed guest ports mapped to local tunnel addresses."""
        value = self._transport.json("GET", f"/v1/sandboxes/{path_segment(self.name)}/tunnels")
        if isinstance(value, Mapping):
            token = value.get("connect_token")
            if isinstance(token, str):
                self.connect_token = token
            raw = value.get("tunnels")
        else:
            raw = None
        out: dict[int, tuple[str, int]] = {}
        if isinstance(raw, Mapping):
            for port, target in raw.items():
                if isinstance(target, Mapping):
                    host = str(target.get("host") or "127.0.0.1")
                    raw_port = target.get("port")
                    if raw_port is None:
                        continue
                    try:
                        out[int(port)] = (host, int(raw_port))
                    except TypeError, ValueError:
                        continue
        return out

    def create_connect_token(self) -> str:
        """Create and cache the server-issued tunnel connection token."""
        value = self._transport.json("GET", f"/v1/sandboxes/{path_segment(self.name)}/tunnels")
        if isinstance(value, Mapping) and isinstance(value.get("connect_token"), str):
            self.connect_token = str(value["connect_token"])
            return self.connect_token
        raise DaemonError("server did not return a connect token", code="protocol")

    def network_policy(self) -> dict[str, Any]:
        """Return the sandbox's current network policy."""
        value = self._transport.json("GET", f"/v1/sandboxes/{path_segment(self.name)}/network")
        if not isinstance(value, Mapping):
            raise DaemonError("network policy returned a non-object response", code="protocol")
        return dict(value)

    def set_network_policy(
        self,
        block_network: bool | None = None,
        cidr_allow: Sequence[str] | None = None,
        domain_allow: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Replace supplied fields in the sandbox network policy."""
        body = _drop_none(
            {
                "block_network": block_network,
                "cidr_allow": list(cidr_allow) if cidr_allow is not None else None,
                "domain_allow": list(domain_allow) if domain_allow is not None else None,
            }
        )
        value = self._transport.json(
            "PUT", f"/v1/sandboxes/{path_segment(self.name)}/network", json_body=body
        )
        if not isinstance(value, Mapping):
            raise DaemonError("network update returned a non-object response", code="protocol")
        return dict(value)

    def migrate(self, target: str) -> dict[str, Any]:
        """Migrate the sandbox to a non-empty mesh node id."""
        target = str(target)
        if not target:
            raise ValueError("migration target must not be empty")
        value = self._transport.json(
            "POST",
            f"/v1/sandboxes/{path_segment(self.name)}/migrate",
            json_body={"target": target},
        )
        if not isinstance(value, Mapping):
            raise DaemonError("migration returned a non-object response", code="protocol")
        self._view.update(value)
        return dict(value)

    def _proxy_params(self, params: Any | None) -> builtins.list[tuple[str, str]]:
        token = self.connect_token or self.create_connect_token()
        items = builtins.list(httpx.QueryParams(params).multi_items()) if params is not None else []
        items = [
            (key, value)
            for key, value in items
            if key not in {"connect_token", "token", "access_token"}
        ]
        items.append(("connect_token", token))
        return items

    def proxy_http(
        self,
        method: str,
        port: int,
        path: str = "",
        *,
        params: Any | None = None,
        headers: Mapping[str, str] | None = None,
        content: bytes | None = None,
        json_body: Any | None = None,
    ) -> httpx.Response:
        """Proxy one HTTP request to an exposed guest port."""
        method = method.upper()
        if method not in {"GET", "PUT", "POST", "DELETE", "OPTIONS", "HEAD", "PATCH"}:
            raise ValueError(f"unsupported proxy HTTP method {method!r}")
        port = int(port)
        if not 0 <= port <= 65535:
            raise ValueError("proxy port must be between 0 and 65535")
        tail = _path_tail(path)
        suffix = f"/{tail}" if tail else ""
        return self._transport.request(
            method,
            f"/v1/sandboxes/{path_segment(self.name)}/ports/{port}{suffix}",
            params=self._proxy_params(params),
            headers={str(key): str(value) for key, value in (headers or {}).items()},
            content=content,
            json_body=json_body,
            raise_for_status=False,
        )

    def proxy_websocket(
        self,
        port: int,
        path: str = "",
        *,
        params: Any | None = None,
    ) -> WebSocketConnection:
        """Open a WebSocket proxied to an exposed guest port."""
        port = int(port)
        if not 0 <= port <= 65535:
            raise ValueError("proxy port must be between 0 and 65535")
        tail = _path_tail(path)
        suffix = f"/{tail}" if tail else ""
        return self._transport.websocket(
            f"/v1/sandboxes/{path_segment(self.name)}/ports/{port}/ws{suffix}",
            params=self._proxy_params(params),
        )

    def close(self) -> None:
        """Release HTTP resources owned by this sandbox object."""
        if self._owns_transport and not self._transport_closed:
            self._transport_closed = True
            self._transport.close()

    def agent(self, connect_timeout: float | None = None) -> Any:
        """Raise because the v1 daemon exposes no direct guest-agent endpoint."""
        del connect_timeout
        raise DaemonError(
            "direct guest-agent access is not part of the thin SDK", code="unsupported"
        )

    def __enter__(self) -> Sandbox:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        try:
            self.terminate()
        finally:
            self.close()


class _AsyncFilesystem:
    """Async filesystem RPC facade for a Sandbox."""

    def __init__(self, filesystem: Filesystem) -> None:
        self._filesystem = filesystem

    async def read_bytes(self, *args: Any, **kwargs: Any) -> bytes:
        return await asyncio.to_thread(self._filesystem.read_bytes, *args, **kwargs)

    async def read_text(self, *args: Any, **kwargs: Any) -> str:
        return await asyncio.to_thread(self._filesystem.read_text, *args, **kwargs)

    async def write_bytes(self, *args: Any, **kwargs: Any) -> None:
        return await asyncio.to_thread(self._filesystem.write_bytes, *args, **kwargs)

    async def write_text(self, *args: Any, **kwargs: Any) -> None:
        return await asyncio.to_thread(self._filesystem.write_text, *args, **kwargs)

    async def list_files(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._filesystem.list_files, *args, **kwargs)

    async def make_directory(self, *args: Any, **kwargs: Any) -> None:
        return await asyncio.to_thread(self._filesystem.make_directory, *args, **kwargs)

    async def remove(self, *args: Any, **kwargs: Any) -> None:
        return await asyncio.to_thread(self._filesystem.remove, *args, **kwargs)

    async def stat(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return await asyncio.to_thread(self._filesystem.stat, *args, **kwargs)


class _AsyncSandbox:
    """Thread-backed async facade for a :class:`Sandbox` instance."""

    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox
        self._filesystem = _AsyncFilesystem(sandbox.filesystem)

    @property
    def filesystem(self) -> _AsyncFilesystem:
        return self._filesystem

    async def refresh(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._sandbox.refresh)

    async def exists(self) -> bool:
        return await asyncio.to_thread(self._sandbox.exists)

    async def exec(self, *cmd: str | Iterable[str], **kwargs: Any) -> Process:
        return await asyncio.to_thread(self._sandbox.exec, *cmd, **kwargs)

    async def exec_capture(self, *cmd: str | Iterable[str], **kwargs: Any) -> ExecResult:
        return await asyncio.to_thread(self._sandbox.exec_capture, *cmd, **kwargs)

    async def attach_console(self) -> ConsoleStream:
        return await asyncio.to_thread(self._sandbox.attach_console)

    async def terminate(self, *args: Any, **kwargs: Any) -> None:
        return await asyncio.to_thread(self._sandbox.terminate, *args, **kwargs)

    async def stop(self, *args: Any, **kwargs: Any) -> None:
        return await asyncio.to_thread(self._sandbox.stop, *args, **kwargs)

    async def remove(self, *args: Any, **kwargs: Any) -> None:
        return await asyncio.to_thread(self._sandbox.remove, *args, **kwargs)

    async def pause(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._sandbox.pause)

    async def resume(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._sandbox.resume)

    async def wait_until_ready(self, *args: Any, **kwargs: Any) -> None:
        return await asyncio.to_thread(self._sandbox.wait_until_ready, *args, **kwargs)

    async def snapshot(self, *args: Any, **kwargs: Any) -> str:
        return await asyncio.to_thread(self._sandbox.snapshot, *args, **kwargs)

    async def snapshot_filesystem(self, *args: Any, **kwargs: Any) -> str:
        return await asyncio.to_thread(self._sandbox.snapshot_filesystem, *args, **kwargs)

    async def poll(self) -> int | None:
        return await asyncio.to_thread(self._sandbox.poll)

    async def network_policy(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._sandbox.network_policy)

    async def set_network_policy(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return await asyncio.to_thread(self._sandbox.set_network_policy, *args, **kwargs)

    async def migrate(self, target: str) -> dict[str, Any]:
        return await asyncio.to_thread(self._sandbox.migrate, target)

    async def extend(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return await asyncio.to_thread(self._sandbox.extend, *args, **kwargs)

    async def metrics(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return await asyncio.to_thread(self._sandbox.metrics, *args, **kwargs)

    async def logs(self, follow: bool = False) -> str | LogStream:
        if follow:
            return await asyncio.to_thread(self._sandbox.logs, True)
        return await asyncio.to_thread(self._sandbox.logs, False)

    async def tunnels(self, *args: Any, **kwargs: Any) -> dict[int, tuple[str, int]]:
        return await asyncio.to_thread(self._sandbox.tunnels, *args, **kwargs)

    async def create_connect_token(self, *args: Any, **kwargs: Any) -> str:
        return await asyncio.to_thread(self._sandbox.create_connect_token, *args, **kwargs)

    async def proxy_http(self, *args: Any, **kwargs: Any) -> httpx.Response:
        return await asyncio.to_thread(self._sandbox.proxy_http, *args, **kwargs)

    async def proxy_websocket(self, *args: Any, **kwargs: Any) -> WebSocketConnection:
        return await asyncio.to_thread(self._sandbox.proxy_websocket, *args, **kwargs)

    async def close(self) -> None:
        return await asyncio.to_thread(self._sandbox.close)

    async def __aenter__(self) -> _AsyncSandbox:
        return self

    async def __aexit__(self, *exc: object) -> None:
        try:
            await self.terminate()
        finally:
            await self.close()


_REMOTE_FUNCTION_RUNNER = r"""
import asyncio
import inspect
import io
import json
import sys
import traceback

payload = json.loads(sys.stdin.buffer.read().decode("utf-8"))
namespace = {"__builtins__": __builtins__, "__name__": "__vmon_remote_function__"}
response = {"ok": False}
captured_stdout = io.StringIO()
real_stdout = sys.stdout
try:
    exec(payload["source"], namespace)
    fn = namespace[payload["name"]]
    sys.stdout = captured_stdout
    result = fn(*payload["args"], **payload["kwargs"])
    if inspect.isawaitable(result):
        result = asyncio.run(result)
    sys.stdout = real_stdout
    try:
        json.dumps(result)
    except TypeError as exc:
        raise TypeError("remote function result must be JSON-serializable") from exc
    response = {"ok": True, "result": result, "stdout": captured_stdout.getvalue()}
except BaseException as exc:
    sys.stdout = real_stdout
    response = {
        "ok": False,
        "type": type(exc).__name__,
        "message": str(exc),
        "traceback": traceback.format_exc(),
        "stdout": captured_stdout.getvalue(),
    }
sys.stdout.buffer.write(json.dumps(response, separators=(",", ":")).encode("utf-8"))
"""


class RemoteFunctionError(RuntimeError):
    """A remote function call failed inside the sandbox."""

    def __init__(self, message: str, *, remote_type: str = "", traceback_text: str = "") -> None:
        super().__init__(message)
        self.remote_type = remote_type
        self.traceback = traceback_text


class _AsyncRemoteFunction:
    """Async facade over :class:`RemoteFunction` (``await fn.aio.remote(...)``)."""

    def __init__(self, fn: RemoteFunction) -> None:
        self._fn = fn

    async def remote(self, *args: Any, **kwargs: Any) -> Any:
        """Await the function in the cached sandbox and return its result."""
        return await asyncio.to_thread(self._fn.remote, *args, **kwargs)

    async def map(self, *args: Any, **kwargs: Any) -> list[Any]:
        """Await a parallel map across an ephemeral sandbox pool."""
        return await asyncio.to_thread(self._fn.map, *args, **kwargs)

    async def starmap(self, *args: Any, **kwargs: Any) -> list[Any]:
        """Await a parallel starmap across an ephemeral sandbox pool."""
        return await asyncio.to_thread(self._fn.starmap, *args, **kwargs)


class RemoteFunction:
    """A source-serialized Python function that can run inside a warm sandbox."""

    def __init__(
        self,
        fn: Callable[..., Any],
        sandbox_kwargs: dict[str, Any],
        sandbox_factory: Callable[..., Any] | None = None,
    ) -> None:
        self._fn = fn
        self._sandbox_kwargs = dict(sandbox_kwargs)
        self._sandbox_factory = sandbox_factory or Sandbox.create
        self._sandbox: Any | None = None
        self._python_checked = False
        self._lock = threading.Lock()
        self._runner_path = "/tmp/vmon-function-runner.py"
        functools.update_wrapper(self, fn)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._fn(*args, **kwargs)

    @property
    def aio(self) -> _AsyncRemoteFunction:
        """Async facade: ``await fn.aio.remote(...)`` / ``.map(...)`` / ``.starmap(...)``."""
        return _AsyncRemoteFunction(self)

    def remote(self, *args: Any, timeout: float | None = None, **kwargs: Any) -> Any:
        """Run the function in a cached sandbox and return its deserialized result."""
        return self._run_once(self._ensure_sandbox(), args, kwargs, timeout)

    def map(
        self,
        iterable: Iterable[Any],
        *,
        concurrency: int = 4,
        ordered: bool = True,
    ) -> list[Any]:
        """Run one remote call per item across an ephemeral sandbox pool.

        Map uses fresh worker sandboxes instead of the cached ``remote()`` sandbox
        because one sandbox cannot execute items concurrently. In mesh contexts the
        factory is ``client.sandboxes.create``, so normal server-side placement
        spreads workers across nodes.
        """
        return self._map_items(iterable, star=False, ordered=ordered, concurrency=concurrency)

    def starmap(
        self,
        iterable: Iterable[Iterable[Any]],
        *,
        concurrency: int = 4,
        ordered: bool = True,
    ) -> list[Any]:
        """Run one remote call per argument tuple across an ephemeral sandbox pool."""
        return self._map_items(iterable, star=True, ordered=ordered, concurrency=concurrency)

    def _map_items(
        self,
        iterable: Iterable[Any],
        *,
        star: bool,
        ordered: bool = True,
        concurrency: int = 4,
    ) -> list[Any]:
        pool_limit = int(concurrency)
        if pool_limit < 1:
            raise ValueError("concurrency must be >= 1")
        items = list(iterable)
        if not items:
            return []
        pool_size = min(pool_limit, len(items))

        work: queue.Queue[int] = queue.Queue()
        for index in range(len(items)):
            work.put(index)
        stop = threading.Event()
        results: list[Any] = [None] * len(items)
        completed: list[tuple[int, Any]] = []
        result_lock = threading.Lock()
        first_error: list[BaseException] = []
        sandboxes: list[Any] = []

        try:
            create_kwargs = {"image": "python:3.14-slim", **self._sandbox_kwargs}
            for _ in range(pool_size):
                sandbox = self._sandbox_factory(**create_kwargs)
                sandboxes.append(sandbox)
                self._check_python(sandbox)

            def worker(sandbox: Any) -> None:
                while not stop.is_set():
                    try:
                        index = work.get_nowait()
                    except queue.Empty:
                        return
                    if stop.is_set():
                        return
                    item = items[index]
                    args = tuple(item) if star else (item,)
                    try:
                        value = self._run_once(sandbox, args, {}, None)
                    except BaseException as exc:
                        with result_lock:
                            if not first_error:
                                first_error.append(exc)
                                stop.set()
                        return
                    with result_lock:
                        results[index] = value
                        completed.append((index, value))

            threads = [
                threading.Thread(target=worker, args=(sandbox,), name=f"vmon-map-{i}", daemon=True)
                for i, sandbox in enumerate(sandboxes)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            if first_error:
                raise first_error[0]
            if ordered:
                return results
            return [value for _, value in completed]
        finally:
            for sandbox in sandboxes:
                with contextlib.suppress(Exception):
                    sandbox.terminate()

    def _run_once(
        self,
        sandbox: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        timeout: float | None,
    ) -> Any:
        sandbox.filesystem.write_text(self._runner_path, _REMOTE_FUNCTION_RUNNER)
        payload = _remote_function_payload(self._fn, args, kwargs)
        proc = sandbox.exec(
            "python3",
            "-u",
            self._runner_path,
            timeout=timeout,
            _track_entry=False,
        )
        proc.write_stdin(payload)
        proc.close_stdin()
        rc = proc.wait(timeout=timeout)
        stdout = proc.stdout.read()
        stderr = proc.stderr.read().decode("utf-8", "replace")
        if rc != 0:
            detail = stderr.strip() or f"remote function runner exited with {rc}"
            raise RemoteFunctionError(detail)
        return _remote_function_result(stdout)

    def terminate(self) -> None:
        """Terminate the cached sandbox backing this function, if it exists."""
        with self._lock:
            sandbox = self._sandbox
            self._sandbox = None
        if sandbox is not None:
            sandbox.terminate()

    def _ensure_sandbox(self) -> Any:
        with self._lock:
            if self._sandbox is None or self._sandbox.poll() is not None:
                kwargs = {"image": "python:3.14-slim", **self._sandbox_kwargs}
                self._sandbox = self._sandbox_factory(**kwargs)
                self._python_checked = False
            sandbox = self._sandbox
            if not self._python_checked:
                self._check_python(sandbox)
                self._python_checked = True
            return sandbox

    @staticmethod
    def _check_python(sandbox: Any) -> None:
        proc = sandbox.exec("python3", "--version", timeout=10, _track_entry=False)
        rc = proc.wait(timeout=10)
        if rc != 0:
            raise RemoteFunctionError(
                "remote function images must provide python3; pass image='python:3.14-slim' "
                "or another Python-capable image"
            )


def _remote_function_payload(
    fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
) -> bytes:
    source = _remote_function_source(fn)
    payload = {"source": source, "name": fn.__name__, "args": args, "kwargs": kwargs}
    try:
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")
    except TypeError as exc:
        raise ValueError("remote function arguments must be JSON-serializable") from exc


def _remote_function_source(fn: Callable[..., Any]) -> str:
    if fn.__closure__:
        raise ValueError("remote functions cannot close over local variables")
    try:
        function_source = textwrap.dedent(inspect.getsource(fn))
    except OSError as exc:
        raise ValueError("remote functions must be defined in source files") from exc
    stripped_target = _strip_decorators(function_source)
    module = inspect.getmodule(fn)
    if module is None:
        return stripped_target
    try:
        module_source = inspect.getsource(module)
    except OSError, TypeError:
        return stripped_target
    try:
        module_tree = ast.parse(module_source)
    except SyntaxError:
        return stripped_target

    imports: dict[str, tuple[int, str]] = {}
    consts: dict[str, tuple[int, str]] = {}
    defs: dict[str, tuple[int, str, set[str]]] = {}
    target_def: ast.FunctionDef | ast.AsyncFunctionDef | None = None
    target_lineno = getattr(getattr(fn, "__code__", None), "co_firstlineno", None)

    for node in module_tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            source = ast.unparse(node)
            for name in _import_bound_names(node):
                imports[name] = (node.lineno, source)
        elif isinstance(node, ast.Assign):
            try:
                ast.literal_eval(node.value)
            except TypeError, ValueError:
                continue
            source = ast.unparse(node)
            for name in _assignment_bound_names(node.targets):
                consts[name] = (node.lineno, source)
        elif isinstance(node, ast.AnnAssign):
            if node.value is None:
                continue
            try:
                ast.literal_eval(node.value)
            except TypeError, ValueError:
                continue
            source = ast.unparse(node)
            for name in _assignment_bound_names([node.target]):
                consts[name] = (node.lineno, source)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            node.decorator_list = []
            source = ast.unparse(node)
            referenced = _free_names(source)
            defs[node.name] = (node.lineno, source, referenced)
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == fn.__name__
                and (target_lineno is None or node.lineno == target_lineno)
            ):
                target_def = node

    if target_def is None:
        for node in module_tree.body:
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == fn.__name__
            ):
                target_def = node
                break
    if target_def is None:
        return stripped_target

    target_source = _strip_decorators(ast.unparse(target_def))
    target_references = _free_names(target_source)
    selected_imports: dict[int, str] = {}
    selected_consts: dict[int, str] = {}
    selected_defs: dict[int, str] = {}
    worklist = list(target_references)
    seen_names: set[str] = set()

    while worklist:
        name = worklist.pop()
        if name in seen_names:
            continue
        seen_names.add(name)
        if name in imports:
            lineno, source = imports[name]
            selected_imports.setdefault(lineno, source)
        elif name in consts:
            lineno, source = consts[name]
            selected_consts.setdefault(lineno, source)
        elif name in defs and name != fn.__name__:
            lineno, source, referenced = defs[name]
            if lineno not in selected_defs:
                selected_defs[lineno] = source
                worklist.extend(referenced)

    parts = [
        source
        for selected in (selected_imports, selected_consts, selected_defs)
        for _lineno, source in sorted(selected.items())
    ]
    parts.append(target_source.rstrip())
    return "\n\n".join(parts) + "\n"


def _strip_decorators(source: str) -> str:
    tree = ast.parse(textwrap.dedent(source))
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            node.decorator_list = []
            return ast.unparse(node) + "\n"
    raise ValueError("remote function source must contain a function definition")


def _import_bound_names(node: ast.Import | ast.ImportFrom) -> list[str]:
    names: list[str] = []
    for alias in node.names:
        if alias.name == "*":
            continue
        if alias.asname:
            names.append(alias.asname)
        else:
            names.append(alias.name.split(".", 1)[0])
    return names


def _assignment_bound_names(targets: Iterable[ast.expr]) -> list[str]:
    names: list[str] = []
    for target in targets:
        if isinstance(target, ast.Name):
            names.append(target.id)
        elif isinstance(target, (ast.Tuple, ast.List)):
            names.extend(_assignment_bound_names(target.elts))
    return names


def _free_names(source: str) -> set[str]:
    """Module-level names a def/class references, via scope-aware symtable analysis.

    Parameters, locals, comprehension/``with``/``except`` targets, and names bound
    inside nested scopes are excluded; only genuine references to the enclosing
    module scope remain (builtins are included but simply never match a module
    symbol). This prevents a local named like a module import (``def f(json):``)
    from spuriously shipping that import into the guest.
    """
    try:
        table = symtable.symtable(source, "<remote-fn>", "exec")
    except SyntaxError:
        return set()
    names: set[str] = set()
    stack = [table]
    while stack:
        scope = stack.pop()
        for symbol in scope.get_symbols():
            if symbol.is_global():
                names.add(symbol.get_name())
        stack.extend(scope.get_children())
    return names


def _remote_function_result(raw: bytes) -> Any:
    try:
        response = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise RemoteFunctionError("remote function returned an invalid response") from exc
    if not isinstance(response, dict):
        raise RemoteFunctionError("remote function returned a non-object response")
    stdout_text = response.get("stdout", "")
    if stdout_text:
        sys.stdout.write(stdout_text if isinstance(stdout_text, str) else str(stdout_text))
    if response.get("ok") is True:
        return response.get("result")
    remote_type = str(response.get("type") or "RemoteError")
    message = str(response.get("message") or "remote function failed")
    traceback_text = str(response.get("traceback") or "")
    raise RemoteFunctionError(message, remote_type=remote_type, traceback_text=traceback_text)


@overload
def function(fn: Callable[..., Any]) -> RemoteFunction: ...


@overload
def function(
    fn: None = None, **sandbox_kwargs: Any
) -> Callable[[Callable[..., Any]], RemoteFunction]: ...


def function(
    fn: Callable[..., Any] | None = None, **sandbox_kwargs: Any
) -> RemoteFunction | Callable[[Callable[..., Any]], RemoteFunction]:
    """Decorate a source-available function with a ``.remote(...)`` sandbox call.

    Bare ``@function`` and parameterized ``@function(image=..., block_network=...)``
    both yield a :class:`RemoteFunction` exposing ``.remote``/``.map``/``.starmap``.
    """

    def decorate(inner: Callable[..., Any]) -> RemoteFunction:
        return RemoteFunction(inner, sandbox_kwargs)

    if fn is not None:
        return decorate(fn)
    return decorate


def default_command(spec: Any | None, override: list[str] | None = None) -> list[str]:
    if spec is None:
        return list(override or ["/bin/sh"])
    argv = spec.argv(override)
    return argv or ["/bin/sh"]
