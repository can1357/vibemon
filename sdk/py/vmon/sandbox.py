"""Bound sandbox resources for the Python SDK."""

from __future__ import annotations

import asyncio
import base64
import builtins
import socket
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, overload

import httpx

from ._endpoint import ResponseStream, WebSocketConnection, path_segment
from .driver import response_endpoint
from .errors import APIError, ProtocolError
from .process import ConsoleStream, EventStream, LogStream, Process, open_process
from .secret import Secret, merge_secrets
from .volume import Volume

if TYPE_CHECKING:
    from .client import Client
    from .models import FileInfo, SandboxInfo


@dataclass(frozen=True, slots=True)
class ExecResult:
    """Captured output and exit status from a non-streaming command."""

    returncode: int
    stdout: bytes
    stderr: bytes


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
    wire: list[dict[str, Any]] = []
    for item in secrets:
        if isinstance(item, Secret):
            wire.append(item.to_wire())
        elif isinstance(item, Mapping):
            wire.append(Secret.from_dict(item).to_wire())
        else:
            raise TypeError(f"expected Secret or mapping, got {type(item).__name__}")
    return wire


def _volume_name(value: Any) -> str:
    if isinstance(value, Volume):
        return value.name
    if isinstance(value, str):
        return Volume(value).name
    raise TypeError("volume spec must be a Volume, name string, tuple, or mapping")


def _volume_wire(volumes: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if volumes is None:
        return None
    wire: dict[str, Any] = {}
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
        wire[str(mount)] = {"name": name, "read_only": True} if read_only else name
    return wire


def _clone_create_extra(kwargs: Mapping[str, Any]) -> dict[str, Any]:
    allowed = {
        "image",
        "template",
        "dockerfile",
        "context",
        "name",
        "sandbox_name",
        "agent",
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
        raise TypeError(f"unsupported sandbox option(s): {', '.join(unknown)}")
    extra = {key: value for key, value in kwargs.items() if value is not None}
    if "sandbox_name" in extra:
        if "name" in extra:
            raise TypeError("name and sandbox_name are mutually exclusive")
        extra["name"] = extra.pop("sandbox_name")
    return extra


class Files:
    """Filesystem operations bound to one sandbox."""

    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox
        self._aio = _AsyncFiles(self)

    @property
    def aio(self) -> _AsyncFiles:
        """Return thread-backed async filesystem operations."""
        return self._aio

    def read_bytes(self, path: str) -> bytes:
        """Read an entire guest file as bytes."""
        return self._sandbox._bytes(
            "GET",
            self._path("files"),
            params={"path": path},
        )

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        """Read and decode an entire guest text file."""
        return self.read_bytes(path).decode(encoding)

    def write_bytes(
        self,
        path: str,
        data: bytes | bytearray | memoryview,
        mode: int = 0o644,
    ) -> None:
        """Replace a guest file with the supplied bytes."""
        del mode
        self._sandbox._json(
            "PUT",
            self._path("files"),
            params={"path": path},
            content=bytes(data),
            headers={"Content-Type": "application/octet-stream"},
        )

    def write_text(
        self,
        path: str,
        text: str,
        encoding: str = "utf-8",
        mode: int = 0o644,
    ) -> None:
        """Encode and replace a guest text file."""
        self.write_bytes(path, text.encode(encoding), mode=mode)

    def list(self, path: str = ".") -> list[FileInfo]:
        """List typed directory entries at a guest path."""
        value = self._sandbox._json(
            "GET",
            self._path("files/list"),
            params={"path": path},
        )
        entries = value.get("entries") if isinstance(value, Mapping) else value
        if not isinstance(entries, list) or not all(isinstance(item, Mapping) for item in entries):
            raise ProtocolError("file list returned a malformed response")
        from .models import FileInfo

        return [FileInfo.from_dict(item) for item in entries]

    def stat(self, path: str) -> FileInfo:
        """Return typed metadata for one guest path."""
        value = self._sandbox._json(
            "GET",
            self._path("files/stat"),
            params={"path": path},
        )
        if not isinstance(value, Mapping):
            raise ProtocolError("file stat returned a malformed response")
        from .models import FileInfo

        return FileInfo.from_dict(value)

    def delete(self, path: str, recursive: bool = False) -> None:
        """Delete a guest path, optionally recursively."""
        self._sandbox._json(
            "DELETE",
            self._path("files"),
            params={"path": path, "recursive": bool(recursive)},
        )

    def mkdir(self, path: str, parents: bool = True) -> None:
        """Create a directory through the guest agent's exec service."""
        command = ["mkdir"]
        if parents:
            command.append("-p")
        command.append(path)
        result = self._sandbox.run(command)
        if result.returncode != 0:
            raise APIError(
                result.stderr.decode("utf-8", "replace") or "mkdir failed",
                code="engine",
            )

    def open(self, path: str) -> ResponseStream:
        """Open a streaming response for a guest file."""
        response = self._sandbox._request(
            "GET",
            self._path("files"),
            params={"path": path},
            stream=True,
        )
        if not isinstance(response, ResponseStream):
            raise ProtocolError("file open did not return a stream")
        return response

    def _path(self, suffix: str) -> str:
        return f"{self._sandbox._base_path}/{suffix}"


class Ports:
    """HTTP and WebSocket proxies bound to one sandbox's exposed ports."""

    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox
        self._aio = _AsyncPorts(self)

    @property
    def aio(self) -> _AsyncPorts:
        """Return thread-backed async port proxy operations."""
        return self._aio

    def http(
        self,
        port: int,
        method: str,
        path: str = "",
        *,
        params: Any | None = None,
        headers: Mapping[str, str] | None = None,
        content: bytes | None = None,
        json: Any | None = None,
    ) -> httpx.Response:
        """Proxy one HTTP request to an exposed guest port."""
        port = _validate_port(port)
        method = method.upper()
        if method not in {"GET", "PUT", "POST", "DELETE", "OPTIONS", "HEAD", "PATCH"}:
            raise ValueError(f"unsupported proxy HTTP method {method!r}")
        tail = _path_tail(path)
        suffix = f"/{tail}" if tail else ""
        response = self._sandbox._request(
            method,
            f"{self._sandbox._base_path}/ports/{port}{suffix}",
            params=self._sandbox._proxy_params(params),
            headers={str(key): str(value) for key, value in (headers or {}).items()},
            content=content,
            json=json,
            raise_for_status=False,
        )
        if isinstance(response, ResponseStream | tuple):
            if isinstance(response, ResponseStream):
                response.close()
            raise ProtocolError("port proxy returned an unexpected response")
        return response

    def websocket(
        self,
        port: int,
        path: str = "",
        *,
        params: Any | None = None,
    ) -> WebSocketConnection:
        """Open a WebSocket proxied to an exposed guest port."""
        port = _validate_port(port)
        tail = _path_tail(path)
        suffix = f"/{tail}" if tail else ""
        result = self._sandbox._request(
            "WS",
            f"{self._sandbox._base_path}/ports/{port}/ws{suffix}",
            params=self._sandbox._proxy_params(params),
            websocket=True,
        )
        if not isinstance(result, tuple):
            raise ProtocolError("port proxy did not return a WebSocket")
        return result[0]


def _validate_port(port: int) -> int:
    value = int(port)
    if not 0 <= value <= 65535:
        raise ValueError("proxy port must be between 0 and 65535")
    return value


class Sandbox:
    """A sandbox resource bound to its owning client and current mesh endpoint."""

    def __init__(
        self,
        client: Client,
        info: Mapping[str, Any],
        *,
        endpoint: str | None = None,
    ) -> None:
        identifier = info.get("id") or info.get("name")
        if not isinstance(identifier, str) or not identifier:
            raise ValueError("sandbox id is required")
        self._client = client
        self._raw = dict(info)
        self._raw.setdefault("id", identifier)
        self._endpoint = endpoint
        self._workdir = self._string_detail("workdir")
        raw_env = self._raw.get("env")
        self._env = (
            {str(key): str(value) for key, value in raw_env.items()}
            if isinstance(raw_env, Mapping)
            else {}
        )
        self._secret_env: dict[str, str] = {}
        self._terminated = False
        self._removed = False
        self._connect_token: str | None = None
        self.files = Files(self)
        self.ports = Ports(self)
        self._aio = _AsyncSandbox(self)

    @property
    def id(self) -> str:
        """Return the stable sandbox identifier."""
        return str(self._raw["id"])

    @property
    def info(self) -> SandboxInfo:
        """Return a typed snapshot of the latest server view."""
        from .models import SandboxInfo

        return SandboxInfo.from_dict(self._raw)

    @property
    def node(self) -> str | None:
        """Return the mesh node id annotated by the server, when available."""
        value = self._raw.get("node")
        return value if isinstance(value, str) and value else None

    @property
    def endpoint(self) -> str | None:
        """Return the base URL currently pinned for sandbox-scoped calls."""
        return self._endpoint

    @property
    def aio(self) -> _AsyncSandbox:
        """Return a thread-backed async facade over every sandbox operation."""
        return self._aio

    @property
    def _base_path(self) -> str:
        return f"/v1/sandboxes/{path_segment(self.id)}"

    def __repr__(self) -> str:
        return f"Sandbox(id={self.id!r}, endpoint={self.endpoint!r})"

    def __str__(self) -> str:
        return self.id

    def _bind_defaults(
        self,
        *,
        workdir: str | None,
        env: Mapping[str, str] | None,
        tags: Mapping[str, str] | None,
        secrets: Iterable[Secret | Mapping[str, object]] | None,
    ) -> None:
        self._workdir = workdir or self._workdir
        if env is not None:
            self._env = {str(key): str(value) for key, value in env.items()}
        if tags is not None:
            self._raw["tags"] = {str(key): str(value) for key, value in tags.items()}
        self._secret_env = merge_secrets(secrets)

    def _string_detail(self, key: str) -> str | None:
        value = self._raw.get(key)
        return value if isinstance(value, str) else None

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Any | None = None,
        json: Any | None = None,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
        stream: bool = False,
        websocket: bool = False,
        raise_for_status: bool = True,
    ) -> httpx.Response | ResponseStream | tuple[WebSocketConnection, str]:
        def send() -> httpx.Response | ResponseStream | tuple[WebSocketConnection, str]:
            if websocket:
                return self._client.driver.websocket(
                    path,
                    query=params,
                    endpoint=self._endpoint,
                )
            return self._client.driver.request(
                method,
                path,
                params=params,
                json=json,
                content=content,
                headers=headers,
                stream=stream,
                endpoint=self._endpoint,
                raise_for_status=raise_for_status,
            )

        try:
            result = send()
        except APIError as original:
            if (
                original.code != "not_found"
                and original.status != 404
                or len(self._client.driver.endpoints()) <= 1
            ):
                raise
            try:
                self._endpoint = self._client.driver.resolve_sandbox(self.id, self._endpoint)
            except APIError:
                raise original from None
            result = send()

        if isinstance(result, tuple):
            self._endpoint = result[1]
        else:
            selected = response_endpoint(result)
            if selected is not None:
                self._endpoint = selected
        return result

    def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        result = self._request(method, path, **kwargs)
        if isinstance(result, ResponseStream):
            result.close()
            raise ProtocolError(f"{path} unexpectedly returned a stream")
        if isinstance(result, tuple):
            result[0].close()
            raise ProtocolError(f"{path} unexpectedly returned a WebSocket")
        if not result.content:
            return {}
        try:
            return result.json()
        except ValueError as exc:
            raise ProtocolError(f"{path} returned invalid JSON") from exc

    def _bytes(self, method: str, path: str, **kwargs: Any) -> bytes:
        result = self._request(method, path, **kwargs)
        if isinstance(result, ResponseStream):
            result.close()
            raise ProtocolError(f"{path} unexpectedly returned a stream")
        if isinstance(result, tuple):
            result[0].close()
            raise ProtocolError(f"{path} unexpectedly returned a WebSocket")
        return result.content

    def refresh(self) -> SandboxInfo:
        """Refresh this object's server view and return its typed form."""
        value = self._json("GET", self._base_path)
        self._update(value, "inspect returned a non-object response")
        return self.info

    def run(
        self,
        *cmd: str | Iterable[str],
        workdir: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        tty: bool = False,
    ) -> ExecResult:
        """Run a command to completion and capture stdout, stderr, and exit status."""
        body = self._exec_body(cmd, workdir=workdir, env=env, timeout=timeout, tty=tty)
        value = self._json("POST", f"{self._base_path}/exec", json=body)
        if not isinstance(value, Mapping):
            raise ProtocolError("captured exec returned a non-object response")
        raw_exit = value.get("exit")
        stdout_b64 = value.get("stdout_b64")
        stderr_b64 = value.get("stderr_b64")
        if (
            isinstance(raw_exit, bool)
            or not isinstance(raw_exit, int)
            or not isinstance(stdout_b64, str)
            or not isinstance(stderr_b64, str)
        ):
            raise ProtocolError("captured exec returned a malformed response")
        try:
            stdout = base64.b64decode(stdout_b64, validate=True)
            stderr = base64.b64decode(stderr_b64, validate=True)
        except (ValueError, TypeError) as exc:
            raise ProtocolError("captured exec returned invalid base64") from exc
        return ExecResult(returncode=raw_exit, stdout=stdout, stderr=stderr)

    def exec(
        self,
        *cmd: str | Iterable[str],
        workdir: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        pty: bool = False,
        tty: bool = False,
    ) -> Process:
        """Open a streaming command session."""
        use_tty = bool(pty or tty)
        body = self._exec_body(
            cmd,
            workdir=workdir,
            env=env,
            timeout=timeout,
            tty=use_tty,
        )
        result = self._request(
            "WS",
            f"{self._base_path}/exec",
            websocket=True,
        )
        if not isinstance(result, tuple):
            raise ProtocolError("exec did not return a WebSocket")
        return open_process(
            result[0],
            body,
            timeout=timeout,
            tty=use_tty,
            thread_name=f"vmon-exec-{self.id}",
        )

    def _exec_body(
        self,
        cmd: tuple[str | Iterable[str], ...],
        *,
        workdir: str | None,
        env: Mapping[str, str] | None,
        timeout: float | None,
        tty: bool,
    ) -> dict[str, Any]:
        argv = _coerce_cmd(cmd)
        if not argv:
            raise ValueError("exec command must not be empty")
        merged_env = {**self._env, **self._secret_env}
        if env:
            merged_env.update({str(key): str(value) for key, value in env.items()})
        return _drop_none(
            {
                "cmd": argv,
                "workdir": workdir or self._workdir,
                "env": merged_env or None,
                "timeout": timeout,
                "tty": tty,
            }
        )

    def attach(self) -> ConsoleStream:
        """Open a live read-only console stream."""
        result = self._request("WS", f"{self._base_path}/attach", websocket=True)
        if not isinstance(result, tuple):
            raise ProtocolError("attach did not return a WebSocket")
        return ConsoleStream(result[0])

    @overload
    def logs(self, follow: Literal[False] = False) -> str: ...

    @overload
    def logs(self, follow: Literal[True]) -> LogStream: ...

    def logs(self, follow: bool = False) -> str | LogStream:
        """Return buffered logs or open a closeable live log stream."""
        path = f"{self._base_path}/logs"
        if not follow:
            result = self._request("GET", path, params={"follow": False})
            if isinstance(result, ResponseStream | tuple):
                if isinstance(result, ResponseStream):
                    result.close()
                raise ProtocolError("logs returned an unexpected response")
            return result.text
        result = self._request("GET", path, params={"follow": True}, stream=True)
        if not isinstance(result, ResponseStream):
            raise ProtocolError("follow logs did not return a stream")
        return LogStream(EventStream(result))

    def metrics(self) -> dict[str, Any]:
        """Return this sandbox's runtime metrics."""
        value = self._json("GET", f"{self._base_path}/metrics")
        if not isinstance(value, Mapping):
            raise ProtocolError("sandbox metrics returned a non-object response")
        return dict(value)

    def stop(self, wait: bool = True) -> None:
        """Stop the sandbox while retaining its server record."""
        del wait
        self._update_optional(self._json("POST", f"{self._base_path}/stop"))

    def terminate(self, wait: bool = True) -> None:
        """Terminate the sandbox idempotently."""
        del wait
        if self._terminated or self._removed:
            return
        self._update_optional(self._json("POST", f"{self._base_path}/terminate"))
        self._terminated = True

    def remove(self) -> None:
        """Delete the sandbox record idempotently."""
        if self._removed:
            return
        self._update_optional(self._json("DELETE", self._base_path))
        self._terminated = True
        self._removed = True

    def pause(self) -> SandboxInfo:
        """Pause virtual CPUs and return the updated view."""
        self._update(self._json("POST", f"{self._base_path}/pause"), "pause returned invalid data")
        return self.info

    def resume(self) -> SandboxInfo:
        """Resume virtual CPUs and return the updated view."""
        self._update(
            self._json("POST", f"{self._base_path}/resume"),
            "resume returned invalid data",
        )
        return self.info

    def extend(self, secs: int) -> SandboxInfo:
        """Extend the sandbox deadline by a non-negative number of seconds."""
        secs = int(secs)
        if secs < 0:
            raise ValueError("extension seconds must be >= 0")
        self._update(
            self._json("POST", f"{self._base_path}/extend", json={"secs": secs}),
            "extend returned a non-object response",
        )
        return self.info

    def migrate(self, target: str) -> SandboxInfo:
        """Migrate to a mesh node and re-pin this object to the new endpoint."""
        target = str(target)
        if not target:
            raise ValueError("migration target must not be empty")
        self._update(
            self._json("POST", f"{self._base_path}/migrate", json={"target": target}),
            "migration returned a non-object response",
        )
        self._endpoint = self._client.driver.resolve_sandbox(self.id, self._endpoint)
        return self.info

    def snapshot(self, name: str | None = None, *, stop: bool = False) -> str:
        """Create a full VM snapshot and return its server-assigned name."""
        value = self._json(
            "POST",
            f"{self._base_path}/snapshots",
            json={"name": name, "stop": bool(stop)},
        )
        snapshot = value.get("snapshot") if isinstance(value, Mapping) else None
        if not isinstance(snapshot, str) or not snapshot:
            raise ProtocolError("snapshot returned a malformed response")
        return snapshot

    def snapshot_filesystem(self, name: str | None = None) -> str:
        """Snapshot the guest filesystem and return its template image name."""
        value = self._json(
            "POST",
            f"{self._base_path}/snapshots/fs",
            json={"name": name},
        )
        image = value.get("image") if isinstance(value, Mapping) else None
        if not isinstance(image, str) or not image:
            raise ProtocolError("filesystem snapshot returned a malformed response")
        return image

    def network(self) -> dict[str, Any]:
        """Return the current sandbox network policy."""
        value = self._json("GET", f"{self._base_path}/network")
        if not isinstance(value, Mapping):
            raise ProtocolError("network policy returned a non-object response")
        return dict(value)

    def set_network(self, policy: Mapping[str, Any]) -> dict[str, Any]:
        """Replace supplied fields in the sandbox network policy."""
        value = self._json("PUT", f"{self._base_path}/network", json=dict(policy))
        if not isinstance(value, Mapping):
            raise ProtocolError("network update returned a non-object response")
        return dict(value)

    def tunnels(self) -> dict[int, tuple[str, int]]:
        """Return exposed guest ports mapped to local tunnel addresses."""
        value = self._json("GET", f"{self._base_path}/tunnels")
        raw = None
        if isinstance(value, Mapping):
            token = value.get("connect_token")
            if isinstance(token, str) and token:
                self._connect_token = token
            raw = value.get("tunnels")
        tunnels: dict[int, tuple[str, int]] = {}
        if isinstance(raw, Mapping):
            for port, target in raw.items():
                if not isinstance(target, Mapping):
                    continue
                host = str(target.get("host") or "127.0.0.1")
                raw_port = target.get("port")
                if (
                    isinstance(port, bool)
                    or not isinstance(port, int | str)
                    or isinstance(raw_port, bool)
                    or not isinstance(raw_port, int | str)
                ):
                    continue
                try:
                    tunnels[int(port)] = (host, int(raw_port))
                except ValueError:
                    continue
        return tunnels

    def wait_ready(self, probe: Any = None, timeout: float = 300) -> None:
        """Poll a tunnel port or guest command until it succeeds or times out."""
        if probe is None:
            return
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                if isinstance(probe, int):
                    port = probe
                elif isinstance(probe, Mapping) and "port" in probe:
                    raw_probe_port = probe["port"]
                    if isinstance(raw_probe_port, bool) or not isinstance(
                        raw_probe_port, int | str
                    ):
                        raise ValueError("readiness probe port must be an integer")
                    port = int(raw_probe_port)
                else:
                    port = None
                if port is not None:
                    target = self.tunnels().get(port)
                    if target is not None:
                        with socket.create_connection(target, timeout=min(1.0, timeout)):
                            return
                else:
                    result = self.run(
                        "sh",
                        "-lc",
                        str(probe),
                        timeout=min(5.0, timeout),
                    )
                    if result.returncode == 0:
                        return
            except Exception as exc:
                last_error = exc
            time.sleep(0.1)
        message = f"sandbox readiness probe timed out after {timeout:.0f}s"
        raise TimeoutError(message) from last_error

    def _proxy_params(self, params: Any | None) -> builtins.list[tuple[str, str]]:
        if self._connect_token is None:
            self.tunnels()
        if self._connect_token is None:
            raise ProtocolError("server did not return a connect token")
        items = builtins.list(httpx.QueryParams(params).multi_items()) if params is not None else []
        filtered = [
            (key, value)
            for key, value in items
            if key not in {"connect_token", "token", "access_token"}
        ]
        filtered.append(("connect_token", self._connect_token))
        return filtered

    def _update(self, value: Any, error: str) -> None:
        if not isinstance(value, Mapping):
            raise ProtocolError(error)
        self._raw.update(value)

    def _update_optional(self, value: Any) -> None:
        if isinstance(value, Mapping):
            self._raw.update(value)

    def __enter__(self) -> Sandbox:
        return self

    def __exit__(self, *exc: object) -> None:
        self.terminate()


class _AsyncFiles:
    def __init__(self, files: Files) -> None:
        self._files = files

    async def read_bytes(self, *args: Any, **kwargs: Any) -> bytes:
        return await asyncio.to_thread(self._files.read_bytes, *args, **kwargs)

    async def read_text(self, *args: Any, **kwargs: Any) -> str:
        return await asyncio.to_thread(self._files.read_text, *args, **kwargs)

    async def write_bytes(self, *args: Any, **kwargs: Any) -> None:
        await asyncio.to_thread(self._files.write_bytes, *args, **kwargs)

    async def write_text(self, *args: Any, **kwargs: Any) -> None:
        await asyncio.to_thread(self._files.write_text, *args, **kwargs)

    async def list(self, *args: Any, **kwargs: Any) -> list[FileInfo]:
        return await asyncio.to_thread(self._files.list, *args, **kwargs)

    async def stat(self, *args: Any, **kwargs: Any) -> FileInfo:
        return await asyncio.to_thread(self._files.stat, *args, **kwargs)

    async def delete(self, *args: Any, **kwargs: Any) -> None:
        await asyncio.to_thread(self._files.delete, *args, **kwargs)

    async def mkdir(self, *args: Any, **kwargs: Any) -> None:
        await asyncio.to_thread(self._files.mkdir, *args, **kwargs)

    async def open(self, *args: Any, **kwargs: Any) -> ResponseStream:
        return await asyncio.to_thread(self._files.open, *args, **kwargs)


class _AsyncPorts:
    def __init__(self, ports: Ports) -> None:
        self._ports = ports

    async def http(self, *args: Any, **kwargs: Any) -> httpx.Response:
        return await asyncio.to_thread(self._ports.http, *args, **kwargs)

    async def websocket(self, *args: Any, **kwargs: Any) -> WebSocketConnection:
        return await asyncio.to_thread(self._ports.websocket, *args, **kwargs)


class _AsyncSandbox:
    """Thread-backed async facade for a bound sandbox."""

    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox

    @property
    def files(self) -> _AsyncFiles:
        return self._sandbox.files.aio

    @property
    def ports(self) -> _AsyncPorts:
        return self._sandbox.ports.aio

    async def refresh(self) -> SandboxInfo:
        return await asyncio.to_thread(self._sandbox.refresh)

    async def run(self, *args: Any, **kwargs: Any) -> ExecResult:
        return await asyncio.to_thread(self._sandbox.run, *args, **kwargs)

    async def exec(self, *args: Any, **kwargs: Any) -> Process:
        return await asyncio.to_thread(self._sandbox.exec, *args, **kwargs)

    async def attach(self) -> ConsoleStream:
        return await asyncio.to_thread(self._sandbox.attach)

    async def logs(self, follow: bool = False) -> str | LogStream:
        if follow:
            return await asyncio.to_thread(self._sandbox.logs, True)
        return await asyncio.to_thread(self._sandbox.logs, False)

    async def metrics(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._sandbox.metrics)

    async def stop(self, wait: bool = True) -> None:
        await asyncio.to_thread(self._sandbox.stop, wait)

    async def terminate(self, wait: bool = True) -> None:
        await asyncio.to_thread(self._sandbox.terminate, wait)

    async def remove(self) -> None:
        await asyncio.to_thread(self._sandbox.remove)

    async def pause(self) -> SandboxInfo:
        return await asyncio.to_thread(self._sandbox.pause)

    async def resume(self) -> SandboxInfo:
        return await asyncio.to_thread(self._sandbox.resume)

    async def extend(self, secs: int) -> SandboxInfo:
        return await asyncio.to_thread(self._sandbox.extend, secs)

    async def migrate(self, target: str) -> SandboxInfo:
        return await asyncio.to_thread(self._sandbox.migrate, target)

    async def snapshot(self, *args: Any, **kwargs: Any) -> str:
        return await asyncio.to_thread(self._sandbox.snapshot, *args, **kwargs)

    async def snapshot_filesystem(self, *args: Any, **kwargs: Any) -> str:
        return await asyncio.to_thread(self._sandbox.snapshot_filesystem, *args, **kwargs)

    async def network(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._sandbox.network)

    async def set_network(self, policy: Mapping[str, Any]) -> dict[str, Any]:
        return await asyncio.to_thread(self._sandbox.set_network, policy)

    async def tunnels(self) -> dict[int, tuple[str, int]]:
        return await asyncio.to_thread(self._sandbox.tunnels)

    async def wait_ready(self, *args: Any, **kwargs: Any) -> None:
        await asyncio.to_thread(self._sandbox.wait_ready, *args, **kwargs)

    async def __aenter__(self) -> _AsyncSandbox:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.terminate()
