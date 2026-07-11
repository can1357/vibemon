"""HTTP and WebSocket transport for the Rust v1 API."""

from __future__ import annotations

import base64
import json
import os
import socket
import ssl
import threading
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import httpx

from .context import LOCAL, ContextStore
from .wsframe import client_handshake, encode_frame, read_frame_sync

DEFAULT_HTTP_TIMEOUT = 60.0


class DaemonError(Exception):
    """A daemon-side failure or transport error.

    ``code`` carries the machine-readable reason (engine/mesh code when the
    gateway returned a structured detail, else the HTTP status as a string, or
    a transport code like ``unreachable``). ``status`` is the raw HTTP status
    for gateway responses, ``None`` for local/transport failures.
    """

    def __init__(self, message: str, *, code: str = "internal", status: int | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.status = status


def state_dir() -> Path:
    """Return the vmon state directory used by local UDS clients."""
    return Path(os.environ.get("VMON_HOME", str(Path.home() / ".vmon")))


def default_socket_path() -> Path:
    """Return the default Rust daemon HTTP-over-UDS socket path."""
    return state_dir() / "vmond.sock"


def _clean_base_url(url: str) -> str:
    value = (url or "").strip()
    if not value:
        raise DaemonError("context endpoint is empty", code="invalid")
    parsed = urlsplit(value)
    if not parsed.scheme:
        value = "http://" + value
        parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"}:
        raise DaemonError(f"unsupported context endpoint scheme {parsed.scheme!r}", code="invalid")
    if not parsed.netloc:
        raise DaemonError(f"invalid context endpoint {url!r}", code="invalid")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


def _safe_segment(value: str) -> str:
    return quote(str(value), safe="")


def _maybe_remote_context(context: str | None) -> tuple[str | None, str | None]:
    """Split a create ``context`` argument into transport context and build context.

    Historically the SDK used ``context=\"prod\"`` to select a saved remote
    context, while Dockerfile builds also use ``context`` for a build directory.
    Preserve that behavior: a safe name that exists in contexts.json selects the
    transport; path-like values remain API build contexts.
    """
    if context is None or context in {"", "."}:
        return None, context
    if context == LOCAL:
        return None, "."
    if any(part in context for part in ("/", "\\", ":")):
        return None, context
    store = ContextStore()
    store.load()
    if store.get(context) is not None:
        return context, "."
    return None, context


def resolve_context_name(explicit: str | None = None) -> str | None:
    """Resolve the active remote context name, if any."""
    if explicit == LOCAL:
        return None
    if explicit:
        return explicit
    store = ContextStore()
    store.load()
    name = store.current_name()
    if not name or name == LOCAL:
        return None
    return name


class WebSocketConnection:
    """Tiny blocking RFC 6455 client for the v1 WS frame protocol."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._send_lock = threading.Lock()
        self._closed = False

    def send_json(self, value: dict[str, Any]) -> None:
        self.send_text(json.dumps(value, separators=(",", ":")))

    def send_text(self, text: str) -> None:
        data = text.encode("utf-8")
        with self._send_lock:
            if self._closed:
                raise DaemonError("websocket is closed", code="closed")
            try:
                self._sock.sendall(encode_frame(0x1, data))
            except OSError as exc:
                self._closed = True
                raise DaemonError(f"websocket write failed: {exc}", code="closed") from exc

    def recv_json(self) -> dict[str, Any] | None:
        while True:
            try:
                opcode, payload = read_frame_sync(self._sock)
            except EOFError:
                self._closed = True
                return None
            except OSError as exc:
                self._closed = True
                raise DaemonError(f"websocket read failed: {exc}", code="closed") from exc
            if opcode == 0x8:  # close
                self._closed = True
                return None
            if opcode == 0x9:  # ping
                with self._send_lock:
                    self._sock.sendall(encode_frame(0xA, payload))
                continue
            if opcode == 0xA:  # pong
                continue
            if opcode not in {0x1, 0x2}:
                continue
            try:
                value = json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise DaemonError("invalid websocket JSON frame", code="protocol") from exc
            if not isinstance(value, dict):
                raise DaemonError("invalid websocket frame", code="protocol")
            return value

    def close(self) -> None:
        with self._send_lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._sock.sendall(encode_frame(0x8, b""))
            except OSError:
                pass
            try:
                self._sock.close()
            except OSError:
                pass

    def __enter__(self) -> WebSocketConnection:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class VmonTransport:
    """Synchronous client for the Rust v1 API."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        token: str | None = None,
        uds: str | os.PathLike[str] | None = None,
        timeout: float = DEFAULT_HTTP_TIMEOUT,
    ) -> None:
        self.uds = Path(uds) if uds is not None else None
        self.base_url = _clean_base_url(base_url) if base_url is not None else "http://vmon"
        self.token = token
        self.timeout = float(timeout)
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if self.uds is not None:
            transport = httpx.HTTPTransport(uds=str(self.uds))
            self._client = httpx.Client(
                base_url="http://vmon", headers=headers, timeout=self.timeout, transport=transport
            )
        else:
            self._client = httpx.Client(
                base_url=self.base_url, headers=headers, timeout=self.timeout
            )

    @classmethod
    def local(cls, *, sock_path: str | os.PathLike[str] | None = None) -> VmonTransport:
        return cls(uds=sock_path or default_socket_path())

    @classmethod
    def for_context(cls, context: str | None = None, *, token: str | None = None) -> VmonTransport:
        name = resolve_context_name(context)
        if name is None:
            return cls.local()
        store = ContextStore()
        store.load()
        ctx = store.get(name)
        if ctx is None:
            raise DaemonError(
                f"selected context {name!r} not found; run 'vmon context ls'", code="invalid"
            )
        if not ctx.endpoints:
            raise DaemonError(f"context {name!r} has no endpoints", code="invalid")
        resolved_token = token or os.environ.get("VMON_API_TOKEN") or store.load_token(name)
        return cls(base_url=ctx.endpoints[0], token=resolved_token)

    def close(self) -> None:
        self._client.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Any | None = None,
        json_body: Any | None = None,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        try:
            response = self._client.request(
                method, path, params=params, json=json_body, content=content, headers=headers
            )
        except httpx.ConnectError as exc:
            target = str(self.uds) if self.uds is not None else self.base_url
            raise DaemonError(
                f"cannot reach vmon daemon at {target}: {exc}", code="unreachable"
            ) from exc
        except httpx.TimeoutException as exc:
            raise DaemonError("vmon API request timed out", code="timeout") from exc
        except httpx.HTTPError as exc:
            raise DaemonError(f"vmon API transport failed: {exc}", code="unreachable") from exc
        if response.status_code >= 400:
            raise self._error_from_response(response)
        return response

    def json(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self.request(method, path, **kwargs)
        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError as exc:
            raise DaemonError("vmon API returned invalid JSON", code="protocol") from exc

    def text(self, method: str, path: str, **kwargs: Any) -> str:
        response = self.request(method, path, **kwargs)
        return response.text

    def bytes(self, method: str, path: str, **kwargs: Any) -> bytes:
        response = self.request(method, path, **kwargs)
        return response.content

    def websocket(self, path: str) -> WebSocketConnection:
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        if self.uds is not None:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.connect(str(self.uds))
                client_handshake(sock, "vmon", 80, path, headers)
            except OSError as exc:
                sock.close()
                raise DaemonError(
                    f"cannot reach vmon daemon at {self.uds}: {exc}", code="unreachable"
                ) from exc
            except Exception:
                sock.close()
                raise
            return WebSocketConnection(sock)

        parsed = urlsplit(self.base_url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        base_path = parsed.path.rstrip("/")
        ws_path = f"{base_path}{path}" if base_path else path
        raw = socket.create_connection((host, port), timeout=self.timeout)
        ws_sock: socket.socket
        if parsed.scheme == "https":
            ws_sock = ssl.create_default_context().wrap_socket(raw, server_hostname=host)
        else:
            ws_sock = raw
        try:
            client_handshake(ws_sock, host, port, ws_path, headers)
        except Exception:
            ws_sock.close()
            raise
        return WebSocketConnection(ws_sock)

    @staticmethod
    def _error_from_response(response: httpx.Response) -> DaemonError:
        code = "unauthorized" if response.status_code in {401, 403} else str(response.status_code)
        message = response.text.strip() or f"vmon API HTTP {response.status_code}"
        try:
            body = response.json()
        except ValueError:
            body = None
        if isinstance(body, dict):
            raw_code = body.get("code")
            raw_message = body.get("message")
            if isinstance(raw_code, str) and raw_code:
                code = raw_code
            detail = body.get("detail")
            if isinstance(raw_message, str) and raw_message:
                message = raw_message
            elif isinstance(detail, str) and detail:
                message = detail
            elif isinstance(detail, dict):
                detail_code = detail.get("code")
                detail_message = detail.get("message")
                if isinstance(detail_code, str) and detail_code:
                    code = detail_code
                if isinstance(detail_message, str) and detail_message:
                    message = detail_message
        return DaemonError(message, code=code, status=response.status_code)


def get_transport(context: str | None = None, *, token: str | None = None) -> VmonTransport:
    """Resolve the SDK transport from an explicit/selected context."""
    return VmonTransport.for_context(context, token=token)


def split_create_context(context: str | None) -> tuple[str | None, str | None]:
    """Return ``(transport_context, api_build_context)`` for Sandbox.create."""
    return _maybe_remote_context(context)


def path_segment(value: str) -> str:
    return _safe_segment(value)


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def unb64(text: str) -> bytes:
    return base64.b64decode(text.encode("ascii"), validate=True)
