"""HTTP and WebSocket transport for the Rust v1 API."""

from __future__ import annotations

import base64
import json
import os
import socket
import ssl
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import httpx

from .context import LOCAL, ContextStore
from .wsframe import (
    WebSocketHandshakeError,
    client_handshake,
    encode_frame,
    read_frame_sync_details,
)

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
    encoded = quote(str(value), safe="")
    return encoded.replace(".", "%2E") if encoded in {".", ".."} else encoded


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
    """Blocking RFC 6455 connection used by streaming SDK operations."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._send_lock = threading.Lock()
        self._closed = False

    @property
    def closed(self) -> bool:
        """Return whether this connection has been closed locally or remotely."""
        return self._closed

    def send_json(self, value: dict[str, Any]) -> None:
        """Send a deterministic JSON object as one text message."""
        self.send_text(json.dumps(value, separators=(",", ":"), sort_keys=True))

    def send_text(self, text: str) -> None:
        """Send one UTF-8 text message."""
        self._send(0x1, text.encode("utf-8"))

    def send_bytes(self, data: bytes | bytearray | memoryview) -> None:
        """Send one binary message."""
        self._send(0x2, bytes(data))

    def _send(self, opcode: int, data: bytes) -> None:
        with self._send_lock:
            if self._closed:
                raise DaemonError("websocket is closed", code="closed")
            try:
                self._sock.sendall(encode_frame(opcode, data))
            except OSError as exc:
                self._closed = True
                self._release_socket()
                raise DaemonError(f"websocket write failed: {exc}", code="closed") from exc

    def recv_message(self) -> str | bytes | None:
        """Receive one complete text or binary message, handling control frames."""
        message_opcode: int | None = None
        fragments: list[bytes] = []
        while True:
            try:
                final, opcode, payload = read_frame_sync_details(self._sock)
            except (ConnectionError, OSError) as exc:
                self._closed = True
                self._release_socket()
                raise DaemonError(f"websocket read failed: {exc}", code="closed") from exc
            if opcode == 0x8:
                self._closed = True
                with self._send_lock:
                    try:
                        self._sock.sendall(encode_frame(0x8, b""))
                    except OSError:
                        pass
                    self._release_socket()
                return None
            if opcode == 0x9:
                with self._send_lock:
                    try:
                        self._sock.sendall(encode_frame(0xA, payload))
                    except OSError as exc:
                        self._closed = True
                        self._release_socket()
                        raise DaemonError(f"websocket pong failed: {exc}", code="closed") from exc
                continue
            if opcode == 0xA:
                continue
            if opcode in {0x1, 0x2}:
                if message_opcode is not None:
                    raise DaemonError("interleaved websocket message", code="protocol")
                message_opcode = opcode
                fragments.append(payload)
            elif opcode == 0x0:
                if message_opcode is None:
                    raise DaemonError("unexpected websocket continuation", code="protocol")
                fragments.append(payload)
            else:
                raise DaemonError(f"unsupported websocket opcode {opcode}", code="protocol")
            if not final:
                continue
            data = b"".join(fragments)
            if message_opcode == 0x2:
                return data
            try:
                return data.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise DaemonError("invalid websocket text frame", code="protocol") from exc

    def recv_json(self) -> dict[str, Any] | None:
        """Receive one JSON object, returning ``None`` after a close frame."""
        message = self.recv_message()
        if message is None:
            return None
        try:
            text = message.decode("utf-8") if isinstance(message, bytes) else message
            value = json.loads(text)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DaemonError("invalid websocket JSON frame", code="protocol") from exc
        if not isinstance(value, dict):
            raise DaemonError("invalid websocket frame", code="protocol")
        return value

    def _release_socket(self) -> None:
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass

    def close(self) -> None:
        """Send a close frame and release the underlying socket."""
        with self._send_lock:
            if self._closed:
                self._release_socket()
                return
            self._closed = True
            try:
                self._sock.sendall(encode_frame(0x8, b""))
            except OSError:
                pass
            self._release_socket()

    def __enter__(self) -> WebSocketConnection:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class ResponseStream:
    """Closeable streaming HTTP response returned by :class:`VmonTransport`."""

    def __init__(self, manager: Any, response: httpx.Response) -> None:
        self._manager = manager
        self.response = response
        self._closed = False

    def iter_lines(self) -> Iterator[str]:
        """Yield decoded response lines and map read failures to ``DaemonError``."""
        try:
            yield from self.response.iter_lines()
        except httpx.TimeoutException as exc:
            raise DaemonError("vmon API stream timed out", code="timeout") from exc
        except httpx.HTTPError as exc:
            raise DaemonError(f"vmon API stream failed: {exc}", code="unreachable") from exc
        finally:
            self.close()

    def iter_bytes(self) -> Iterator[bytes]:
        """Yield response chunks and map read failures to ``DaemonError``."""
        try:
            yield from self.response.iter_bytes()
        except httpx.TimeoutException as exc:
            raise DaemonError("vmon API stream timed out", code="timeout") from exc
        except httpx.HTTPError as exc:
            raise DaemonError(f"vmon API stream failed: {exc}", code="unreachable") from exc
        finally:
            self.close()

    def close(self) -> None:
        """Close the response stream idempotently."""
        if not self._closed:
            self._closed = True
            self._manager.__exit__(None, None, None)

    def __enter__(self) -> ResponseStream:
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

    def clone(self) -> VmonTransport:
        """Return an independent transport with the same endpoint and credentials."""
        return type(self)(
            base_url=None if self.uds is not None else self.base_url,
            token=self.token,
            uds=self.uds,
            timeout=self.timeout,
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
        """Close pooled HTTP connections."""
        self._client.close()

    def __enter__(self) -> VmonTransport:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Any | None = None,
        json_body: Any | None = None,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
        raise_for_status: bool = True,
    ) -> httpx.Response:
        """Perform one buffered HTTP request."""
        request_headers, request_content = self._request_data(json_body, content, headers)
        try:
            response = self._client.request(
                method,
                path,
                params=params,
                content=request_content,
                headers=request_headers,
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
        if raise_for_status and response.status_code >= 400:
            raise self._error_from_response(response)
        return response

    def stream(
        self,
        method: str,
        path: str,
        *,
        params: Any | None = None,
        json_body: Any | None = None,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> ResponseStream:
        """Open a streaming HTTP response that must be closed by the caller."""
        request_headers, request_content = self._request_data(json_body, content, headers)
        manager = self._client.stream(
            method,
            path,
            params=params,
            content=request_content,
            headers=request_headers,
        )
        try:
            response = manager.__enter__()
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
            try:
                response.read()
                error = self._error_from_response(response)
            except httpx.TimeoutException as exc:
                raise DaemonError("vmon API stream timed out", code="timeout") from exc
            except httpx.HTTPError as exc:
                raise DaemonError(f"vmon API stream failed: {exc}", code="unreachable") from exc
            finally:
                manager.__exit__(None, None, None)
            raise error
        return ResponseStream(manager, response)

    def _request_data(
        self,
        json_body: Any | None,
        content: bytes | None,
        headers: dict[str, str] | None,
    ) -> tuple[dict[str, str], bytes | None]:
        if json_body is not None and content is not None:
            raise ValueError("json_body and content are mutually exclusive")
        request_headers = dict(headers or {})
        if self.token:
            request_headers["Authorization"] = f"Bearer {self.token}"
        if json_body is not None:
            request_headers.setdefault("Content-Type", "application/json")
            content = json.dumps(
                json_body, ensure_ascii=False, separators=(",", ":"), sort_keys=True
            ).encode("utf-8")
        return request_headers, content

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

    def websocket(self, path: str, *, params: Any | None = None) -> WebSocketConnection:
        """Open an authenticated WebSocket connection."""
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        ws_path = path
        if params:
            query = str(httpx.QueryParams(params))
            if query:
                ws_path = f"{ws_path}{'&' if '?' in ws_path else '?'}{query}"
        if self.uds is not None:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            try:
                sock.connect(str(self.uds))
                client_handshake(sock, "vmon", 80, ws_path, headers)
            except WebSocketHandshakeError as exc:
                sock.close()
                raise self._error_from_handshake(exc) from exc
            except TimeoutError as exc:
                sock.close()
                raise DaemonError("vmon websocket timed out", code="timeout") from exc
            except OSError as exc:
                sock.close()
                raise DaemonError(
                    f"cannot reach vmon daemon at {self.uds}: {exc}", code="unreachable"
                ) from exc
            return WebSocketConnection(sock)

        parsed = urlsplit(self.base_url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        base_path = parsed.path.rstrip("/")
        ws_path = f"{base_path}{ws_path}" if base_path else ws_path
        raw: socket.socket | None = None
        ws_sock: socket.socket | None = None
        try:
            raw = socket.create_connection((host, port), timeout=self.timeout)
            if parsed.scheme == "https":
                ws_sock = ssl.create_default_context().wrap_socket(raw, server_hostname=host)
            else:
                ws_sock = raw
            client_handshake(ws_sock, host, port, ws_path, headers)
        except WebSocketHandshakeError as exc:
            if ws_sock is not None:
                ws_sock.close()
            elif raw is not None:
                raw.close()
            raise self._error_from_handshake(exc) from exc
        except TimeoutError as exc:
            if ws_sock is not None:
                ws_sock.close()
            elif raw is not None:
                raw.close()
            raise DaemonError("vmon websocket timed out", code="timeout") from exc
        except OSError as exc:
            if ws_sock is not None:
                ws_sock.close()
            elif raw is not None:
                raw.close()
            raise DaemonError(
                f"cannot reach vmon daemon at {self.base_url}: {exc}", code="unreachable"
            ) from exc
        if ws_sock is None:
            raise DaemonError("websocket connection was not established", code="protocol")
        return WebSocketConnection(ws_sock)

    @staticmethod
    def _error_from_handshake(error: WebSocketHandshakeError) -> DaemonError:
        if error.status >= 400:
            response = httpx.Response(
                error.status,
                content=error.body,
                request=httpx.Request("GET", "http://vmon/websocket"),
            )
            return VmonTransport._error_from_response(response)
        return DaemonError(
            str(error), code="protocol", status=error.status if error.status else None
        )

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
