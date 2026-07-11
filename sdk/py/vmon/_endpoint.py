"""Per-endpoint HTTP and WebSocket mechanics for the Rust v1 API."""

from __future__ import annotations

import base64
import json
import socket
import ssl
import threading
from collections.abc import Iterator
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import httpx

from .errors import APIError, ProtocolError, TransportError
from .wsframe import (
    WebSocketHandshakeError,
    client_handshake,
    encode_frame,
    read_frame_sync_details,
)

DEFAULT_HTTP_TIMEOUT = 60.0


def _clean_base_url(url: str) -> str:
    value = (url or "").strip()
    if not value:
        raise ValueError("endpoint is empty")
    parsed = urlsplit(value)
    if not parsed.scheme:
        value = "http://" + value
        parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"unsupported endpoint scheme {parsed.scheme!r}")
    if not parsed.netloc:
        raise ValueError(f"invalid endpoint {url!r}")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


def _safe_segment(value: str) -> str:
    encoded = quote(str(value), safe="")
    return encoded.replace(".", "%2E") if encoded in {".", ".."} else encoded


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
                raise TransportError("websocket is closed")
            try:
                self._sock.sendall(encode_frame(opcode, data))
            except OSError as exc:
                self._closed = True
                self._release_socket()
                raise TransportError(f"websocket write failed: {exc}") from exc

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
                raise TransportError(f"websocket read failed: {exc}") from exc
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
                        raise TransportError(f"websocket pong failed: {exc}") from exc
                continue
            if opcode == 0xA:
                continue
            if opcode in {0x1, 0x2}:
                if message_opcode is not None:
                    raise ProtocolError("interleaved websocket message")
                message_opcode = opcode
                fragments.append(payload)
            elif opcode == 0x0:
                if message_opcode is None:
                    raise ProtocolError("unexpected websocket continuation")
                fragments.append(payload)
            else:
                raise ProtocolError(f"unsupported websocket opcode {opcode}")
            if not final:
                continue
            data = b"".join(fragments)
            if message_opcode == 0x2:
                return data
            try:
                return data.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ProtocolError("invalid websocket text frame") from exc

    def recv_json(self) -> dict[str, Any] | None:
        """Receive one JSON object, returning ``None`` after a close frame."""
        message = self.recv_message()
        if message is None:
            return None
        try:
            text = message.decode("utf-8") if isinstance(message, bytes) else message
            value = json.loads(text)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ProtocolError("invalid websocket JSON frame") from exc
        if not isinstance(value, dict):
            raise ProtocolError("invalid websocket frame")
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
    """Closeable streaming HTTP response returned by :class:`EndpointTransport`."""

    def __init__(self, manager: Any, response: httpx.Response) -> None:
        self._manager = manager
        self.response = response
        self._closed = False

    def iter_lines(self) -> Iterator[str]:
        """Yield decoded response lines and map read failures to transport errors."""
        try:
            yield from self.response.iter_lines()
        except httpx.TimeoutException as exc:
            raise TransportError("vmon API stream timed out") from exc
        except httpx.HTTPError as exc:
            raise TransportError(f"vmon API stream failed: {exc}") from exc
        finally:
            self.close()

    def iter_bytes(self) -> Iterator[bytes]:
        """Yield response chunks and map read failures to transport errors."""
        try:
            yield from self.response.iter_bytes()
        except httpx.TimeoutException as exc:
            raise TransportError("vmon API stream timed out") from exc
        except httpx.HTTPError as exc:
            raise TransportError(f"vmon API stream failed: {exc}") from exc
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


class EndpointTransport:
    """Synchronous transport bound to one HTTP or Unix-socket endpoint."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        token: str | None = None,
        uds: str | None = None,
        timeout: float = DEFAULT_HTTP_TIMEOUT,
    ) -> None:
        self.uds: str | None = uds
        self.base_url: str = _clean_base_url(base_url) if base_url is not None else "http://vmon"
        self.token: str | None = token
        self.timeout: float = float(timeout)
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if self.uds is not None:
            transport = httpx.HTTPTransport(uds=self.uds)
            self._client = httpx.Client(
                base_url="http://vmon", headers=headers, timeout=self.timeout, transport=transport
            )
        else:
            self._client = httpx.Client(
                base_url=self.base_url, headers=headers, timeout=self.timeout
            )

    def clone(self) -> EndpointTransport:
        """Return an independent transport with the same endpoint and credentials."""
        return type(self)(
            base_url=None if self.uds is not None else self.base_url,
            token=self.token,
            uds=self.uds,
            timeout=self.timeout,
        )

    def close(self) -> None:
        """Close pooled HTTP connections."""
        self._client.close()

    def __enter__(self) -> EndpointTransport:
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
            target = self.uds if self.uds is not None else self.base_url
            raise TransportError(
                f"cannot reach vmon daemon at {target}: {exc}", endpoint=target
            ) from exc
        except httpx.TimeoutException as exc:
            raise TransportError("vmon API request timed out", endpoint=self.base_url) from exc
        except httpx.HTTPError as exc:
            raise TransportError(
                f"vmon API transport failed: {exc}", endpoint=self.base_url
            ) from exc
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
            target = self.uds if self.uds is not None else self.base_url
            raise TransportError(
                f"cannot reach vmon daemon at {target}: {exc}", endpoint=target
            ) from exc
        except httpx.TimeoutException as exc:
            raise TransportError("vmon API request timed out", endpoint=self.base_url) from exc
        except httpx.HTTPError as exc:
            raise TransportError(
                f"vmon API transport failed: {exc}", endpoint=self.base_url
            ) from exc
        if response.status_code >= 400:
            try:
                response.read()
                error = self._error_from_response(response)
            except httpx.TimeoutException as exc:
                raise TransportError("vmon API stream timed out", endpoint=self.base_url) from exc
            except httpx.HTTPError as exc:
                raise TransportError(
                    f"vmon API stream failed: {exc}", endpoint=self.base_url
                ) from exc
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
            raise ProtocolError("vmon API returned invalid JSON") from exc

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
                raise TransportError("vmon websocket timed out", endpoint=self.uds) from exc
            except OSError as exc:
                sock.close()
                raise TransportError(
                    f"cannot reach vmon daemon at {self.uds}: {exc}", endpoint=self.uds
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
            raise TransportError("vmon websocket timed out", endpoint=self.base_url) from exc
        except OSError as exc:
            if ws_sock is not None:
                ws_sock.close()
            elif raw is not None:
                raw.close()
            raise TransportError(
                f"cannot reach vmon daemon at {self.base_url}: {exc}", endpoint=self.base_url
            ) from exc
        if ws_sock is None:
            raise ProtocolError("websocket connection was not established")
        return WebSocketConnection(ws_sock)

    @staticmethod
    def _error_from_handshake(error: WebSocketHandshakeError) -> APIError | ProtocolError:
        if error.status >= 400:
            response = httpx.Response(
                error.status,
                content=error.body,
                request=httpx.Request("GET", "http://vmon/websocket"),
            )
            return EndpointTransport._error_from_response(response)
        return ProtocolError(str(error))

    @staticmethod
    def _error_from_response(response: httpx.Response) -> APIError:
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
        return APIError(message, code=code, status=response.status_code)


def path_segment(value: str) -> str:
    return _safe_segment(value)


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def unb64(text: str) -> bytes:
    return base64.b64decode(text.encode("ascii"), validate=True)
