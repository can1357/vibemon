"""Mesh owner routing and HTTP/WebSocket proxy helpers."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import os
import re
import ssl
from collections.abc import MutableMapping
from typing import Any
from urllib.parse import urlencode, urlsplit

from fastapi import HTTPException, Request, Response, WebSocket, status
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.background import BackgroundTask

from ..core import EngineError
from ..mesh import Mesh, MeshError
from ..wsframe import encode_frame, read_frame
from .auth import (
    _bearer_token_authorized,
    _client_token_only,
    _is_admin_path,
    _request_bearer_token,
)
from .runtime import ServerRuntime
from .supervisor import Supervisor, _sse


def _target_query(items: list[tuple[str, str]]) -> str:
    return urlencode([(key, value) for key, value in items if key not in {"token", "access_token"}])


def _decode_chunked_http_body(body: bytes) -> bytes:
    chunks: list[bytes] = []
    rest = body
    while rest:
        size_line, sep, rest = rest.partition(b"\r\n")
        if not sep:
            return body
        try:
            size = int(size_line.split(b";", 1)[0], 16)
        except ValueError:
            return body
        if size == 0:
            return b"".join(chunks)
        chunks.append(rest[:size])
        rest = rest[size + 2 :]
    return b"".join(chunks)


async def _proxy_http_request(request: Request, target: tuple[str, int], rest: str) -> Response:
    # The SDK/network layer already owns guest TCP forwarding, so the REST proxy relays
    # HTTP over the exposed localhost tunnel instead of opening guest-facing sockets here.
    host, port = target
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except OSError as exc:
        raise coded_http(
            status.HTTP_502_BAD_GATEWAY, "unreachable", f"tunnel connect failed: {exc}"
        ) from exc
    path = "/" + rest.lstrip("/")
    query = _target_query(list(request.query_params.multi_items()))
    if query:
        path += "?" + query
    body = await request.body()
    headers = [
        f"{request.method} {path} HTTP/1.1",
        f"Host: {host}:{port}",
        "Connection: close",
        f"Content-Length: {len(body)}",
    ]
    for key, value in request.headers.items():
        lower = key.lower()
        if lower in {"host", "connection", "content-length", "authorization"}:
            continue
        headers.append(f"{key}: {value}")
    writer.write(("\r\n".join(headers) + "\r\n\r\n").encode("latin-1") + body)
    await writer.drain()
    raw = await reader.read()
    writer.close()
    with contextlib.suppress(Exception):
        await writer.wait_closed()
    head, sep, response_body = raw.partition(b"\r\n\r\n")
    if not sep:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail="invalid tunnel HTTP response")
    lines = head.decode("iso-8859-1", "replace").split("\r\n")
    try:
        status_code = int(lines[0].split(" ", 2)[1])
    except Exception as exc:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY, detail="invalid tunnel HTTP status"
        ) from exc
    response_headers: dict[str, str] = {}
    chunked = False
    for line in lines[1:]:
        key, _, value = line.partition(":")
        if not key:
            continue
        lower = key.lower()
        if lower == "transfer-encoding" and "chunked" in value.lower():
            chunked = True
            continue
        if lower in {"connection", "content-length", "transfer-encoding"}:
            continue
        response_headers[key] = value.strip()
    if chunked:
        response_body = _decode_chunked_http_body(response_body)
    return Response(
        b"" if request.method == "HEAD" else response_body,
        status_code=status_code,
        headers=response_headers,
    )


_SANDBOX_PATH_RE = re.compile(r"^/v1/sandboxes/(?:from_id/)?([^/]+)(?:/.*)?$")
_RESERVED_SANDBOX_SEGMENTS = {"from_id"}
_HOP_HEADERS = {"host", "connection", "content-length", "transfer-encoding"}


def _filter_resp_headers(headers: MutableMapping[str, str]) -> dict[str, str]:
    return {
        key: value
        for key, value in headers.items()
        if key.lower() not in {"connection", "content-length", "transfer-encoding"}
    }


async def _proxy_to_peer(request: Request, peer_url: str, token: str) -> Response:
    import httpx

    headers = {
        key: value for key, value in request.headers.items() if key.lower() not in _HOP_HEADERS
    }
    headers["Authorization"] = f"Bearer {token}"
    headers["X-Vmon-Mesh-Hop"] = "1"
    body = await request.body()
    target = peer_url.rstrip("/") + request.url.path
    if request.url.query:
        target += f"?{request.url.query}"
    client = request.app.state.mesh_http
    req = client.build_request(request.method, target, content=body, headers=headers)
    try:
        resp = await client.send(req, stream=True)
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
        # Connection never established: the request provably did not run.
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "sandbox owner unreachable"
        ) from exc
    except httpx.HTTPError as exc:
        # Bytes were sent without a clean response: the outcome is unknown, so a
        # mutating caller must retry with the same idempotency key rather than
        # assume failure.
        raise HTTPException(
            status.HTTP_504_GATEWAY_TIMEOUT,
            "sandbox owner response ambiguous; retry with the same idempotency key",
        ) from exc
    return StreamingResponse(
        resp.aiter_raw(),
        status_code=resp.status_code,
        headers=_filter_resp_headers(resp.headers),
        background=BackgroundTask(resp.aclose),
    )



def _sandbox_id_in_path(path: str) -> str | None:
    match = _SANDBOX_PATH_RE.match(path)
    if match is None:
        return None
    sid = match.group(1)
    return None if sid in _RESERVED_SANDBOX_SEGMENTS else sid


def _engine_has(supervisor: Supervisor, sandbox_id: str) -> bool:
    try:
        supervisor._engine.get(sandbox_id)
    except EngineError:
        return False
    return True


async def _scatter_locate(
    mesh: Mesh, sandbox_id: str, record_store: Any | None = None
) -> str | None:
    for peer in mesh.peers():
        try:
            response = await asyncio.to_thread(
                mesh.transport.get, peer.advertise, f"/v1/mesh/locate/{sandbox_id}"
            )
        except Exception:
            continue
        owner = response.get("owner")
        if isinstance(owner, str) and owner:
            mesh.record_owner(sandbox_id, owner, int(response.get("epoch") or 0))
            return owner
    record = record_store.get(sandbox_id) if record_store is not None else None
    if record is not None:
        mesh.record_owner(sandbox_id, record.owner, record.epoch)
        return record.owner
    return None


def _require_bearer(
    request: Request, expected_token: str | None, client_token: str | None = None
) -> None:
    if not _bearer_token_authorized(_request_bearer_token(request), expected_token, client_token):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="unauthorized",
            headers={"WWW-Authenticate": "Bearer"},
        )


def _mesh_http_status(exc: MeshError) -> int:
    return {
        "unauthorized": status.HTTP_401_UNAUTHORIZED,
        "unreachable": status.HTTP_502_BAD_GATEWAY,
        "ambiguous": status.HTTP_504_GATEWAY_TIMEOUT,
        "conflict": status.HTTP_409_CONFLICT,
        "record_unreplicated": status.HTTP_503_SERVICE_UNAVAILABLE,
        "unplaceable": status.HTTP_409_CONFLICT,
        "arch_required": status.HTTP_422_UNPROCESSABLE_CONTENT,
    }.get(exc.code, status.HTTP_400_BAD_REQUEST)


def coded_http(status_code: int, code: str, message: str) -> HTTPException:
    """Build an HTTPException whose detail carries a machine-readable code."""
    return HTTPException(status_code, detail={"code": code, "message": message})


def mesh_http_exception(exc: MeshError) -> HTTPException:
    """Map a MeshError to a coded HTTPException (status from the error code)."""
    return coded_http(_mesh_http_status(exc), exc.code, exc.message)


def _split_http_authority(url: str) -> tuple[str, int, bool]:
    parsed = urlsplit(url)
    if parsed.scheme and parsed.scheme not in {"http", "https"}:
        raise MeshError("unsupported peer URL scheme", code="invalid")
    if not parsed.hostname:
        raise MeshError("invalid peer URL", code="invalid")
    tls = parsed.scheme == "https"
    return parsed.hostname, parsed.port or (443 if tls else 80), tls


def _ws_build_path(rest: str, websocket: WebSocket, *, preserve_query: bool = False) -> str:
    path = rest if preserve_query and rest.startswith("/") else "/" + rest.lstrip("/")
    if preserve_query:
        query = websocket.url.query
    else:
        query = _target_query(list(websocket.query_params.multi_items()))
    return path + (f"?{query}" if query else "")


async def _proxy_websocket(
    websocket: WebSocket,
    target: tuple[str, int] | tuple[str, int, bool],
    rest: str,
    *,
    extra_headers: dict[str, str] | None = None,
    preserve_query: bool = False,
) -> None:
    # Minimal RFC 6455 client relay: FastAPI decodes the public WebSocket, then
    # this bridge re-encodes frames to the sandbox's localhost tunnel.
    if len(target) == 2:
        host, port = target
        tls = False
    else:
        host, port, tls = target
    try:
        if tls:
            reader, writer = await asyncio.open_connection(
                host,
                port,
                ssl=ssl.create_default_context(),
                server_hostname=host,
            )
        else:
            reader, writer = await asyncio.open_connection(host, port)
    except OSError:
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="tunnel connect failed")
        return
    key = base64.b64encode(os.urandom(16)).decode()
    path = _ws_build_path(rest, websocket, preserve_query=preserve_query)
    request_headers = [
        f"GET {path} HTTP/1.1",
        f"Host: {host}:{port}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        "Sec-WebSocket-Version: 13",
    ]
    for header, value in (extra_headers or {}).items():
        request_headers.append(f"{header}: {value}")
    request = "\r\n".join(request_headers) + "\r\n\r\n"
    try:
        writer.write(request.encode("latin-1"))
        await writer.drain()
        response = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=10.0)
    except OSError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, TimeoutError:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="websocket upgrade failed")
        return
    expected = base64.b64encode(
        hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
    ).decode()
    if b" 101 " not in response.split(b"\r\n", 1)[0]:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="websocket upgrade failed")
        return
    if expected.encode() not in response:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        await websocket.close(
            code=status.WS_1011_INTERNAL_ERROR, reason="websocket accept mismatch"
        )
        return
    await websocket.accept()

    async def _client_to_guest() -> None:
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                writer.write(encode_frame(8))
                await writer.drain()
                return
            if message.get("bytes") is not None:
                writer.write(encode_frame(2, bytes(message["bytes"])))
                await writer.drain()
            elif message.get("text") is not None:
                writer.write(encode_frame(1, str(message["text"]).encode()))
                await writer.drain()

    async def _guest_to_client() -> None:
        while True:
            opcode, payload = await read_frame(reader)
            if opcode == 1:
                await websocket.send_text(payload.decode("utf-8", "replace"))
            elif opcode == 2:
                await websocket.send_bytes(payload)
            elif opcode == 8:
                await websocket.close()
                return
            elif opcode == 9:
                writer.write(encode_frame(10, payload))
                await writer.drain()

    tasks = {asyncio.create_task(_client_to_guest()), asyncio.create_task(_guest_to_client())}
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        with contextlib.suppress(Exception):
            task.result()
    writer.close()


def register_proxy_middleware(app: Any, ctx: ServerRuntime, require_auth: Any) -> None:
    supervisor = ctx.supervisor
    mesh = ctx.mesh
    expected_token = ctx.expected_token
    outbound_token = ctx.outbound_token or ""
    client_token = ctx.client_token
    record_store = ctx.record_store

    async def _mesh_router(request: Request, call_next: Any) -> Response:
        if _is_admin_path(request.url.path) and _client_token_only(
            _request_bearer_token(request), expected_token, client_token
        ):
            return JSONResponse({"detail": "forbidden"}, status_code=status.HTTP_403_FORBIDDEN)
        if not mesh.enabled or request.headers.get("x-vmon-mesh-hop"):
            return await call_next(request)
        sandbox_id = _sandbox_id_in_path(request.url.path)
        if sandbox_id is None or _engine_has(supervisor, sandbox_id):
            return await call_next(request)
        owner = mesh.owner_of(sandbox_id) or await _scatter_locate(mesh, sandbox_id, record_store)
        if owner is None:
            return await call_next(request)
        if owner == mesh.node_id:
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "sandbox owner unreachable")
        peer_url = mesh.peer_url(owner)
        if peer_url is None:
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "sandbox owner unreachable")
        _require_bearer(request, expected_token, client_token)
        return await _proxy_to_peer(request, peer_url, outbound_token)
    app.middleware("http")(_mesh_router)

    async def proxy_ws_owner(websocket: WebSocket, sandbox_id: str) -> bool:
        if not mesh.enabled or websocket.headers.get("x-vmon-mesh-hop"):
            return False
        if _engine_has(supervisor, sandbox_id):
            return False
        owner = mesh.owner_of(sandbox_id) or await _scatter_locate(mesh, sandbox_id, record_store)
        if owner is None:
            return False
        if owner == mesh.node_id:
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="owner unreachable")
            return True
        url = mesh.peer_url(owner)
        if url is None:
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="owner unreachable")
            return True
        try:
            target = _split_http_authority(url)
        except MeshError as exc:
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason=exc.message)
            return True
        await _proxy_websocket(
            websocket,
            target,
            websocket.url.path,
            extra_headers={
                "Authorization": f"Bearer {outbound_token}",
                "X-Vmon-Mesh-Hop": "1",
            },
            preserve_query=True,
        )
        return True

    async def events_stream(request: Request):
        subscriber = supervisor.subscribe_events()
        try:
            while True:
                if await request.is_disconnected():
                    return
                try:
                    payload = await asyncio.wait_for(subscriber.get(), timeout=15.0)
                except TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                yield _sse(payload)
        finally:
            supervisor.unsubscribe_events(subscriber)


async def proxy_ws_owner(ctx: ServerRuntime, websocket: WebSocket, sandbox_id: str) -> bool:
    supervisor = ctx.supervisor
    mesh = ctx.mesh
    outbound_token = ctx.outbound_token
    record_store = ctx.record_store

    if _engine_has(supervisor, sandbox_id):
        return False
    owner = mesh.owner_of(sandbox_id) or await _scatter_locate(mesh, sandbox_id, record_store)
    if owner is None:
        return False
    if owner == mesh.node_id:
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="owner unreachable")
        return True
    peer_url = mesh.peer_url(owner)
    if peer_url is None:
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="owner unreachable")
        return True
    try:
        target = _split_http_authority(peer_url)
    except MeshError as exc:
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason=exc.message)
        return True
    await _proxy_websocket(
        websocket,
        target,
        websocket.url.path,
        extra_headers={
            "Authorization": f"Bearer {outbound_token}",
            "X-Vmon-Mesh-Hop": "1",
        },
        preserve_query=True,
    )
    return True


async def events_stream(ctx: ServerRuntime, request: Request):
    subscriber = ctx.supervisor.subscribe_events()
    try:
        while True:
            if await request.is_disconnected():
                return
            try:
                payload = await asyncio.wait_for(subscriber.get(), timeout=15.0)
            except TimeoutError:
                yield ": keepalive\n\n"
                continue
            yield _sse(payload)
    finally:
        ctx.supervisor.unsubscribe_events(subscriber)
