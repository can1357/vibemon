"""Small RFC 6455 frame helpers shared by server and gateway CLI."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import socket
from collections.abc import Mapping


class WebSocketHandshakeError(ConnectionError):
    """An HTTP error returned while upgrading a WebSocket connection."""

    def __init__(self, status: int, body: bytes, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.body = body


def _read_upgrade_response(sock: socket.socket) -> tuple[int, dict[str, str], bytes]:
    response = bytearray()
    while not response.endswith(b"\r\n\r\n"):
        chunk = sock.recv(1)
        if not chunk:
            raise WebSocketHandshakeError(0, b"", "websocket upgrade response was truncated")
        response.extend(chunk)
        if len(response) > 64 * 1024:
            raise WebSocketHandshakeError(0, b"", "websocket upgrade headers are too large")

    lines = bytes(response[:-4]).split(b"\r\n")
    try:
        status = int(lines[0].split(b" ", 2)[1])
    except (IndexError, ValueError) as exc:
        raise WebSocketHandshakeError(0, b"", "invalid websocket upgrade status") from exc

    headers: dict[str, str] = {}
    for line in lines[1:]:
        name, separator, value = line.partition(b":")
        if not separator:
            raise WebSocketHandshakeError(status, b"", "invalid websocket upgrade header")
        headers[name.decode("latin-1").strip().lower()] = value.decode("latin-1").strip()

    raw_length = headers.get("content-length", "0")
    try:
        length = int(raw_length)
    except ValueError as exc:
        raise WebSocketHandshakeError(status, b"", "invalid websocket response length") from exc
    if length < 0:
        raise WebSocketHandshakeError(status, b"", "invalid websocket response length")
    return status, headers, _recv_exact(sock, length) if length else b""


def encode_frame(opcode: int, payload: bytes = b"") -> bytes:
    """Encode a masked client-to-server WebSocket frame."""
    first = 0x80 | (opcode & 0x0F)
    mask_key = os.urandom(4)
    length = len(payload)
    if length < 126:
        header = bytes([first, 0x80 | length])
    elif length <= 0xFFFF:
        header = bytes([first, 0x80 | 126]) + length.to_bytes(2, "big")
    else:
        header = bytes([first, 0x80 | 127]) + length.to_bytes(8, "big")
    masked = bytes(byte ^ mask_key[i % 4] for i, byte in enumerate(payload))
    return header + mask_key + masked


async def read_frame(reader: asyncio.StreamReader) -> tuple[int, bytes]:
    """Read one WebSocket frame from an asyncio stream."""
    head = await reader.readexactly(2)
    opcode = head[0] & 0x0F
    masked = bool(head[1] & 0x80)
    length = head[1] & 0x7F
    if length == 126:
        length = int.from_bytes(await reader.readexactly(2), "big")
    elif length == 127:
        length = int.from_bytes(await reader.readexactly(8), "big")
    mask = await reader.readexactly(4) if masked else b""
    payload = await reader.readexactly(length) if length else b""
    if masked:
        payload = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
    return opcode, payload


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("websocket closed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_frame_sync_details(sock: socket.socket) -> tuple[bool, int, bytes]:
    """Read one blocking WebSocket frame, including its final-fragment flag."""
    head = _recv_exact(sock, 2)
    final = bool(head[0] & 0x80)
    opcode = head[0] & 0x0F
    masked = bool(head[1] & 0x80)
    length = head[1] & 0x7F
    if length == 126:
        length = int.from_bytes(_recv_exact(sock, 2), "big")
    elif length == 127:
        length = int.from_bytes(_recv_exact(sock, 8), "big")
    mask = _recv_exact(sock, 4) if masked else b""
    payload = _recv_exact(sock, length) if length else b""
    if masked:
        payload = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
    return final, opcode, payload


def read_frame_sync(sock: socket.socket) -> tuple[int, bytes]:
    """Read one WebSocket frame from a blocking socket."""
    _, opcode, payload = read_frame_sync_details(sock)
    return opcode, payload


def client_handshake(
    sock: socket.socket, host: str, port: int, path: str, headers: Mapping[str, str]
) -> None:
    """Send a client WebSocket upgrade request and validate the 101 response."""
    key = base64.b64encode(os.urandom(16)).decode()
    authority = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
    lines = [
        f"GET {path} HTTP/1.1",
        f"Host: {authority}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        "Sec-WebSocket-Version: 13",
    ]
    lines.extend(f"{name}: {value}" for name, value in headers.items())
    sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("latin-1"))

    status, response_headers, body = _read_upgrade_response(sock)
    if status != 101:
        raise WebSocketHandshakeError(status, body, f"websocket upgrade returned HTTP {status}")

    expected = base64.b64encode(
        hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
    ).decode()
    connection = {
        token.strip().lower() for token in response_headers.get("connection", "").split(",")
    }
    if (
        response_headers.get("upgrade", "").lower() != "websocket"
        or "upgrade" not in connection
        or response_headers.get("sec-websocket-accept") != expected
    ):
        raise WebSocketHandshakeError(status, body, "invalid websocket upgrade response")
