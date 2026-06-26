"""Small RFC 6455 frame helpers shared by server and gateway CLI."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import socket
from collections.abc import Mapping


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


def read_frame_sync(sock: socket.socket) -> tuple[int, bytes]:
    """Read one WebSocket frame from a blocking socket."""
    head = _recv_exact(sock, 2)
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
    return opcode, payload


def client_handshake(
    sock: socket.socket, host: str, port: int, path: str, headers: Mapping[str, str]
) -> None:
    """Send a client WebSocket upgrade request and validate the 101 response."""
    key = base64.b64encode(os.urandom(16)).decode()
    lines = [
        f"GET {path} HTTP/1.1",
        f"Host: {host}:{port}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        "Sec-WebSocket-Version: 13",
    ]
    lines.extend(f"{name}: {value}" for name, value in headers.items())
    sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("latin-1"))
    response = b""
    while b"\r\n\r\n" not in response:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("websocket upgrade failed")
        response += chunk
    expected = base64.b64encode(
        hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
    ).decode()
    if b" 101 " not in response.split(b"\r\n", 1)[0] or expected.encode() not in response:
        raise ConnectionError("websocket upgrade failed")
