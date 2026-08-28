from __future__ import annotations

import asyncio
import queue
import socket
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any

import pytest

import vmon.host_gateway as host_gateway_module
from vmon import HostGateway, ProtocolError
from vmon.host_gateway import parse_host_gateway_target
from vmon.sandbox import Sandbox
from vmon.v1 import api_pb2

_END = object()


class _Responses:
    def __init__(self, *frames: api_pb2.HostGatewayOutput) -> None:
        self._frames: queue.Queue[api_pb2.HostGatewayOutput | object] = queue.Queue()
        for frame in frames:
            self._frames.put(frame)
        self.cancelled = False

    def push(self, frame: api_pb2.HostGatewayOutput) -> None:
        self._frames.put(frame)

    def __iter__(self) -> _Responses:
        return self

    def __next__(self) -> api_pb2.HostGatewayOutput:
        frame = self._frames.get()
        if frame is _END:
            raise StopIteration
        assert isinstance(frame, api_pb2.HostGatewayOutput)
        return frame

    def cancel(self) -> None:
        if not self.cancelled:
            self.cancelled = True
            self._frames.put(_END)


class _SandboxService:
    def __init__(self, responses: _Responses) -> None:
        self.responses = responses
        self.inputs: Any = None

    def HostGateway(self, inputs: Any) -> _Responses:
        self.inputs = inputs
        return self.responses


class _Driver:
    def __init__(self, service: _SandboxService) -> None:
        self.service = service

    def call(self, operation: Any, *, endpoint: str | None = None, stream: bool = False):
        assert stream is True
        return operation(SimpleNamespace(sandbox=self.service)), endpoint or "owner"

    def endpoints(self) -> list[Any]:
        return []


def _sandbox(responses: _Responses) -> tuple[Sandbox, _SandboxService]:
    service = _SandboxService(responses)
    client = SimpleNamespace(driver=_Driver(service))
    return Sandbox(client, {"id": "box"}), service


def _next_input(service: _SandboxService) -> api_pb2.HostGatewayInput:
    with ThreadPoolExecutor(max_workers=1) as executor:
        return executor.submit(next, service.inputs).result(timeout=2)


def _ready(url: str = "http://192.0.2.1:1025") -> api_pb2.HostGatewayOutput:
    return api_pb2.HostGatewayOutput(ready={"url": url})


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        ("localhost:8080", ("localhost", 8080)),
        (":8080", ("127.0.0.1", 8080)),
        ("http://example.test", ("example.test", 80)),
        ("https://[::1]", ("::1", 443)),
        ("[::1]:8443", ("::1", 8443)),
    ],
)
def test_gateway_target_syntax(target: str, expected: tuple[str, int]) -> None:
    assert parse_host_gateway_target(target) == expected


@pytest.mark.parametrize(
    "target",
    ["localhost", "ftp://localhost:21", "http://localhost/path", "[::1", ":0"],
)
def test_gateway_target_rejects_invalid_values(target: str) -> None:
    with pytest.raises(ValueError):
        parse_host_gateway_target(target)


def test_ready_handshake_and_bidirectional_relay() -> None:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.settimeout(2)
    responses = _Responses(_ready())
    sandbox, service = _sandbox(responses)

    with sandbox.host_gateway(f":{listener.getsockname()[1]}") as gateway:
        assert isinstance(gateway, HostGateway)
        assert gateway.url == "http://192.0.2.1:1025"
        attach = _next_input(service)
        assert attach.WhichOneof("input") == "attach"
        assert attach.attach.sandbox_id == "box"

        responses.push(api_pb2.HostGatewayOutput(open={"conn": 7}))
        target, _ = listener.accept()
        target.settimeout(2)
        responses.push(api_pb2.HostGatewayOutput(data={"conn": 7, "data": b"from-guest"}))
        assert target.recv(64) == b"from-guest"

        target.sendall(b"from-runner")
        outbound = _next_input(service)
        assert outbound.WhichOneof("input") == "data"
        assert outbound.data.conn == 7
        assert outbound.data.data == b"from-runner"

        responses.push(api_pb2.HostGatewayOutput(close={"conn": 7}))
        assert target.recv(1) == b""
        target.close()

    assert responses.cancelled is True
    listener.close()


def test_target_dial_failure_sends_close(monkeypatch) -> None:
    def fail_dial(*args: Any, **kwargs: Any) -> socket.socket:
        raise OSError("dial failed")

    monkeypatch.setattr(host_gateway_module.socket, "create_connection", fail_dial)
    responses = _Responses(_ready())
    sandbox, service = _sandbox(responses)

    gateway = sandbox.host_gateway(":1")
    _next_input(service)
    responses.push(api_pb2.HostGatewayOutput(open={"conn": 11}))
    outbound = _next_input(service)
    assert outbound.WhichOneof("input") == "close"
    assert outbound.close.conn == 11
    gateway.close()


def test_server_close_echo_after_local_close_is_idempotent() -> None:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.settimeout(2)
    responses = _Responses(_ready())
    sandbox, service = _sandbox(responses)
    gateway = sandbox.host_gateway(f":{listener.getsockname()[1]}")
    _next_input(service)

    responses.push(api_pb2.HostGatewayOutput(open={"conn": 7}))
    target, _ = listener.accept()
    target.close()
    outbound = _next_input(service)
    assert outbound.WhichOneof("input") == "close"
    assert outbound.close.conn == 7

    responses.push(api_pb2.HostGatewayOutput(close={"conn": 7}))
    responses.push(api_pb2.HostGatewayOutput(data={"conn": 99, "data": b"bad"}))
    with pytest.raises(ProtocolError, match="unknown connection 99"):
        gateway.wait(timeout=2)
    listener.close()


def test_malformed_frame_order_fails_and_cancels_rpc() -> None:
    responses = _Responses(_ready())
    sandbox, service = _sandbox(responses)
    gateway = sandbox.host_gateway(":80")
    _next_input(service)

    responses.push(api_pb2.HostGatewayOutput(data={"conn": 99, "data": b"bad"}))
    with pytest.raises(ProtocolError, match="unknown connection 99"):
        gateway.wait(timeout=2)
    assert responses.cancelled is True


def test_bad_ready_frame_cancels_rpc_before_starting_relay() -> None:
    responses = _Responses(api_pb2.HostGatewayOutput(open={"conn": 1}))
    sandbox, _ = _sandbox(responses)

    with pytest.raises(ProtocolError, match="ready frame"):
        sandbox.host_gateway(":80")
    assert responses.cancelled is True


def test_async_facade_returns_ready_gateway() -> None:
    async def scenario() -> None:
        responses = _Responses(_ready("http://192.0.2.2:1025"))
        sandbox, service = _sandbox(responses)
        gateway = await sandbox.aio.host_gateway(":80")
        assert gateway.url == "http://192.0.2.2:1025"
        assert _next_input(service).attach.sandbox_id == "box"
        gateway.close()

    asyncio.run(scenario())
