"""Client-served TCP gateway relay for sandboxes."""

from __future__ import annotations

import contextlib
import queue
import socket
import threading
from collections.abc import Callable, Iterator
from typing import Any

import grpc

from ._endpoint import translate_rpc_error
from .errors import ProtocolError
from .v1 import api_pb2

_INPUT_DONE = object()
_CONNECTION_CLOSE = object()


def parse_host_gateway_target(target: str) -> tuple[str, int]:
    """Parse the same runner target syntax accepted by ``vmon gateway --to``."""
    if not isinstance(target, str):
        raise TypeError("gateway target must be a string")
    if target.startswith("http://"):
        authority, default_port = target[7:], 80
    elif target.startswith("https://"):
        authority, default_port = target[8:], 443
    elif "://" in target:
        raise ValueError("gateway target URL must use http or https")
    else:
        authority, default_port = target, None

    if any(part in authority for part in "/?#"):
        raise ValueError("gateway target URL must not contain a path, query, or fragment")

    if authority.startswith("["):
        end = authority.find("]")
        if end < 0:
            raise ValueError("gateway target has an invalid IPv6 address")
        host = authority[1:end]
        suffix = authority[end + 1 :]
        if not suffix:
            if default_port is None:
                raise ValueError("gateway target requires a port")
            port_text = str(default_port)
        elif suffix.startswith(":"):
            port_text = suffix[1:]
        else:
            raise ValueError("gateway target has an invalid port")
    elif ":" in authority:
        host, port_text = authority.rsplit(":", 1)
        if not host:
            host = "127.0.0.1"
    else:
        host = authority
        if default_port is None:
            raise ValueError("gateway target requires a port")
        port_text = str(default_port)

    if not port_text.isascii() or not port_text.isdigit():
        raise ValueError("gateway target has an invalid port")
    port = int(port_text, 10)
    if port > 65535:
        raise ValueError("gateway target has an invalid port")
    if not host or port == 0:
        raise ValueError("gateway target requires a non-empty host and nonzero port")
    return host, port


class _GatewayConnection:
    def __init__(self, gateway: HostGateway, conn: int) -> None:
        self._gateway = gateway
        self.conn = conn
        self._commands: queue.SimpleQueue[bytes | object] = queue.SimpleQueue()
        self._socket: socket.socket | None = None
        self._socket_lock = threading.Lock()
        self._send_close = True
        self._stopped = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"vmon-host-gateway-{conn}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def data(self, data: bytes) -> None:
        self._commands.put(data)

    def stop(self) -> None:
        self._send_close = False
        self._stopped.set()
        self._commands.put(_CONNECTION_CLOSE)
        with self._socket_lock:
            stream = self._socket
        if stream is not None:
            with contextlib.suppress(OSError):
                stream.shutdown(socket.SHUT_RDWR)
            with contextlib.suppress(OSError):
                stream.close()

    def join(self) -> None:
        if self._thread is not threading.current_thread():
            self._thread.join()

    def _run(self) -> None:
        notify_close = True
        try:
            if self._stopped.is_set():
                notify_close = False
                return
            try:
                stream = socket.create_connection(self._gateway._target, timeout=5.0)
            except OSError:
                return
            stream.settimeout(0.1)
            with self._socket_lock:
                if self._gateway._closing or self._stopped.is_set():
                    stream.close()
                    notify_close = False
                    return
                self._socket = stream

            while not self._gateway._closing:
                while True:
                    try:
                        command = self._commands.get_nowait()
                    except queue.Empty:
                        break
                    if command is _CONNECTION_CLOSE:
                        notify_close = False
                        return
                    if not isinstance(command, bytes):
                        return
                    try:
                        stream.sendall(command)
                    except OSError:
                        return
                try:
                    data = stream.recv(64 * 1024)
                except TimeoutError:
                    continue
                except OSError:
                    return
                if not data:
                    return
                self._gateway._send(
                    api_pb2.HostGatewayInput(data={"conn": self.conn, "data": data})
                )
        finally:
            with self._socket_lock:
                active_stream = self._socket
                self._socket = None
            if active_stream is not None:
                with contextlib.suppress(OSError):
                    active_stream.close()
            if notify_close and self._send_close and not self._gateway._closing:
                self._gateway._send(api_pb2.HostGatewayInput(close={"conn": self.conn}))
            self._gateway._connection_finished(self)


class HostGateway:
    """A live relay from a sandbox's private host gateway to a runner TCP target."""

    def __init__(
        self,
        starter: Callable[[Iterator[api_pb2.HostGatewayInput]], tuple[Any, str | None]],
        sandbox_id: str,
        target: str,
    ) -> None:
        self._target = parse_host_gateway_target(target)
        self._inputs: queue.SimpleQueue[Any] = queue.SimpleQueue()
        self._connections: dict[int, _GatewayConnection] = {}
        self._closed_connections: set[int] = set()
        self._connections_lock = threading.Lock()
        self._done = threading.Event()
        self._closing = False
        self._error: BaseException | None = None
        self._endpoint: str | None = None
        self._responses: Any = None
        self._inputs.put(
            api_pb2.HostGatewayInput(attach=api_pb2.HostGatewayAttach(sandbox_id=sandbox_id))
        )
        try:
            self._responses, self._endpoint = starter(self._make_inputs())
            first = next(iter(self._responses))
        except StopIteration:
            self._shutdown()
            raise ProtocolError("host gateway closed before its ready frame") from None
        except grpc.RpcError as exc:
            self._shutdown()
            raise translate_rpc_error(
                exc,
                endpoint=self._endpoint,
                fallback_message="host gateway setup failed",
            ) from exc
        except BaseException:
            self._shutdown()
            raise
        if first.WhichOneof("output") != "ready" or not first.ready.url:
            self._shutdown()
            raise ProtocolError("host gateway ready frame omitted its URL")
        self._url = first.ready.url
        self._reader = threading.Thread(
            target=self._read_loop,
            name=f"vmon-host-gateway-{sandbox_id}",
            daemon=True,
        )
        self._reader.start()

    @property
    def url(self) -> str:
        """Return the gateway URL reachable from inside the sandbox."""
        return self._url

    def _make_inputs(self) -> Iterator[api_pb2.HostGatewayInput]:
        while True:
            item = self._inputs.get()
            if item is _INPUT_DONE:
                return
            yield item

    def _send(self, frame: api_pb2.HostGatewayInput) -> None:
        if not self._closing:
            self._inputs.put(frame)

    def _read_loop(self) -> None:
        error: BaseException | None = None
        try:
            for frame in self._responses:
                kind = frame.WhichOneof("output")
                if kind == "open":
                    conn = frame.open.conn
                    with self._connections_lock:
                        if conn in self._connections or conn in self._closed_connections:
                            raise ProtocolError(f"host gateway opened duplicate connection {conn}")
                        connection = _GatewayConnection(self, conn)
                        self._connections[conn] = connection
                    connection.start()
                elif kind == "data":
                    connection = self._connection(frame.data.conn)
                    connection.data(bytes(frame.data.data))
                elif kind == "close":
                    closing_connection = self._server_close(frame.close.conn)
                    if closing_connection is not None:
                        closing_connection.stop()
                elif kind == "ready":
                    raise ProtocolError("host gateway received an unexpected ready frame")
                else:
                    raise ProtocolError("host gateway received an unknown frame")
            if not self._closing:
                error = ProtocolError("host gateway stream ended unexpectedly")
        except grpc.RpcError as exc:
            if not self._closing:
                error = translate_rpc_error(
                    exc,
                    endpoint=self._endpoint,
                    fallback_message="host gateway failed",
                )
        except BaseException as exc:
            if not self._closing:
                error = exc
        finally:
            self._finish(error)

    def _connection(self, conn: int) -> _GatewayConnection:
        with self._connections_lock:
            connection = self._connections.get(conn)
        if connection is None:
            raise ProtocolError(f"host gateway referenced unknown connection {conn}")
        return connection

    def _server_close(self, conn: int) -> _GatewayConnection | None:
        with self._connections_lock:
            connection = self._connections.pop(conn, None)
            if connection is not None:
                return connection
            if conn in self._closed_connections:
                self._closed_connections.remove(conn)
                return None
        raise ProtocolError(f"host gateway referenced unknown connection {conn}")

    def _connection_finished(self, connection: _GatewayConnection) -> None:
        with self._connections_lock:
            if self._connections.get(connection.conn) is connection:
                del self._connections[connection.conn]
                self._closed_connections.add(connection.conn)

    def _shutdown(self) -> None:
        self._closing = True
        self._inputs.put(_INPUT_DONE)
        responses = self._responses
        if responses is not None:
            with contextlib.suppress(Exception):
                responses.cancel()

    def _finish(self, error: BaseException | None) -> None:
        if error is not None and self._error is None:
            self._error = error
        self._shutdown()
        with self._connections_lock:
            connections = list(self._connections.values())
        for connection in connections:
            connection.stop()
        for connection in connections:
            connection.join()
        self._done.set()

    def wait(self, timeout: float | None = None) -> None:
        """Wait for the relay to end and raise any transport or protocol error."""
        if not self._done.wait(timeout):
            raise TimeoutError("host gateway did not close in time")
        if self._error is not None:
            raise self._error

    def close(self) -> None:
        """Cancel the RPC and close every target connection idempotently."""
        if not self._closing:
            self._finish(None)
        reader = getattr(self, "_reader", None)
        if reader is not None and reader is not threading.current_thread():
            reader.join()

    def __enter__(self) -> HostGateway:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
