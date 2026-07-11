"""Streaming process, console, event, and log objects."""

from __future__ import annotations

import base64
import contextlib
import json
import queue
import threading
from collections.abc import Iterable, Iterator, Mapping
from typing import TYPE_CHECKING, Any

from ._endpoint import ResponseStream, WebSocketConnection
from .errors import APIError, ProtocolError, TransportError

if TYPE_CHECKING:
    from .models import ExecExit

_EOF = object()


class ByteStream(Iterable[bytes]):
    """Thread-safe byte iterator used for process output streams."""

    def __init__(self) -> None:
        self._queue: queue.Queue[bytes | object] = queue.Queue()
        self._buffer = bytearray()
        self._closed = False
        self._eof = False

    def feed(self, data: bytes) -> None:
        if data:
            self._queue.put(bytes(data))

    def close(self) -> None:
        """Mark the stream complete and wake blocked readers."""
        if not self._closed:
            self._closed = True
            self._queue.put(_EOF)

    def __iter__(self) -> ByteStream:
        return self

    def __next__(self) -> bytes:
        if self._buffer:
            data = bytes(self._buffer)
            self._buffer.clear()
            return data
        if self._eof:
            raise StopIteration
        item = self._queue.get()
        if item is _EOF:
            self._eof = True
            raise StopIteration
        if not isinstance(item, bytes):
            raise ProtocolError("process stream received an invalid chunk")
        return item

    def read_chunk(self, timeout: float | None = None) -> bytes | None:
        """Return the next available chunk, or ``None`` at end-of-stream.

        Raises ``TimeoutError`` when no chunk arrives within ``timeout`` seconds.
        """
        if self._buffer:
            data = bytes(self._buffer)
            self._buffer.clear()
            return data
        if self._eof:
            return None
        try:
            item = self._queue.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError("timed out waiting for process output") from None
        if item is _EOF:
            self._eof = True
            return None
        if not isinstance(item, bytes):
            raise ProtocolError("process stream received an invalid chunk")
        return item

    def read(self, size: int = -1) -> bytes:
        """Read up to ``size`` bytes, or all bytes through end-of-stream."""
        if size == 0:
            return b""
        if size > 0:
            while len(self._buffer) < size and not self._eof:
                item = self._queue.get()
                if item is _EOF:
                    self._eof = True
                    break
                if not isinstance(item, bytes):
                    raise ProtocolError("process stream received an invalid chunk")
                self._buffer.extend(item)
            data = bytes(self._buffer[:size])
            del self._buffer[:size]
            return data

        chunks: list[bytes] = []
        if self._buffer:
            chunks.append(bytes(self._buffer))
            self._buffer.clear()
        while not self._eof:
            item = self._queue.get()
            if item is _EOF:
                self._eof = True
                break
            if not isinstance(item, bytes):
                raise ProtocolError("process stream received an invalid chunk")
            chunks.append(item)
        return b"".join(chunks)


class _ProcessStdin:
    def __init__(self, session: _ProcessSession) -> None:
        self._session = session
        self._closed = False

    def write(self, data: bytes | bytearray | memoryview | str) -> None:
        """Write bytes or UTF-8 text to the process standard input."""
        self._session.write_stdin(data)

    def close(self) -> None:
        """Send end-of-file once."""
        if not self._closed:
            try:
                self._session.close_stdin()
            finally:
                self._closed = True


class _ProcessSession:
    def __init__(
        self,
        websocket: WebSocketConnection,
        request: Mapping[str, Any],
        *,
        timeout: float | None,
        tty: bool,
        consume_ready: bool,
        thread_name: str,
    ) -> None:
        self.stdout = ByteStream()
        self.stderr = ByteStream()
        self._websocket = websocket
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
        try:
            websocket.send_json(dict(request))
            if consume_ready:
                self._consume_ready()
        except BaseException:
            websocket.close()
            raise
        self._reader = threading.Thread(target=self._read_loop, name=thread_name, daemon=True)
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

    def _consume_ready(self) -> None:
        frame = self._websocket.recv_json()
        if frame is None:
            raise ProtocolError("shell closed before its ready frame")
        error = frame.get("error")
        if isinstance(error, Mapping):
            raise APIError(
                str(error.get("message") or "shell setup failed"),
                code=str(error.get("code") or "engine"),
            )
        ready = frame.get("ready")
        if not isinstance(ready, str) or not ready:
            raise ProtocolError("shell ready frame omitted the sandbox id")
        self._ready_name = ready

    def write_stdin(self, data: bytes | bytearray | memoryview | str) -> None:
        raw = data.encode() if isinstance(data, str) else bytes(data)
        if not raw:
            return
        with self._send_lock:
            if self._stdin_closed:
                raise TransportError("process stdin is closed")
            self._websocket.send_json({"stdin_b64": base64.b64encode(raw).decode("ascii")})

    def close_stdin(self) -> None:
        with self._send_lock:
            if self._stdin_closed:
                return
            self._stdin_closed = True
            self._websocket.send_json({"eof": True})

    def resize(self, rows: int, cols: int) -> None:
        if rows <= 0 or cols <= 0:
            raise ValueError("terminal rows and columns must be positive")
        self._websocket.send_json({"resize": [int(rows), int(cols)]})

    def kill(self, signal: int = 15) -> None:
        self._closing = True
        self._signal = int(signal)
        self._websocket.close()

    def wait(self, timeout: float | None = None) -> ExecExit:
        if not self._tty and not self._stdin_closed:
            with contextlib.suppress(Exception):
                self.close_stdin()
        effective = self._timeout if timeout is None else timeout
        if not self._exit.wait(effective):
            self.kill()
            raise TimeoutError("process timed out")
        if self._error is not None:
            raise self._error
        from .models import ExecExit

        return ExecExit(
            code=self._returncode if self._returncode is not None else -1,
            signal=self._signal,
        )

    def _read_loop(self) -> None:
        try:
            while True:
                frame = self._websocket.recv_json()
                if frame is None:
                    if self._returncode is None:
                        self._error = ProtocolError("process stream closed before an exit frame")
                    return
                error = frame.get("error")
                if isinstance(error, Mapping):
                    self._error = APIError(
                        str(error.get("message") or "exec failed"),
                        code=str(error.get("code") or "engine"),
                    )
                    return
                stream = frame.get("stream")
                if stream in {"stdout", "stderr", "console"}:
                    encoded = frame.get("b64")
                    if not isinstance(encoded, str):
                        self._error = ProtocolError("invalid exec stream frame")
                        return
                    try:
                        data = base64.b64decode(encoded, validate=True)
                    except ValueError, TypeError:
                        self._error = ProtocolError("invalid exec stream payload")
                        return
                    (self.stderr if stream == "stderr" else self.stdout).feed(data)
                    continue
                if "exit" in frame:
                    raw_exit = frame.get("exit")
                    if isinstance(raw_exit, bool) or not isinstance(raw_exit, int):
                        self._error = ProtocolError("invalid process exit frame")
                        return
                    raw_signal = frame.get("signal")
                    if raw_signal is not None and (
                        isinstance(raw_signal, bool) or not isinstance(raw_signal, int)
                    ):
                        self._error = ProtocolError("invalid process signal")
                        return
                    self._returncode = raw_exit
                    self._signal = raw_signal
                    return
                if "ready" in frame:
                    self._error = ProtocolError("unexpected process ready frame")
                    return
                self._error = ProtocolError("unknown process frame")
                return
        except BaseException as exc:
            if not self._closing:
                self._error = exc
        finally:
            self.stdout.close()
            self.stderr.close()
            self._exit.set()
            self._websocket.close()


class Process:
    """A streaming command or shell with stdin, output streams, and an exit result."""

    def __init__(self, session: _ProcessSession) -> None:
        self._session = session
        self.stdout = session.stdout
        self.stderr = session.stderr
        self.stdin = _ProcessStdin(session)

    @property
    def returncode(self) -> int | None:
        """Return the exit code after completion, otherwise ``None``."""
        return self._session.returncode

    @property
    def sandbox_id(self) -> str | None:
        """Return the ephemeral shell sandbox id announced by the server."""
        return self._session.ready_name

    def resize(self, rows: int, cols: int) -> None:
        """Resize the remote pseudo-terminal."""
        self._session.resize(rows, cols)

    def wait(self, timeout: float | None = None) -> ExecExit:
        """Wait for completion and return both exit code and signal."""
        try:
            return self._session.wait(timeout)
        finally:
            self._close_streams()

    def close(self) -> None:
        """Close this process, terminating a command that is still running."""
        if self.returncode is None:
            self._session.kill()
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
    """A closeable byte stream from a sandbox console attachment."""

    def __init__(self, websocket: WebSocketConnection) -> None:
        self._websocket = websocket
        self._stream = ByteStream()
        self._done = threading.Event()
        self._error: BaseException | None = None
        self._closing = False
        self._reader = threading.Thread(
            target=self._read_loop,
            name="vmon-console-attach",
            daemon=True,
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
        """Read console bytes through completion or the requested size."""
        data = self._stream.read(size)
        if size < 0:
            self.wait()
        return data

    def wait(self, timeout: float | None = None) -> None:
        """Wait for completion and raise a structured stream error."""
        if not self._done.wait(timeout):
            raise TimeoutError("console attachment timed out")
        if self._error is not None:
            raise self._error

    def close(self) -> None:
        """Close the console attachment idempotently."""
        self._closing = True
        self._websocket.close()
        self._stream.close()

    def _read_loop(self) -> None:
        try:
            while True:
                frame = self._websocket.recv_json()
                if frame is None:
                    return
                error = frame.get("error")
                if isinstance(error, Mapping):
                    self._error = APIError(
                        str(error.get("message") or "console attachment failed"),
                        code=str(error.get("code") or "engine"),
                    )
                    return
                if frame.get("stream") != "console":
                    self._error = ProtocolError("unknown console frame")
                    return
                encoded = frame.get("b64")
                if not isinstance(encoded, str):
                    self._error = ProtocolError("invalid console stream frame")
                    return
                try:
                    self._stream.feed(base64.b64decode(encoded, validate=True))
                except ValueError, TypeError:
                    self._error = ProtocolError("invalid console stream payload")
                    return
        except BaseException as exc:
            if not self._closing:
                self._error = exc
        finally:
            self._stream.close()
            self._done.set()
            self._websocket.close()

    def __enter__(self) -> ConsoleStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class EventStream(Iterable[dict[str, Any]]):
    """A closeable iterator over JSON objects from an SSE endpoint."""

    def __init__(self, response: ResponseStream) -> None:
        self._response = response
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
            raise ProtocolError("vmon event stream returned invalid JSON") from exc
        if not isinstance(value, dict):
            raise ProtocolError("vmon event stream returned a non-object event")
        return value

    def close(self) -> None:
        """Close the streaming response idempotently."""
        if not self._closed:
            self._closed = True
            self._response.close()

    def __enter__(self) -> EventStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class LogStream(Iterable[str]):
    """A closeable iterator over decoded sandbox log chunks."""

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
                    raise ProtocolError("invalid sandbox log stream payload") from None

    def close(self) -> None:
        """Close the underlying event stream."""
        self._events.close()

    def __enter__(self) -> LogStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def open_process(
    websocket: WebSocketConnection,
    request: Mapping[str, Any],
    *,
    timeout: float | None = None,
    tty: bool = False,
    consume_ready: bool = False,
    thread_name: str = "vmon-process",
) -> Process:
    return Process(
        _ProcessSession(
            websocket,
            request,
            timeout=timeout,
            tty=tty,
            consume_ready=consume_ready,
            thread_name=thread_name,
        )
    )
