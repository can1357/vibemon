"""Guest-agent client for vmon's framed virtio-console RPC."""

from __future__ import annotations

import json
import queue
import signal as _signal
import socket
import struct
import threading
from collections.abc import Iterator, Mapping, Sequence
from itertools import count
from os import PathLike
from typing import Any

MAX_PAYLOAD = 1 << 20

TYPE_REQ = 1
TYPE_STDIN = 2
TYPE_STDOUT = 3
TYPE_STDERR = 4
TYPE_EXIT = 5
TYPE_RESP = 6
TYPE_KILL = 7

_HEADER = struct.Struct("<IBI")
_EOF = object()
type _BytesLike = bytes | bytearray | memoryview


class AgentClosed(RuntimeError):
    """Raised when the guest-agent connection is no longer usable."""


class AgentError(RuntimeError):
    """Raised for an agent-side ``RESP {ok:false}`` result."""


class ProtocolError(RuntimeError):
    """Raised when a frame violates the GC4 wire contract."""


class _Pending:
    def __init__(self) -> None:
        self._event = threading.Event()
        self.result: Any = None
        self.error: BaseException | None = None

    def resolve(self, result: Any) -> None:
        self.result = result
        self._event.set()

    def reject(self, error: BaseException) -> None:
        self.error = error
        self._event.set()

    def wait(self, timeout: float | None = None) -> Any:
        if not self._event.wait(timeout):
            raise TimeoutError("agent request timed out")
        if self.error is not None:
            raise self.error
        return self.result


class ByteStream(Iterator[bytes]):
    """Thread-safe byte iterator used for stdout/stderr streams."""

    def __init__(self) -> None:
        self._q: queue.Queue[bytes | object] = queue.Queue()
        self._buf = bytearray()
        self._closed = False
        self._eof = False

    def feed(self, data: bytes) -> None:
        if data:
            self._q.put(bytes(data))

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._q.put(_EOF)

    def __iter__(self) -> ByteStream:
        return self

    def __next__(self) -> bytes:
        if self._buf:
            data = bytes(self._buf)
            self._buf.clear()
            return data
        if self._eof:
            raise StopIteration
        item = self._q.get()
        if item is _EOF:
            self._eof = True
            raise StopIteration
        return item  # type: ignore[return-value]

    def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b""
        if size is not None and size > 0:
            while len(self._buf) < size and not self._eof:
                item = self._q.get()
                if item is _EOF:
                    self._eof = True
                    break
                self._buf.extend(item)  # type: ignore[arg-type]
            out = bytes(self._buf[:size])
            del self._buf[:size]
            return out

        chunks: list[bytes] = []
        if self._buf:
            chunks.append(bytes(self._buf))
            self._buf.clear()
        while not self._eof:
            item = self._q.get()
            if item is _EOF:
                self._eof = True
                break
            chunks.append(item)  # type: ignore[arg-type]
        return b"".join(chunks)


class ExecSession:
    """A live guest exec session."""

    def __init__(self, conn: AgentConn, session_id: int, timeout: float | None = None) -> None:
        self._conn = conn
        self.id = session_id
        self.stdout = ByteStream()
        self.stderr = ByteStream()
        self._default_timeout = timeout
        self._exit = threading.Event()
        self._code: int | None = None
        self._signal: int | None = None
        self._wait_error: BaseException | None = None

    @property
    def returncode(self) -> int | None:
        return self._code

    @property
    def signal(self) -> int | None:
        return self._signal

    def write_stdin(self, data: _BytesLike | str) -> None:
        if isinstance(data, str):
            raw = data.encode()
        else:
            raw = bytes(data)
        for off in range(0, len(raw), MAX_PAYLOAD):
            self._conn._send_frame(TYPE_STDIN, self.id, raw[off : off + MAX_PAYLOAD])

    def close_stdin(self) -> None:
        self._conn._send_frame(TYPE_STDIN, self.id, b"")

    def resize(self, rows: int, cols: int) -> None:
        """Resize the pty for this session.

        Fire-and-forget: the agent acknowledges with a RESP for this session id
        which the reader tolerates without a pending entry, so nothing waits on
        it. Only meaningful for tty sessions (``exec(..., tty=True)``).
        """
        self._conn._send_json(
            TYPE_REQ, self.id, {"op": "resize", "rows": int(rows), "cols": int(cols)}
        )

    def kill(self, sig: int = _signal.SIGTERM) -> None:
        self._conn._send_json(TYPE_KILL, self.id, {"signal": int(sig)})

    def wait(self, timeout: float | None = None) -> int:
        limit = self._default_timeout if timeout is None else timeout
        if not self._exit.wait(limit):
            try:
                self.kill(_signal.SIGKILL)
            finally:
                raise TimeoutError("agent exec timed out")
        if self._wait_error is not None:
            raise self._wait_error
        return -1 if self._code is None else self._code

    def _set_exit(self, code: int, sig: int | None = None) -> None:
        if not self._exit.is_set():
            self._code = int(code)
            self._signal = None if sig is None else int(sig)
            self._exit.set()

    def _set_error(self, error: BaseException) -> None:
        self._wait_error = error
        self.stdout.close()
        self.stderr.close()
        self._set_exit(-1)

    def _disconnect(self) -> None:
        self.stdout.close()
        self.stderr.close()
        self._set_exit(-1)


class AgentConn:
    """Client for the GC4 guest-agent framed protocol over a Unix socket."""

    def __init__(
        self, sock_path: str | PathLike[str], connect_timeout: float | None = None
    ) -> None:
        self.sock_path = str(sock_path)
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if connect_timeout is not None:
            self._sock.settimeout(connect_timeout)
        self._sock.connect(self.sock_path)
        self._sock.settimeout(None)

        self._send_lock = threading.Lock()
        self._lock = threading.Lock()
        self._ids = count(1)
        self._closed = False
        self._pending: dict[int, _Pending] = {}
        self._sessions: dict[int, ExecSession] = {}
        self._downloads: dict[int, ByteStream] = {}

        self._reader_thread = threading.Thread(
            target=self._reader, name="vmon-agent-reader", daemon=True
        )
        self._reader_thread.start()

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def close(self) -> None:
        self._abort(AgentClosed("agent connection closed"))

    def request(self, op: str, timeout: float | None = None, **params: Any) -> dict[str, Any]:
        req_id = self._new_id()
        pending = _Pending()
        with self._lock:
            self._ensure_open_locked()
            self._pending[req_id] = pending
        try:
            self._send_json(TYPE_REQ, req_id, {"op": op, **params})
            result = pending.wait(timeout)
        except BaseException:
            with self._lock:
                self._pending.pop(req_id, None)
            raise
        if not isinstance(result, dict):
            raise ProtocolError("agent response payload is not a JSON object")
        if result.get("ok") is False:
            raise AgentError(str(result.get("error") or "agent request failed"))
        return result

    def ping(self, timeout: float | None = None) -> dict[str, Any]:
        return self.request("ping", timeout=timeout)

    def net_config(
        self,
        ip: str,
        prefix: int,
        gw: str,
        dns: Sequence[str] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        return self.request(
            "net_config", timeout=timeout, ip=ip, prefix=prefix, gw=gw, dns=dns or []
        )

    def mount(
        self,
        tag: str,
        path: str | PathLike[str],
        ro: bool = False,
        fstype: str = "virtiofs",
        timeout: float | None = None,
    ) -> dict[str, Any]:
        return self.request(
            "mount", timeout=timeout, tag=str(tag), path=str(path), fstype=str(fstype), ro=bool(ro)
        )

    def tcp_probe(self, port: int, host: str = "127.0.0.1", timeout: float | None = None) -> bool:
        r = self.request("tcp_probe", timeout=timeout, port=int(port), host=str(host))
        return bool(r.get("connected"))

    def exec(
        self,
        cmd: Sequence[str | PathLike[str]],
        cwd: str | PathLike[str] | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        tty: bool = False,
    ) -> ExecSession:
        if not cmd:
            raise ValueError("exec command must not be empty")
        req_id = self._new_id()
        session = ExecSession(self, req_id, timeout=timeout)
        payload: dict[str, Any] = {"op": "exec", "cmd": [str(c) for c in cmd], "tty": bool(tty)}
        if cwd is not None:
            payload["cwd"] = str(cwd)
        if env is not None:
            payload["env"] = {str(k): str(v) for k, v in env.items()}
        with self._lock:
            self._ensure_open_locked()
            self._sessions[req_id] = session
        try:
            self._send_json(TYPE_REQ, req_id, payload)
        except BaseException:
            with self._lock:
                self._sessions.pop(req_id, None)
            session._disconnect()
            raise
        return session

    def fs_read(self, path: str | PathLike[str], timeout: float | None = None) -> bytes:
        req_id = self._new_id()
        pending = _Pending()
        stream = ByteStream()
        with self._lock:
            self._ensure_open_locked()
            self._pending[req_id] = pending
            self._downloads[req_id] = stream
        try:
            self._send_json(TYPE_REQ, req_id, {"op": "fs_read", "path": str(path)})
            result = pending.wait(timeout)
            data = stream.read()
        except BaseException:
            with self._lock:
                self._pending.pop(req_id, None)
                self._downloads.pop(req_id, None)
            stream.close()
            raise
        if not isinstance(result, dict):
            raise ProtocolError("fs_read response payload is not a JSON object")
        if result.get("ok") is False:
            raise OSError(str(result.get("error") or "fs_read failed"))
        return data

    def fs_write(
        self,
        path: str | PathLike[str],
        data: _BytesLike | str,
        mode: int = 0o644,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        raw = data.encode() if isinstance(data, str) else bytes(data)
        req_id = self._new_id()
        pending = _Pending()
        with self._lock:
            self._ensure_open_locked()
            self._pending[req_id] = pending
        try:
            self._send_json(
                TYPE_REQ, req_id, {"op": "fs_write", "path": str(path), "mode": int(mode)}
            )
            for off in range(0, len(raw), MAX_PAYLOAD):
                self._send_frame(TYPE_STDIN, req_id, raw[off : off + MAX_PAYLOAD])
            self._send_frame(TYPE_STDIN, req_id, b"")
            result = pending.wait(timeout)
        except BaseException:
            with self._lock:
                self._pending.pop(req_id, None)
            raise
        if not isinstance(result, dict):
            raise ProtocolError("fs_write response payload is not a JSON object")
        if result.get("ok") is False:
            raise OSError(str(result.get("error") or "fs_write failed"))
        return result

    def fs_list(
        self, path: str | PathLike[str], timeout: float | None = None
    ) -> list[dict[str, Any]]:
        result = self.request("fs_list", timeout=timeout, path=str(path))
        return list(result.get("entries") or [])

    def fs_stat(self, path: str | PathLike[str], timeout: float | None = None) -> dict[str, Any]:
        return self.request("fs_stat", timeout=timeout, path=str(path))

    def fs_mkdir(
        self, path: str | PathLike[str], parents: bool = True, timeout: float | None = None
    ) -> dict[str, Any]:
        return self.request("fs_mkdir", timeout=timeout, path=str(path), parents=bool(parents))

    def fs_remove(
        self, path: str | PathLike[str], recursive: bool = False, timeout: float | None = None
    ) -> dict[str, Any]:
        return self.request("fs_remove", timeout=timeout, path=str(path), recursive=bool(recursive))

    def _new_id(self) -> int:
        with self._lock:
            self._ensure_open_locked()
            while True:
                value = next(self._ids) & 0xFFFFFFFF
                if value != 0:
                    return value

    def _ensure_open_locked(self) -> None:
        if self._closed:
            raise AgentClosed("agent connection is closed")

    def _send_json(self, frame_type: int, frame_id: int, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, separators=(",", ":")).encode()
        self._send_frame(frame_type, frame_id, raw)

    def _send_frame(self, frame_type: int, frame_id: int, payload: bytes) -> None:
        if len(payload) > MAX_PAYLOAD:
            raise ValueError("agent frame payload exceeds 1 MiB")
        with self._lock:
            self._ensure_open_locked()
        frame = _HEADER.pack(len(payload), frame_type, frame_id) + payload
        try:
            with self._send_lock:
                self._sock.sendall(frame)
        except OSError as e:
            closed = AgentClosed(f"agent connection write failed: {e}")
            self._abort(closed)
            raise closed from e

    def _reader(self) -> None:
        try:
            while True:
                frame_type, frame_id, payload = self._read_frame()
                self._dispatch(frame_type, frame_id, payload)
        except BaseException as e:
            if isinstance(e, AgentClosed):
                self._abort(e)
            elif isinstance(e, EOFError):
                self._abort(AgentClosed("agent connection closed by peer"))
            else:
                self._abort(AgentClosed(f"agent connection failed: {e}"))

    def _read_frame(self) -> tuple[int, int, bytes]:
        header = self._read_exact(_HEADER.size)
        payload_len, frame_type, frame_id = _HEADER.unpack(header)
        if payload_len > MAX_PAYLOAD:
            raise ProtocolError("agent frame payload exceeds 1 MiB")
        return frame_type, frame_id, self._read_exact(payload_len)

    def _read_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise EOFError
            buf.extend(chunk)
        return bytes(buf)

    def _dispatch(self, frame_type: int, frame_id: int, payload: bytes) -> None:
        if frame_type == TYPE_RESP:
            self._handle_resp(frame_id, payload)
            return
        if frame_type in (TYPE_STDOUT, TYPE_STDERR):
            self._handle_stream(frame_type, frame_id, payload)
            return
        if frame_type == TYPE_EXIT:
            self._handle_exit(frame_id, payload)
            return
        raise ProtocolError(f"unexpected agent frame type {frame_type}")

    def _handle_resp(self, frame_id: int, payload: bytes) -> None:
        try:
            result = json.loads(payload.decode() or "{}")
        except json.JSONDecodeError as e:
            raise ProtocolError(f"invalid JSON response: {e}") from e
        with self._lock:
            pending = self._pending.pop(frame_id, None)
            download = self._downloads.pop(frame_id, None)
            session = self._sessions.get(frame_id)
        if pending is not None:
            pending.resolve(result)
        if download is not None:
            download.close()
        if pending is None and session is not None:
            if isinstance(result, dict) and result.get("ok") is False:
                session._set_error(AgentError(str(result.get("error") or "exec failed")))

    def _handle_stream(self, frame_type: int, frame_id: int, payload: bytes) -> None:
        with self._lock:
            session = self._sessions.get(frame_id)
            download = self._downloads.get(frame_id)
        target: ByteStream | None = None
        if session is not None:
            target = session.stdout if frame_type == TYPE_STDOUT else session.stderr
        elif frame_type == TYPE_STDOUT and download is not None:
            target = download
        if target is None:
            return
        if payload:
            target.feed(payload)
        else:
            target.close()

    def _handle_exit(self, frame_id: int, payload: bytes) -> None:
        try:
            result = json.loads(payload.decode() or "{}")
        except json.JSONDecodeError as e:
            raise ProtocolError(f"invalid EXIT payload: {e}") from e
        with self._lock:
            session = self._sessions.pop(frame_id, None)
        if session is not None:
            session._set_exit(int(result.get("code", -1)), result.get("signal"))

    def _abort(self, error: BaseException) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            pending = list(self._pending.values())
            sessions = list(self._sessions.values())
            downloads = list(self._downloads.values())
            self._pending.clear()
            self._sessions.clear()
            self._downloads.clear()
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass
        closed = error if isinstance(error, AgentClosed) else AgentClosed(str(error))
        for p in pending:
            p.reject(closed)
        for stream in downloads:
            stream.close()
        for session in sessions:
            session._disconnect()


def encode_frame(frame_type: int, frame_id: int, payload: bytes) -> bytes:
    """Encode one frame; useful for lightweight tests and fake agents."""
    if len(payload) > MAX_PAYLOAD:
        raise ValueError("agent frame payload exceeds 1 MiB")
    return _HEADER.pack(len(payload), frame_type, frame_id) + payload
