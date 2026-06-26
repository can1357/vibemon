"""Thin stdlib client for the ``vmond`` daemon — what the ``vmon`` CLI talks to.

:class:`DaemonClient` speaks the newline-delimited JSON protocol (see
:mod:`vmon.daemon`). By default it connects to the local Unix socket and
*auto-starts* the daemon on first use if it is not running, so ``vmon`` behaves
like ``docker``: the user never launches ``vmond`` by hand. Set ``VMON_REMOTE=host:port``
to target a remote daemon over TCP (authenticated with ``VMON_API_TOKEN``); the
remote path never auto-starts.

Each request uses its own short-lived connection. :meth:`call` returns the final
result; :meth:`stream` invokes ``on_event(stream, data: bytes)`` for each frame
(base64-decoding ``stdout``/``stderr``) and optionally forwards an stdin stream.
"""

from __future__ import annotations

import base64
import binascii
import fcntl
import itertools
import json
import os
import select
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, BinaryIO

from .daemon import API_VERSION, daemon_paths

OnEvent = Callable[[str, bytes], None]

_AUTOSTART_TIMEOUT = 10.0


class DaemonError(Exception):
    """A daemon-side failure or transport error; ``code`` mirrors the error frame."""

    def __init__(self, message: str, *, code: str = "internal") -> None:
        super().__init__(message)
        self.message = message
        self.code = code


class _Conn:
    """One client connection: newline-JSON framing with a per-request id."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._rfile = sock.makefile("rb")
        self._wlock = threading.Lock()
        self._ids = itertools.count(1)
        self.rid: int | None = None

    def read_obj(self) -> dict[str, Any] | None:
        line = self._rfile.readline()
        if not line:
            return None
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return None
        return obj if isinstance(obj, dict) else None

    def _send(self, obj: dict[str, Any]) -> None:
        data = (json.dumps(obj, separators=(",", ":")) + "\n").encode("utf-8")
        with self._wlock:
            self._sock.sendall(data)

    def send_auth(self, token: str) -> None:
        self._send({"auth": token})

    def send_request(self, method: str, params: dict[str, Any]) -> None:
        self.rid = next(self._ids)
        self._send({"id": self.rid, "method": method, "params": params})

    def send_stdin(self, data: bytes) -> None:
        self._send({"id": self.rid, "stdin": base64.b64encode(data).decode("ascii")})

    def send_eof(self) -> None:
        self._send({"id": self.rid, "eof": True})

    def send_resize(self, rows: int, cols: int) -> None:
        self._send({"id": self.rid, "resize": [int(rows), int(cols)]})

    def close(self) -> None:
        try:
            self._rfile.close()
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass


class DaemonClient:
    """Connects to the ``vmond`` daemon, auto-starting a local one on demand."""

    def __init__(
        self, sock_path: str | os.PathLike[str] | None = None, *, autostart: bool = True
    ) -> None:
        self._token = os.environ.get("VMON_API_TOKEN")
        self._remote = self._parse_remote(os.environ.get("VMON_REMOTE"))
        self._sock_path = Path(sock_path) if sock_path is not None else daemon_paths()["sock"]
        self._autostart = autostart and self._remote is None

    @staticmethod
    def _parse_remote(value: str | None) -> tuple[str, int] | None:
        if not value:
            return None
        host, _, port = value.rpartition(":")
        if not host or not port.isdigit():
            raise DaemonError(f"invalid VMON_REMOTE {value!r}; expected host:port", code="invalid")
        return (host, int(port))

    # -- public API ---------------------------------------------------------

    def call(self, method: str, **params: Any) -> dict[str, Any]:
        """Send a request and return its result, raising :class:`DaemonError`."""
        conn = self._open()
        try:
            conn.send_request(method, params)
            return self._await_result(conn, on_event=None)
        finally:
            conn.close()

    def stream(
        self, method: str, on_event: OnEvent, stdin: BinaryIO | None = None, **params: Any
    ) -> dict[str, Any]:
        """Send a streaming request, dispatching event frames to *on_event*.

        When *stdin* is a readable binary stream its bytes are forwarded as
        ``stdin`` frames (then an ``eof``) for the life of the call.
        """
        conn = self._open()
        stop = threading.Event()
        try:
            conn.send_request(method, params)
            if stdin is not None:
                threading.Thread(
                    target=self._forward_stdin,
                    args=(conn, stdin, stop),
                    name="vmon-stdin",
                    daemon=True,
                ).start()
            return self._await_result(conn, on_event=on_event)
        finally:
            stop.set()
            conn.close()

    def interactive(
        self,
        method: str,
        on_event: OnEvent,
        *,
        tty: bool = True,
        on_ready: Callable[[str], None] | None = None,
        **params: Any,
    ) -> dict[str, Any]:
        """Run a PTY-style streaming method (``shell`` / interactive ``exec``).

        Waits for the daemon's ``ready`` event (so a caller can stop a boot
        spinner via *on_ready*), then — when *tty* and both std streams are a
        terminal — puts the local TTY in raw mode, forwards stdin bytes and
        ``SIGWINCH`` resizes, and dispatches stdout/stderr frames to *on_event*
        until the final result frame. The terminal is always restored.
        """
        conn = self._open()
        stop = threading.Event()
        raw: tuple[int, Any] | None = None
        winch: Any = None
        # Ask the daemon for a guest PTY only when requested; the local side may
        # still fall back to plain stdin forwarding if this process is not on a TTY.
        requested_tty = bool(tty)
        params = {**params, "tty": requested_tty}
        stdin_thread: threading.Thread | None = None
        try:
            conn.send_request(method, params)
            while True:
                frame = conn.read_obj()
                if frame is None:
                    raise DaemonError("daemon closed the connection", code="closed")
                if frame.get("event") == "ready":
                    use_raw_tty = requested_tty and sys.stdin.isatty() and sys.stdout.isatty()
                    if use_raw_tty:
                        raw = self._enter_raw()
                        self._send_winsize(conn)
                        winch = self._install_winch(conn)
                        target: Callable[..., None] = self._forward_raw_stdin
                        args: tuple[Any, ...] = (conn, stop)
                    else:
                        stdin = getattr(sys.stdin, "buffer", sys.stdin)
                        target = self._forward_stdin
                        args = (conn, stdin, stop)
                    stdin_thread = threading.Thread(
                        target=target,
                        args=args,
                        name="vmon-stdin",
                        daemon=True,
                    )
                    stdin_thread.start()
                    # After the stdin path is live, stop any boot spinner so the
                    # guest prompt/output owns the terminal from this point on.
                    if on_ready is not None:
                        on_ready(str(frame.get("data") or ""))
                    continue
                if "event" in frame:
                    self._dispatch_event(frame, on_event)
                    continue
                if frame.get("ok"):
                    result = frame.get("result")
                    return result if isinstance(result, dict) else {}
                error = frame.get("error") or {}
                raise DaemonError(
                    error.get("message", "request failed"), code=error.get("code", "internal")
                )
        finally:
            stop.set()
            if stdin_thread is not None and raw is not None:
                stdin_thread.join(timeout=0.3)
            if winch is not None:
                self._restore_winch(winch)
            if raw is not None:
                self._exit_raw(raw)
            conn.close()

    @staticmethod
    def _enter_raw() -> tuple[int, Any]:
        import termios
        import tty as _tty

        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        _tty.setraw(fd, termios.TCSANOW)
        return fd, old

    @staticmethod
    def _exit_raw(raw: tuple[int, Any]) -> None:
        import termios

        fd, old = raw
        try:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)
        except OSError:
            pass

    @staticmethod
    def _send_winsize(conn: _Conn) -> None:
        import shutil

        try:
            cols, lines = os.get_terminal_size(sys.stdout.fileno())
        except OSError, ValueError, AttributeError:
            cols, lines = 0, 0
        if cols <= 0 or lines <= 0:
            cols, lines = shutil.get_terminal_size((80, 24))
        conn.send_resize(lines, cols)

    def _install_winch(self, conn: _Conn) -> Any:
        def handler(_signum: int, _frame: Any) -> None:
            try:
                self._send_winsize(conn)
            except OSError:
                pass

        try:
            return signal.signal(signal.SIGWINCH, handler)
        except ValueError, AttributeError:
            return None  # not the main thread, or no SIGWINCH on this platform

    @staticmethod
    def _restore_winch(old: Any) -> None:
        try:
            signal.signal(signal.SIGWINCH, old)
        except ValueError, AttributeError:
            pass

    @staticmethod
    def _forward_raw_stdin(conn: _Conn, stop: threading.Event) -> None:
        """Forward raw stdin bytes until *stop*; ``select`` keeps it responsive."""
        try:
            fd = sys.stdin.fileno()
            while not stop.is_set():
                ready, _, _ = select.select([fd], [], [], 0.2)
                if not ready:
                    continue
                chunk = os.read(fd, 65536)
                if not chunk:
                    conn.send_eof()
                    return
                conn.send_stdin(chunk)
        except OSError, ValueError:
            pass

    def ensure_running(self) -> dict[str, Any]:
        """Ensure the daemon is up (auto-starting a local one) and return its info."""
        return self.call("info")

    def stop_daemon(self) -> dict[str, Any]:
        """Ask the running local daemon to exit (SIGTERM); returns its prior info.

        Refused for remote targets: ``info["pid"]`` is a PID on the remote host,
        so a local ``os.kill`` would hit an unrelated process.
        """
        if self._remote is not None:
            raise DaemonError("cannot stop a remote daemon over VMON_REMOTE", code="invalid")
        info = self.call("info")
        pid = info.get("pid")
        if pid:
            try:
                os.kill(int(pid), signal.SIGTERM)
            except ProcessLookupError, ValueError:
                pass
        return info

    # -- framing ------------------------------------------------------------

    def _await_result(self, conn: _Conn, on_event: OnEvent | None) -> dict[str, Any]:
        while True:
            frame = conn.read_obj()
            if frame is None:
                raise DaemonError("daemon closed the connection", code="closed")
            if "event" in frame:
                if on_event is not None:
                    self._dispatch_event(frame, on_event)
                continue
            if frame.get("ok"):
                result = frame.get("result")
                return result if isinstance(result, dict) else {}
            error = frame.get("error") or {}
            raise DaemonError(
                error.get("message", "request failed"), code=error.get("code", "internal")
            )

    @staticmethod
    def _dispatch_event(frame: dict[str, Any], on_event: OnEvent) -> None:
        event = str(frame.get("event"))
        if event == "ready":
            return  # PTY handshake; consumed by interactive(), ignored elsewhere
        data = frame.get("data") or ""
        if event in ("stdout", "stderr"):
            try:
                payload = base64.b64decode(data, validate=True)
            except (binascii.Error, TypeError, ValueError) as e:
                raise DaemonError(f"invalid {event} frame from daemon", code="protocol") from e
            on_event(event, payload)
        else:  # console / logs are plain UTF-8
            on_event(event, str(data).encode("utf-8"))

    @staticmethod
    def _forward_stdin(conn: _Conn, stdin: BinaryIO, stop: threading.Event) -> None:
        try:
            while not stop.is_set():
                chunk = stdin.read(65536)
                if not chunk:
                    conn.send_eof()
                    return
                if isinstance(chunk, str):
                    chunk = chunk.encode()
                conn.send_stdin(chunk)
        except Exception:
            pass

    # -- connection / auto-start -------------------------------------------

    def _open(self) -> _Conn:
        sock = self._connect()
        try:
            sock.settimeout(10.0)
            conn = _Conn(sock)
            banner = conn.read_obj()
            if not banner or banner.get("api") != API_VERSION:
                conn.close()
                raise DaemonError(f"unsupported daemon banner: {banner!r}", code="protocol")
            sock.settimeout(None)
            if self._remote is not None:
                # TCP always leads with an auth frame (empty when no token is set).
                conn.send_auth(self._token or "")
            return conn
        except Exception:
            sock.close()
            raise

    def _connect(self) -> socket.socket:
        if self._remote is not None:
            host, port = self._remote
            try:
                return socket.create_connection((host, port), timeout=10.0)
            except OSError as exc:
                raise DaemonError(
                    f"cannot reach remote daemon {host}:{port}: {exc}", code="unreachable"
                ) from exc
        try:
            return self._connect_uds()
        except FileNotFoundError, ConnectionRefusedError:
            if not self._autostart:
                raise DaemonError(
                    f"vmond not running at {self._sock_path} (vmon daemon start)",
                    code="unreachable",
                ) from None
            self._autostart_daemon()
            return self._connect_uds()

    def _connect_uds(self) -> socket.socket:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(str(self._sock_path))
        except OSError:
            sock.close()
            raise
        return sock

    def _autostart_daemon(self) -> None:
        """Spawn ``python -m vmon.daemon``, serialized by a dedicated start lock.

        A separate ``vmond.start.lock`` (not the daemon's owner ``vmond.lock``)
        serializes concurrent CLI starts so the spawned daemon can still acquire
        the owner lock; whoever wins the race, everyone re-checks ``ping`` first.
        """
        paths = daemon_paths()
        paths["home"].mkdir(parents=True, exist_ok=True)
        lock_fd = os.open(paths["home"] / "vmond.start.lock", os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            if self._ping_ok():
                return
            with paths["log"].open("ab") as log:
                proc = subprocess.Popen(
                    [sys.executable, "-m", "vmon.daemon"],
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=log,
                    start_new_session=True,
                )
            deadline = time.time() + _AUTOSTART_TIMEOUT
            while time.time() < deadline:
                if proc.poll() is not None:
                    raise DaemonError(
                        f"vmond exited (code {proc.returncode}) on startup; see {paths['log']}",
                        code="start_failed",
                    )
                if self._ping_ok():
                    return
                time.sleep(0.05)
            raise DaemonError(
                f"vmond did not become ready in {_AUTOSTART_TIMEOUT:.0f}s; see {paths['log']}",
                code="start_failed",
            )
        finally:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd)

    def _ping_ok(self) -> bool:
        try:
            sock = self._connect_uds()
        except OSError:
            return False
        conn: _Conn | None = None
        try:
            sock.settimeout(1.0)
            conn = _Conn(sock)
            banner = conn.read_obj()
            if not banner or banner.get("api") != API_VERSION:
                return False
            conn.send_request("ping", {})
            frame = conn.read_obj()
            return bool(frame and frame.get("ok"))
        except OSError:
            return False
        finally:
            if conn is not None:
                conn.close()
            else:
                sock.close()
