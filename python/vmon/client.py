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
import contextlib
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
from typing import Any, BinaryIO, cast

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
                        stdin_thread = threading.Thread(
                            target=self._forward_raw_stdin,
                            args=(conn, stop),
                            name="vmon-stdin",
                            daemon=True,
                        )
                        stdin_thread.start()
                    # A non-PTY one-off (`shell -c`) reads no stdin; forwarding it
                    # races the quick command's exit against a stdin-EOF teardown
                    # that can SIGHUP the guest and surface a spurious 129 exit.
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
            try:
                fd = stdin.fileno()
            except Exception:
                fd = None

            if fd is None:
                while not stop.is_set():
                    chunk = stdin.read(65536)
                    if not chunk:
                        conn.send_eof()
                        return
                    if isinstance(chunk, str):
                        chunk = chunk.encode()
                    conn.send_stdin(chunk)
                return

            while not stop.is_set():
                readable, _, _ = select.select([fd], [], [], 0.2)
                if not readable:
                    continue
                chunk = os.read(fd, 65536)
                if not chunk:
                    conn.send_eof()
                    return
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


class GatewayClient:
    """HTTP/WebSocket transport that talks to a local or remote vmon gateway."""

    def __init__(self, base_url: str, token: str | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self._token = token or os.environ.get("VMON_API_TOKEN")
        if not self._token:
            raise DaemonError("VMON_API_TOKEN required for mesh CLI", code="invalid")

    def call(self, method: str, **params: Any) -> dict[str, Any]:
        if method == "ps":
            data = self._json("GET", "/v1/sandboxes")
            rows = []
            for item in data.get("sandboxes") or []:
                rows.append(
                    {
                        "name": item.get("name") or item.get("id"),
                        "status": item.get("status"),
                        "pid": item.get("pid"),
                        "source": item.get("source") or item.get("image") or item.get("template"),
                        "node": item.get("node"),
                    }
                )
            return {"vms": rows}
        if method in {"pause", "resume", "stop"}:
            return self._json("POST", f"/v1/sandboxes/{self._q(params['name'])}/{method}", {})
        if method == "rm":
            return self._json("DELETE", f"/v1/sandboxes/{self._q(params['name'])}/remove")
        if method == "snapshot":
            return self._json(
                "POST",
                f"/v1/sandboxes/{self._q(params['name'])}/snapshot_template",
                {"snapshot": params.get("snapshot"), "stop": params.get("stop")},
            )
        if method == "fork":
            return self._json("POST", "/v1/fork", params)
        if method == "cp_read":
            path = self._urlencode({"path": params["path"]})
            file_data = self._bytes("GET", f"/v1/sandboxes/{self._q(params['name'])}/files?{path}")
            return {"data": base64.b64encode(file_data).decode("ascii")}
        if method == "cp_write":
            path = self._urlencode({"path": params["path"]})
            raw = base64.b64decode(str(params["data"]))
            self._bytes("PUT", f"/v1/sandboxes/{self._q(params['name'])}/files?{path}", raw)
            return {"written": len(raw)}
        if method == "run":
            return self._json("POST", "/v1/run?detach=1", params)
        if method == "restore":
            payload = {**params, "detach": True}
            return self._json("POST", "/v1/restore", payload)
        if method == "inspect":
            return self._json("GET", f"/v1/sandboxes/{self._q(params['name'])}")
        if method == "metrics":
            return self._json("GET", f"/v1/sandboxes/{self._q(params['name'])}/metrics")
        if method == "extend":
            return self._json(
                "POST",
                f"/v1/sandboxes/{self._q(params['name'])}/extend",
                {"secs": int(params["secs"])},
            )
        raise DaemonError(f"gateway does not support {method}", code="unsupported")

    def stream(
        self, method: str, on_event: OnEvent, stdin: BinaryIO | None = None, **params: Any
    ) -> dict[str, Any]:
        if method in {"run", "restore"}:
            return self._sse("POST", f"/v1/{method}", params, on_event)
        if method == "logs":
            query = self._urlencode({"follow": "1" if params.get("follow") else "0"})
            path = f"/v1/sandboxes/{self._q(params['name'])}/logs?{query}"
            if params.get("follow"):
                return self._sse("GET", path, None, on_event)
            on_event("console", self._bytes("GET", path))
            return {}
        if method == "exec":
            return self._ws_exec(params, on_event, tty=False, stdin=stdin)
        raise DaemonError(f"gateway does not support streaming {method}", code="unsupported")

    def interactive(
        self,
        method: str,
        on_event: OnEvent,
        *,
        tty: bool = True,
        on_ready: Callable[[str], None] | None = None,
        **params: Any,
    ) -> dict[str, Any]:
        if method == "exec":
            return self._ws_exec(params, on_event, tty=tty, on_ready=on_ready)
        if method == "shell":
            return self._ws_shell(params, on_event, tty=tty, on_ready=on_ready)
        raise DaemonError(f"gateway does not support interactive {method}", code="unsupported")

    def ensure_running(self) -> dict[str, Any]:
        self._json("GET", "/healthz")
        return {}

    def stop_daemon(self) -> dict[str, Any]:
        raise DaemonError("not supported over gateway", code="unsupported")

    @staticmethod
    def _q(value: object) -> str:
        import urllib.parse

        return urllib.parse.quote(str(value), safe="")

    @staticmethod
    def _urlencode(values: dict[str, object]) -> str:
        import urllib.parse

        return urllib.parse.urlencode(values)

    def _request(
        self, method: str, path: str, payload: Any | None = None, *, raw: bytes | None = None
    ) -> Any:
        import urllib.error
        import urllib.request

        headers = {"Authorization": f"Bearer {self._token}"}
        data = raw
        if payload is not None:
            data = json.dumps(payload).encode()
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            self.base_url + path, data=data, method=method, headers=headers
        )
        try:
            return urllib.request.urlopen(req, timeout=30)
        except urllib.error.HTTPError as exc:
            body = exc.read()
            detail = body.decode("utf-8", "replace")
            with contextlib.suppress(Exception):
                parsed = json.loads(detail)
                if isinstance(parsed, dict):
                    detail = str(parsed.get("detail") or detail)
            raise DaemonError(detail or f"gateway HTTP {exc.code}", code=str(exc.code)) from exc
        except urllib.error.URLError as exc:
            raise DaemonError(
                f"cannot reach gateway {self.base_url}: {exc.reason}", code="unreachable"
            ) from exc

    def _json(self, method: str, path: str, payload: Any | None = None) -> dict[str, Any]:
        with self._request(method, path, payload) as response:
            raw = response.read()
        if not raw:
            return {}
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}

    def _bytes(self, method: str, path: str, raw: bytes | None = None) -> bytes:
        with self._request(method, path, raw=raw) as response:
            return response.read()

    def _sse(
        self, method: str, path: str, payload: Any | None, on_event: OnEvent
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        with self._request(method, path, payload) as response:
            for raw_line in response:
                line = raw_line.decode("utf-8", "replace").strip()
                if not line.startswith("data:"):
                    continue
                frame = json.loads(line[5:].strip())
                if "stream" in frame:
                    on_event(str(frame["stream"]), str(frame.get("data") or "").encode())
                elif "exit" in frame:
                    result["returncode"] = frame.get("exit")
                    result.update({key: value for key, value in frame.items() if value is not None})
                elif "error" in frame:
                    raise DaemonError(str(frame["error"]))
        return result

    def _open_ws(self, path: str) -> socket.socket:
        import ssl
        import urllib.parse

        from .wsframe import client_handshake

        parsed = urllib.parse.urlsplit(self.base_url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        sock = socket.create_connection((host, port), timeout=30.0)
        if parsed.scheme == "https":
            sock = ssl.create_default_context().wrap_socket(sock, server_hostname=host)
        client_handshake(sock, host, port, path, {"Authorization": f"Bearer {self._token}"})
        return sock

    def _ws_exec(
        self,
        params: dict[str, Any],
        on_event: OnEvent,
        *,
        tty: bool,
        stdin: BinaryIO | None = None,
        on_ready: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        name = self._q(params["name"])
        sock = self._open_ws(f"/v1/sandboxes/{name}/exec/ws")
        request = {
            "cmd": params.get("cmd") or [],
            "tty": tty,
            "env": params.get("env"),
            "workdir": params.get("workdir"),
            "timeout": params.get("timeout"),
        }
        return self._ws_session(
            sock, request, on_event, tty=tty, stdin=stdin, on_ready=on_ready, wait_ready=False
        )

    def _ws_shell(
        self,
        params: dict[str, Any],
        on_event: OnEvent,
        *,
        tty: bool,
        on_ready: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        import urllib.parse

        request = {**params, "tty": tty}
        query: dict[str, str] = {}
        for key, value in request.items():
            if value is None:
                continue
            if isinstance(value, (dict, list)):
                query[key] = json.dumps(value, separators=(",", ":"))
            else:
                query[key] = str(value)
        path = "/v1/shell/ws?" + urllib.parse.urlencode(query)
        sock = self._open_ws(path)
        return self._ws_session(
            sock, request, on_event, tty=tty, on_ready=on_ready, wait_ready=True
        )

    def _ws_session(
        self,
        sock: socket.socket,
        request: dict[str, Any],
        on_event: OnEvent,
        *,
        tty: bool,
        stdin: BinaryIO | None = None,
        on_ready: Callable[[str], None] | None = None,
        wait_ready: bool = True,
    ) -> dict[str, Any]:
        from .wsframe import encode_frame, read_frame_sync

        stop = threading.Event()
        raw: tuple[int, Any] | None = None
        winch: Any = None
        sock.sendall(encode_frame(1, json.dumps(request, separators=(",", ":")).encode()))

        def send_obj(obj: dict[str, Any]) -> None:
            sock.sendall(encode_frame(1, json.dumps(obj, separators=(",", ":")).encode()))

        def forward(src: BinaryIO) -> None:
            try:
                while not stop.is_set():
                    chunk = src.read(65536)
                    if not chunk:
                        send_obj({"close_stdin": True})
                        return
                    if isinstance(chunk, str):
                        chunk = chunk.encode()
                    send_obj({"stdin_b64": base64.b64encode(chunk).decode("ascii")})
            except Exception:
                pass

        stdin_thread_ref: list[threading.Thread | None] = [None]

        def start_input() -> None:
            nonlocal raw
            if stdin_thread_ref[0] is not None:
                return
            src = stdin
            if src is None and tty and sys.stdin.isatty() and sys.stdout.isatty():
                raw = DaemonClient._enter_raw()
                src = cast(BinaryIO, getattr(sys.stdin, "buffer", sys.stdin))
            elif src is None:
                src = cast(BinaryIO, getattr(sys.stdin, "buffer", sys.stdin))
            thread = threading.Thread(target=forward, args=(src,), daemon=True)
            stdin_thread_ref[0] = thread
            thread.start()

        try:
            if not wait_ready:
                start_input()
            while True:
                opcode, payload = read_frame_sync(sock)
                if opcode == 8:
                    raise DaemonError("gateway websocket closed", code="closed")
                if opcode != 1:
                    continue
                frame = json.loads(payload.decode("utf-8", "replace"))
                if "ready" in frame:
                    if on_ready is not None:
                        on_ready(str(frame.get("ready") or ""))
                    start_input()
                    continue
                if "stream" in frame:
                    on_event(str(frame["stream"]), str(frame.get("data") or "").encode())
                    continue
                if "exit" in frame:
                    return {"returncode": frame.get("exit"), "name": frame.get("name")}
                if "error" in frame:
                    raise DaemonError(str(frame["error"]))
        finally:
            stop.set()
            if (stdin_thread := stdin_thread_ref[0]) is not None and raw is not None:
                stdin_thread.join(timeout=0.3)
            if winch is not None:
                DaemonClient._restore_winch(winch)
            if raw is not None:
                DaemonClient._exit_raw(raw)
            with contextlib.suppress(OSError):
                sock.close()
