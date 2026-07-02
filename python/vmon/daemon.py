"""``vmond`` — the single-owner local daemon serving :class:`vmon.core.Engine`.

A thin ``vmon`` CLI (:mod:`vmon.client`) talks to this process over a Unix socket
at ``$VMON_HOME/vmond.sock`` using newline-delimited JSON, mirroring the per-VM
control socket protocol. The daemon owns the registry and spawns one ``vmon`` VMM
process per microVM; it is started on demand by the client and rehydrates VMs
from disk on restart.

Wire protocol (one request per connection)::

    daemon -> {"vmond": <version>, "api": 1}                       # banner
    client -> {"id": 1, "method": "ps", "params": {}}
    daemon -> {"id": 1, "event": "stdout", "data": "<base64>"}    # 0+ stream frames
    daemon -> {"id": 1, "ok": true, "result": {...}}              # final frame
            | {"id": 1, "ok": false, "error": {"code", "message"}}

``stdout``/``stderr``/exec bytes are base64 in ``data``; ``console``/``logs``
text stays UTF-8. During ``exec`` the client may send ``{"stdin": "<base64>"}``,
``{"eof": true}`` or ``{"resize": [rows, cols]}`` frames on the same connection.

Run it directly with ``python -m vmon.daemon`` (the CLI reaches it via
``vmon daemon``).
"""

from __future__ import annotations

import base64
import errno
import fcntl
import json
import os
import signal
import socket
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from . import __version__
from .core import Engine, EngineError, state_dir

API_VERSION = 1


def daemon_paths() -> dict[str, Path]:
    """Filesystem endpoints the daemon owns, all under :func:`vmon.core.state_dir`."""
    home = state_dir()
    return {
        "home": home,
        "sock": home / "vmond.sock",
        "lock": home / "vmond.lock",
        "pid": home / "vmond.pid",
        "log": home / "vmond.log",
    }


class _Conn:
    """Newline-delimited JSON framing over one client socket (thread-safe writes)."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._rfile = sock.makefile("rb")
        self._wlock = threading.Lock()

    def read_obj(self) -> dict[str, Any] | None:
        try:
            line = self._rfile.readline()
        except OSError:
            return None
        if not line:
            return None
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return None
        return obj if isinstance(obj, dict) else None

    def send(self, obj: dict[str, Any]) -> bool:
        data = (json.dumps(obj, separators=(",", ":")) + "\n").encode("utf-8")
        with self._wlock:
            try:
                self._sock.sendall(data)
            except OSError:
                return False
        return True

    def shutdown_read(self) -> None:
        try:
            self._sock.shutdown(socket.SHUT_RD)
        except OSError:
            pass

    def close(self) -> None:
        try:
            self._rfile.close()
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass


def _emitter(conn: _Conn, rid: Any) -> Callable[[str, bytes], None]:
    """An ``on_output(stream, data)`` callback writing event frames to *conn*."""

    def on_output(stream: str, data: bytes) -> None:
        if stream == "console":
            conn.send({"id": rid, "event": "console", "data": data.decode("utf-8", "replace")})
        else:
            conn.send({"id": rid, "event": stream, "data": base64.b64encode(data).decode("ascii")})

    return on_output


class Daemon:
    """Accept loop + request dispatcher over one shared :class:`Engine`.

    Bind the owner Unix socket and serve until :meth:`shutdown`. One thread per
    connection; handler exceptions become an error frame and never take down
    the daemon.
    """

    def __init__(self, engine: Engine, *, sock_path: str | os.PathLike[str]) -> None:
        self._engine = engine
        self._sock_path = Path(sock_path)
        self._stop = threading.Event()
        self._listeners: list[socket.socket] = []
        self._dispatch: dict[str, Callable[[_Conn, Any, dict[str, Any]], None]] = {
            "ping": self._h_ping,
            "info": self._h_info,
            "ps": self._h_ps,
            "run": self._h_run,
            "logs": self._h_logs,
            "exec": self._h_exec,
            "shell": self._h_shell,
            "cp_read": self._h_cp_read,
            "cp_write": self._h_cp_write,
            "fs_list": self._h_fs_list,
            "fs_stat": self._h_fs_stat,
            "pause": self._h_pause,
            "resume": self._h_resume,
            "snapshot": self._h_snapshot,
            "restore": self._h_restore,
            "fork": self._h_fork,
            "stop": self._h_stop,
            "rm": self._h_rm,
            "inspect": self._h_inspect,
            "metrics": self._h_metrics,
            "extend": self._h_extend,
        }

    # -- lifecycle ----------------------------------------------------------

    def serve_forever(self, ready: threading.Event | None = None) -> None:
        self._open_listeners()
        threads = [
            threading.Thread(
                target=self._accept_loop, args=(sock,), name="vmond-accept", daemon=True
            )
            for sock in self._listeners
        ]
        for thread in threads:
            thread.start()
        if ready is not None:
            ready.set()
        try:
            self._stop.wait()
        finally:
            self._close_listeners()

    def shutdown(self) -> None:
        self._stop.set()
        self._close_listeners()

    def _open_listeners(self) -> None:
        self._sock_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self._sock_path.parent, 0o700)
        except OSError:
            pass
        # We hold the owner lock, so any leftover socket is stale.
        try:
            self._sock_path.unlink()
        except FileNotFoundError:
            pass
        uds = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        uds.bind(str(self._sock_path))
        os.chmod(self._sock_path, 0o600)
        uds.listen(128)
        self._listeners.append(uds)

    def _close_listeners(self) -> None:
        for sock in self._listeners:
            try:
                sock.close()
            except OSError:
                pass
        self._listeners = []
        try:
            self._sock_path.unlink()
        except FileNotFoundError:
            pass

    def _accept_loop(self, listener: socket.socket) -> None:
        while not self._stop.is_set():
            try:
                client, _ = listener.accept()
            except OSError:
                return
            threading.Thread(
                target=self._handle, args=(client,), name="vmond-conn", daemon=True
            ).start()

    # -- connection handling ------------------------------------------------

    def _handle(self, client: socket.socket) -> None:
        conn = _Conn(client)
        try:
            conn.send({"vmond": __version__, "api": API_VERSION})
            req = conn.read_obj()
            if req is None:
                return
            self._serve_request(conn, req)
        except Exception:
            pass
        finally:
            conn.close()

    def _serve_request(self, conn: _Conn, req: dict[str, Any]) -> None:
        rid = req.get("id")
        method = req.get("method")
        params = req.get("params", {})
        if params is None:
            params = {}
        handler = self._dispatch.get(str(method))
        try:
            if not isinstance(params, dict):
                raise EngineError("params must be an object", code="invalid")
            if handler is None:
                raise EngineError(f"unknown method {method!r}", code="invalid")
            handler(conn, rid, params)
        except EngineError as exc:
            conn.send({"id": rid, "ok": False, "error": {"code": exc.code, "message": exc.message}})
        except Exception as exc:
            conn.send({"id": rid, "ok": False, "error": {"code": "internal", "message": str(exc)}})

    @staticmethod
    def _name(params: dict[str, Any]) -> str:
        name = params.get("name")
        if not name:
            raise EngineError("name is required", code="invalid")
        return str(name)

    # -- non-streaming handlers --------------------------------------------

    def _h_ping(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        conn.send({"id": rid, "ok": True, "result": {"pong": True}})

    def _h_info(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        conn.send(
            {
                "id": rid,
                "ok": True,
                "result": {
                    "pid": os.getpid(),
                    "version": __version__,
                    "api": API_VERSION,
                    "socket": str(self._sock_path),
                },
            }
        )

    def _h_ps(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        conn.send({"id": rid, "ok": True, "result": {"vms": self._engine.ps()}})

    def _h_pause(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        conn.send({"id": rid, "ok": True, "result": self._engine.pause(self._name(params))})

    def _h_resume(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        conn.send({"id": rid, "ok": True, "result": self._engine.resume(self._name(params))})

    def _h_snapshot(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        snapshot = params.get("snapshot")
        if not snapshot:
            raise EngineError("snapshot name is required", code="invalid")
        snap_dir = self._engine.snapshot_template(
            self._name(params), str(snapshot), stop=bool(params.get("stop"))
        )
        conn.send({"id": rid, "ok": True, "result": {"snapshot": str(snapshot), "dir": snap_dir}})

    def _h_fork(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        conn.send({"id": rid, "ok": True, "result": {"clones": self._engine.fork(params)}})

    def _h_stop(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        conn.send({"id": rid, "ok": True, "result": self._engine.stop(self._name(params))})

    def _h_rm(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        conn.send({"id": rid, "ok": True, "result": self._engine.remove(self._name(params))})

    def _h_inspect(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        conn.send({"id": rid, "ok": True, "result": self._engine.inspect(self._name(params))})

    def _h_metrics(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        conn.send({"id": rid, "ok": True, "result": self._engine.metrics(self._name(params))})

    def _h_extend(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        secs = params.get("secs")
        if not isinstance(secs, int) or isinstance(secs, bool) or secs <= 0:
            raise EngineError("secs must be a positive integer", code="invalid")
        conn.send({"id": rid, "ok": True, "result": self._engine.extend(self._name(params), secs)})

    def _h_cp_read(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        data = self._engine.cp_read(self._name(params), str(params["path"]))
        conn.send(
            {"id": rid, "ok": True, "result": {"data": base64.b64encode(data).decode("ascii")}}
        )

    def _h_cp_write(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        data = base64.b64decode(params.get("data") or "")
        self._engine.cp_write(self._name(params), str(params["path"]), data)
        conn.send({"id": rid, "ok": True, "result": {"written": len(data)}})

    def _h_fs_list(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        entries = self._engine.list_files(self._name(params), str(params.get("path") or "/"))
        conn.send({"id": rid, "ok": True, "result": {"entries": entries}})

    def _h_fs_stat(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        stat = self._engine.stat_file(self._name(params), str(params.get("path") or "/"))
        conn.send({"id": rid, "ok": True, "result": stat})

    # -- streaming handlers -------------------------------------------------

    def _h_run(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        self._streamed(conn, rid, self._engine.run, params)

    def _h_restore(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        self._streamed(conn, rid, self._engine.restore, params)

    def _streamed(
        self, conn: _Conn, rid: Any, call: Callable[..., dict[str, Any]], params: dict[str, Any]
    ) -> None:
        """Run a foreground console/process stream bound to the client connection."""
        cancel = threading.Event()
        watcher = threading.Thread(
            target=self._watch_disconnect, args=(conn, cancel), name="vmond-watch", daemon=True
        )
        watcher.start()
        try:
            result = call(params, on_output=_emitter(conn, rid), cancel=cancel)
        finally:
            cancel.set()
        conn.send({"id": rid, "ok": True, "result": result})

    def _h_logs(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        follow = bool(params.get("follow"))
        cancel = threading.Event()
        if follow:
            watcher = threading.Thread(
                target=self._watch_disconnect, args=(conn, cancel), name="vmond-watch", daemon=True
            )
            watcher.start()
        try:
            self._engine.logs(
                self._name(params), follow=follow, on_line=_emitter(conn, rid), cancel=cancel
            )
        finally:
            cancel.set()
        conn.send({"id": rid, "ok": True, "result": {"done": True}})

    def _h_exec(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        name = self._name(params)
        workdir = params.get("workdir")
        if workdir is None:
            workdir = params.get("cwd")
        proc = self._engine.start_exec(
            name,
            list(params.get("cmd") or []),
            env=params.get("env"),
            workdir=workdir,
            tty=bool(params.get("tty")),
            timeout=params.get("timeout"),
        )
        self._pump_interactive(conn, rid, proc, name)

    def _h_shell(self, conn: _Conn, rid: Any, params: dict[str, Any]) -> None:
        """Boot/attach an ephemeral shell, emit ``ready``, then pump it like exec."""
        proc, name, cleanup = self._engine.start_shell(params)
        self._pump_interactive(conn, rid, proc, name, cleanup=cleanup, result_extra={"name": name})

    def _pump_interactive(
        self,
        conn: _Conn,
        rid: Any,
        proc: Any,
        name: str,
        *,
        cleanup: Callable[[], None] | None = None,
        result_extra: dict[str, Any] | None = None,
    ) -> None:
        """Emit ``ready``, pump a PTY process, and never leak it.

        ``proc`` is killed on any error before the stdin reader is live (e.g. the
        ``ready`` send fails on a client that disconnected mid-boot); the reader
        kills it on later disconnects. ``cleanup`` (ephemeral teardown, or
        ``None`` for attach) always runs.
        """
        stop = threading.Event()
        cancel = threading.Event()
        rc = -1
        try:
            if not conn.send({"id": rid, "event": "ready", "data": name}):
                raise EngineError("client disconnected", code="closed")
            reader = threading.Thread(
                target=self._exec_input,
                args=(conn, proc, stop, cancel),
                name="vmond-stdin",
                daemon=True,
            )
            reader.start()
            rc = self._engine.pump_process(proc, _emitter(conn, rid), cancel=cancel)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
            raise
        finally:
            stop.set()
            conn.shutdown_read()
            if cleanup is not None:
                cleanup()
        # Result is sent only after ephemeral teardown, so a client that polls
        # ``ps`` right after the shell exits never sees the removed VM.
        conn.send({"id": rid, "ok": True, "result": {"returncode": rc, **(result_extra or {})}})

    @staticmethod
    def _exec_input(conn: _Conn, proc: Any, stop: threading.Event, cancel: threading.Event) -> None:
        """Forward client stdin/eof/resize frames; kill the process on disconnect."""
        while not stop.is_set():
            msg = conn.read_obj()
            if msg is None:
                cancel.set()
                if not stop.is_set():
                    try:
                        proc.kill()
                    except Exception:
                        pass
                return
            try:
                if "stdin" in msg:
                    proc.write_stdin(base64.b64decode(msg["stdin"]))
                elif msg.get("eof"):
                    proc.close_stdin()
                elif "resize" in msg:
                    rows, cols = msg["resize"]
                    proc.resize(int(rows), int(cols))
            except Exception:
                pass

    @staticmethod
    def _watch_disconnect(conn: _Conn, cancel: threading.Event) -> None:
        """Cancel a foreground stream when the client closes its connection."""
        if conn.read_obj() is None:
            cancel.set()


def acquire_owner_lock(lock_path: Path) -> int | None:
    """Take the exclusive ``vmond.lock``; return the held fd, or ``None`` if owned.

    Shared by the daemon entrypoint and ``vmon serve`` so exactly one process owns
    a given ``$VMON_HOME`` at a time.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        os.close(fd)
        if exc.errno in (errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK):
            return None
        raise
    return fd


def release_owner_lock(fd: int) -> None:
    """Release and close the owner-lock fd from :func:`acquire_owner_lock`."""
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def main(argv: list[str] | None = None) -> int:
    """Run the single-owner daemon until SIGTERM (entry for ``python -m vmon.daemon``)."""
    paths = daemon_paths()
    paths["home"].mkdir(parents=True, exist_ok=True)
    lock_fd = acquire_owner_lock(paths["lock"])
    if lock_fd is None:
        # Another daemon already owns this home; nothing to do.
        return 0
    try:
        paths["pid"].write_text(f"{os.getpid()}\n", encoding="utf-8")
        engine = Engine()
        daemon = Daemon(engine, sock_path=paths["sock"])

        def _signal(_signum: int, _frame: Any) -> None:
            daemon.shutdown()

        signal.signal(signal.SIGTERM, _signal)
        signal.signal(signal.SIGINT, _signal)
        daemon.serve_forever()
    finally:
        for path in (paths["sock"], paths["pid"]):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        release_owner_lock(lock_fd)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
