"""Async HTTP/SSE/WebSocket adapter over the local Engine."""

from __future__ import annotations

import asyncio
import base64
import builtins
import contextlib
import json
import logging
import queue
import threading
import time
from collections.abc import MutableMapping
from typing import Any

from fastapi import HTTPException, Request, WebSocket, status
from starlette.concurrency import run_in_threadpool

from ..core import Engine, EngineError, VMRecord
from .models import ExecRequest, NetworkRequest, SandboxCreate

LOGGER = logging.getLogger(__name__)

class Supervisor:
    """Async HTTP adapter over a shared :class:`vmon.core.Engine`.

    The engine owns the VM registry and all VM logic; this class adds only the
    web-side concerns. Bearer/connect-token policy lives in the routes; here we
    keep event subscribers, Prometheus counters, and the idle reaper. VM
    operations run in a worker thread, and :class:`EngineError` is mapped to an
    HTTP status. Routes pass the :class:`vmon.core.VMRecord` returned by
    :meth:`get` and delegate to the engine by ``record.name``.
    """

    _STATUS = {
        "not_found": status.HTTP_404_NOT_FOUND,
        "not_running": status.HTTP_409_CONFLICT,
        "busy": status.HTTP_409_CONFLICT,
        "invalid": status.HTTP_400_BAD_REQUEST,
        "unsupported": status.HTTP_501_NOT_IMPLEMENTED,
    }

    def __init__(self, engine: Engine, *, idle_poll: float = 5.0):
        self._engine = engine
        self._lock = threading.RLock()
        self._counters: dict[str, int] = {
            "created": 0,
            "terminated": 0,
            "idle_reaped": 0,
            "exec": 0,
            "file_read": 0,
            "file_write": 0,
            "file_delete": 0,
            "snapshot": 0,
            "auth_failed": 0,
        }
        self.idle_poll = idle_poll
        self._latency_sum_ms = 0.0
        self._latency_count = 0
        self._event_subscribers: set[
            tuple[asyncio.AbstractEventLoop, asyncio.Queue[dict[str, Any]]]
        ] = set()

    def _http_error(self, exc: EngineError) -> HTTPException:
        code = self._STATUS.get(exc.code, status.HTTP_503_SERVICE_UNAVAILABLE)
        return HTTPException(code, detail=exc.message)

    async def _run(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Run a blocking engine call off the event loop, mapping engine errors."""
        try:
            return await asyncio.to_thread(fn, *args, **kwargs)
        except EngineError as exc:
            raise self._http_error(exc) from exc

    def count(self, name: str, by: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + by

    def subscribe_events(self) -> asyncio.Queue[dict[str, Any]]:
        queue_: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        loop = asyncio.get_running_loop()
        with self._lock:
            self._event_subscribers.add((loop, queue_))
        return queue_

    def unsubscribe_events(self, queue_: asyncio.Queue[dict[str, Any]]) -> None:
        with self._lock:
            self._event_subscribers = {
                (loop, sub) for loop, sub in self._event_subscribers if sub is not queue_
            }

    def emit(self, event: str, **data: Any) -> None:
        payload = {"event": event, "ts": time.time(), **data}
        with self._lock:
            subscribers = list(self._event_subscribers)
        stale: list[asyncio.Queue[dict[str, Any]]] = []
        for loop, queue_ in subscribers:
            if loop.is_closed():
                stale.append(queue_)
                continue
            loop.call_soon_threadsafe(queue_.put_nowait, payload)
        for queue_ in stale:
            self.unsubscribe_events(queue_)

    # -- registry (delegated to the engine) --------------------------------

    async def create(self, request: SandboxCreate) -> VMRecord:
        params = request.model_dump(exclude_none=True)
        params.pop("arch", None)
        record = await self._run(self._engine.create, params)
        latency = float(record.detail.get("create_latency_ms") or 0.0)
        with self._lock:
            self._counters["created"] += 1
            self._latency_sum_ms += latency
            self._latency_count += 1
        self.emit("created", id=record.id, name=record.name, tags=dict(record.tags))
        self.emit("ready", id=record.id, name=record.name)
        return record

    def list(self, tags: dict[str, str] | None = None) -> builtins.list[VMRecord]:
        return self._engine.list(tags)

    def get(self, sid: str, *, require_running: bool = False) -> VMRecord:
        try:
            return self._engine.get(sid, require_running=require_running)
        except EngineError as exc:
            raise self._http_error(exc) from exc

    async def terminate(self, sid: str, *, reason: str = "api") -> VMRecord:
        record = await self._run(self._engine.terminate, sid, reason=reason)
        actual_reason = record.detail.get("terminated_reason", reason)
        returncode = record.detail.get("returncode")
        with self._lock:
            self._counters["terminated"] += 1
        self.emit(
            "finished", id=record.id, name=record.name, reason=actual_reason, returncode=returncode
        )
        self.emit(
            "terminated",
            id=record.id,
            name=record.name,
            reason=actual_reason,
            returncode=returncode,
        )
        return record

    async def pause(self, sid: str) -> dict[str, Any]:
        return await self._run(self._engine.pause, sid)

    async def resume(self, sid: str) -> dict[str, Any]:
        return await self._run(self._engine.resume, sid)

    async def stop(self, sid: str) -> dict[str, Any]:
        result = await self._run(self._engine.stop, sid)
        self.emit("stopped", id=sid, name=sid)
        return result

    async def remove(self, sid: str) -> dict[str, Any]:
        result = await self._run(self._engine.remove, sid)
        self.emit("removed", id=sid, name=sid)
        return result

    async def logs(self, sid: str) -> str:
        return await self._run(self._engine.logs, sid)

    async def snapshot_template(
        self, sid: str, snapshot: str, stop: bool = False
    ) -> dict[str, str]:
        path = await self._run(self._engine.snapshot_template, sid, snapshot, stop)
        self.count("snapshot")
        return {"snapshot": snapshot, "dir": path}

    async def fork(self, params: dict[str, Any]) -> builtins.list[dict[str, Any]]:
        return await self._run(self._engine.fork, params)

    async def reap_expired(self) -> None:
        now = time.time()
        expired = [
            r
            for r in self._engine.list()
            if r.status == "running" and r.expires_at is not None and r.expires_at <= now
        ]
        for record in expired:
            try:
                await self.terminate(record.id, reason="idle_timeout")
                self.count("idle_reaped")
            except Exception as exc:
                record.status = "terminated"
                record.terminated_at = time.time()
                record.error = str(exc)

    async def reap_forever(self) -> None:
        while True:
            await asyncio.sleep(self.idle_poll)
            await self.reap_expired()

    # -- per-sandbox operations (delegated to the engine) ------------------

    async def start_exec(self, record: VMRecord, request: ExecRequest) -> Any:
        self.count("exec")
        workdir = request.workdir if request.workdir is not None else request.cwd
        return await self._run(
            self._engine.start_exec,
            record.name,
            request.cmd,
            env=request.env,
            workdir=workdir,
            tty=request.tty,
            timeout=request.timeout,
        )

    async def read_file(self, record: VMRecord, path: str) -> bytes:
        self.count("file_read")
        return await self._run(self._engine.read_file, record.name, path)

    async def write_file(self, record: VMRecord, path: str, data: bytes) -> None:
        self.count("file_write")
        await self._run(self._engine.write_file, record.name, path, data)

    async def delete_file(self, record: VMRecord, path: str, recursive: bool = False) -> None:
        self.count("file_delete")
        await self._run(self._engine.delete_file, record.name, path, recursive)

    async def list_files(self, record: VMRecord, path: str) -> builtins.list[dict[str, Any]]:
        return await self._run(self._engine.list_files, record.name, path)

    async def stat_file(self, record: VMRecord, path: str) -> dict[str, Any]:
        return await self._run(self._engine.stat_file, record.name, path)

    async def snapshot(self, record: VMRecord, name: str | None) -> str:
        self.count("snapshot")
        return await self._run(self._engine.snapshot_filesystem, record.name, name)

    async def extend(self, record: VMRecord, secs: int) -> dict[str, Any]:
        # ``Engine.extend`` realigns ``record.timeout``/``last_active`` in place.
        return await self._run(self._engine.extend, record.name, secs)

    async def metrics(self, record: VMRecord) -> dict[str, Any]:
        return await self._run(self._engine.metrics, record.name)

    async def set_network_policy(self, record: VMRecord, request: NetworkRequest) -> dict[str, Any]:
        result = await self._run(
            self._engine.set_network_policy,
            record.name,
            block_network=request.block_network,
            cidr_allow=request.cidr_allow,
            domain_allow=request.domain_allow,
        )
        if request.block_network is not None:
            record.detail["block_network"] = request.block_network
        if request.cidr_allow is not None:
            record.detail["egress_allow"] = request.cidr_allow
        if request.domain_allow is not None:
            record.detail["egress_allow_domains"] = request.domain_allow
        return dict(result or self.network_policy(record))

    def network_policy(self, record: VMRecord) -> dict[str, Any]:
        return {
            "block_network": record.detail.get("block_network"),
            "egress_allow": record.detail.get("egress_allow"),
            "egress_allow_domains": record.detail.get("egress_allow_domains"),
            "inbound_cidr_allowlist": record.detail.get("inbound_cidr_allowlist"),
        }

    def connect_token(self, record: VMRecord, *, create: bool = True) -> str | None:
        token = getattr(record.sandbox, "connect_token", None) or record.detail.get("connect_token")
        if token is None and create:
            ctor = getattr(record.sandbox, "create_connect_token", None)
            if ctor is not None:
                token = ctor()
        if token is not None:
            record.detail["connect_token"] = str(token)
        return str(token) if token is not None else None

    def tunnels(self, record: VMRecord) -> dict[int, tuple[str, int]]:
        try:
            return self._engine.tunnels(record.name)
        except EngineError as exc:
            raise self._http_error(exc) from exc

    def tunnel_target(self, record: VMRecord, port: int) -> tuple[str, int]:
        target = self.tunnels(record).get(int(port))
        if target is None:
            raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail="no tunnel for sandbox port")
        return target

    def metrics_text(self) -> str:
        records = self._engine.list()
        with self._lock:
            counters = dict(self._counters)
            latency_sum = self._latency_sum_ms
            latency_count = self._latency_count
        statuses: dict[str, int] = {}
        pool_hits = 0
        pool_misses = 0
        for record in records:
            statuses[record.status] = statuses.get(record.status, 0) + 1
            pool = getattr(record.sandbox, "pool", None) or getattr(record.sandbox, "_pool", None)
            stats = getattr(pool, "stats", None)
            try:
                data = stats() if stats is not None else None
            except Exception:
                data = None
            if isinstance(data, dict):
                pool_hits += int(data.get("hits", 0))
                pool_misses += int(data.get("misses", 0))
        lines = [
            "# HELP vmon_server_sandboxes Number of sandboxes by status.",
            "# TYPE vmon_server_sandboxes gauge",
        ]
        for status_name, value in sorted(statuses.items()):
            lines.append(f'vmon_server_sandboxes{{status="{status_name}"}} {value}')
        lines.extend(
            [
                "# HELP vmon_server_events_total Server supervisor events.",
                "# TYPE vmon_server_events_total counter",
            ]
        )
        for name, value in sorted(counters.items()):
            lines.append(f'vmon_server_events_total{{event="{name}"}} {value}')
        lines.extend(
            [
                "# HELP vmon_server_create_latency_ms_sum Total sandbox create latency"
                " in milliseconds.",
                "# TYPE vmon_server_create_latency_ms_sum counter",
                f"vmon_server_create_latency_ms_sum {latency_sum}",
                "# HELP vmon_server_create_latency_ms_count Count of observed sandbox creates.",
                "# TYPE vmon_server_create_latency_ms_count counter",
                f"vmon_server_create_latency_ms_count {latency_count}",
                "# HELP vmon_server_pool_hits Warm-pool claim hits observed by the server.",
                "# TYPE vmon_server_pool_hits counter",
                f"vmon_server_pool_hits {pool_hits}",
                "# HELP vmon_server_pool_misses Warm-pool claim misses observed by the server.",
                "# TYPE vmon_server_pool_misses counter",
                f"vmon_server_pool_misses {pool_misses}",
            ]
        )
        return "\n".join(lines) + "\n"


def _chunk_to_text(chunk: Any) -> str:
    if isinstance(chunk, bytes):
        return chunk.decode("utf-8", errors="replace")
    return str(chunk)


def _sse(payload: dict[str, Any]) -> str:
    return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"


def _spawn_exec_readers(process: Any) -> queue.Queue[tuple[str, str | int | None]]:
    """Start reader/wait threads for an exec process; events land on the returned queue.

    Event tuples: ("stdout"|"stderr", text), ("<name>_eof", None), ("error", text), ("exit", code).
    """
    events: queue.Queue[tuple[str, str | int | None]] = queue.Queue()

    def _read_stream(name: str, stream: Any) -> None:
        try:
            if stream is None:
                return
            if stream.__class__.__name__ == "ByteStream" and hasattr(stream, "__next__"):
                for chunk in stream:
                    events.put((name, _chunk_to_text(chunk)))
            elif hasattr(stream, "read"):
                while True:
                    chunk = stream.read(65536)
                    if not chunk:
                        break
                    events.put((name, _chunk_to_text(chunk)))
            else:
                for chunk in stream:
                    events.put((name, _chunk_to_text(chunk)))
        except Exception as exc:
            events.put(("error", f"{name}: {exc}"))
        finally:
            events.put((f"{name}_eof", None))

    def _wait() -> None:
        try:
            code = process.wait()
        except Exception as exc:
            events.put(("error", f"wait: {exc}"))
            code = -1
        events.put(("exit", int(code) if code is not None else 0))

    stdout = getattr(process, "stdout", None)
    stderr = getattr(process, "stderr", None)
    for name, stream in (("stdout", stdout), ("stderr", stderr)):
        threading.Thread(target=_read_stream, args=(name, stream), daemon=True).start()
    threading.Thread(target=_wait, daemon=True).start()
    return events


async def _exec_sse(supervisor: Supervisor, record: VMRecord, request: ExecRequest):
    process = await supervisor.start_exec(record, request)
    events = _spawn_exec_readers(process)
    eof: set[str] = set()
    exit_code: int | None = None
    while True:
        kind, payload = await run_in_threadpool(events.get)
        if kind in {"stdout", "stderr"}:
            yield _sse({"stream": kind, "data": payload})
        elif kind.endswith("_eof"):
            eof.add(kind.removesuffix("_eof"))
        elif kind == "error":
            yield _sse({"error": payload})
        elif kind == "exit":
            exit_code = int(payload) if payload is not None else 0
        if exit_code is not None and {"stdout", "stderr"}.issubset(eof):
            yield _sse({"exit": exit_code})
            return


async def _logs_sse(supervisor: Supervisor, name: str, request: Request):
    events: queue.Queue[dict[str, Any] | None] = queue.Queue()
    cancel = threading.Event()

    def _on_line(stream: str, data: bytes) -> None:
        events.put({"stream": stream, "data": _chunk_to_text(data)})

    def _run() -> None:
        try:
            supervisor._engine.logs(name, follow=True, on_line=_on_line, cancel=cancel)
        except EngineError as exc:
            events.put({"error": exc.message})
        except Exception as exc:
            events.put({"error": str(exc)})
        finally:
            events.put(None)

    threading.Thread(target=_run, name=f"vmon-logs-{name}", daemon=True).start()
    try:
        while True:
            if await request.is_disconnected():
                return
            payload = await run_in_threadpool(events.get)
            if payload is None:
                return
            yield _sse(payload)
    finally:
        cancel.set()


async def _creator_sse(supervisor: Supervisor, verb: str, params: dict[str, Any], request: Request):
    events: queue.Queue[dict[str, Any] | None] = queue.Queue()
    cancel = threading.Event()
    method = supervisor._engine.run if verb == "run" else supervisor._engine.restore

    def _on_output(stream: str, data: bytes) -> None:
        events.put({"stream": stream, "data": _chunk_to_text(data)})

    def _run() -> None:
        try:
            result = method(params, on_output=_on_output, cancel=cancel)
        except EngineError as exc:
            events.put({"error": exc.message})
        except Exception as exc:
            events.put({"error": str(exc)})
        else:
            events.put(
                {
                    "exit": result.get("returncode", 0),
                    "name": result.get("name"),
                    "image": result.get("image"),
                    "reconstruct_ms": result.get("reconstruct_ms"),
                    "restore_ms": result.get("restore_ms"),
                }
            )
        finally:
            events.put(None)

    threading.Thread(target=_run, name=f"vmon-{verb}", daemon=True).start()
    try:
        while True:
            if await request.is_disconnected():
                return
            payload = await run_in_threadpool(events.get)
            if payload is None:
                return
            yield _sse(payload)
    finally:
        cancel.set()


def _query_shell_params(websocket: WebSocket) -> dict[str, Any]:
    params: dict[str, Any] = dict(websocket.query_params)
    if "command" in params and "cmd" not in params:
        params["cmd"] = ["/bin/sh", "-c", params.pop("command")]
    if isinstance(params.get("cmd"), str):
        with contextlib.suppress(json.JSONDecodeError, TypeError):
            loaded = json.loads(str(params["cmd"]))
            if isinstance(loaded, list):
                params["cmd"] = loaded
    if isinstance(params.get("env"), str):
        with contextlib.suppress(json.JSONDecodeError, TypeError):
            loaded = json.loads(str(params["env"]))
            if isinstance(loaded, dict):
                params["env"] = loaded
    for key in ("mem", "cpus", "disk_mb"):
        if key in params:
            params[key] = int(params[key])
    if "timeout" in params:
        params["timeout"] = float(params["timeout"])
    if "pty" in params:
        params["tty"] = str(params.pop("pty")).lower() not in {"0", "false", "no"}
    return params


async def _shell_ws(
    supervisor: Supervisor,
    params: dict[str, Any],
    websocket: WebSocket,
    *,
    accepted: bool = False,
) -> None:
    cleanup = None
    try:
        process, name, cleanup = await supervisor._run(supervisor._engine.start_shell, params)
    except HTTPException as exc:
        if not accepted:
            await websocket.accept()
        await websocket.send_json({"error": exc.detail})
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return
    except Exception as exc:
        if not accepted:
            await websocket.accept()
        await websocket.send_json({"error": str(exc)})
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
        return
    if not accepted:
        await websocket.accept()
    await websocket.send_json({"ready": name})
    try:
        await _pump_exec_ws(process, websocket)
    finally:
        if cleanup is not None:
            cleanup()


async def _pump_exec_ws(process: Any, websocket: WebSocket) -> None:
    events = _spawn_exec_readers(process)
    write_stdin = getattr(process, "write_stdin", None)
    close_stdin = getattr(process, "close_stdin", None)
    resize = getattr(process, "resize", None)
    clean_exit = False

    async def _pump_output() -> None:
        nonlocal clean_exit
        eof: set[str] = set()
        exit_code: int | None = None
        while True:
            kind, payload = await run_in_threadpool(events.get)
            if kind in {"stdout", "stderr"}:
                await websocket.send_json({"stream": kind, "data": payload})
            elif kind.endswith("_eof"):
                eof.add(kind.removesuffix("_eof"))
            elif kind == "error":
                await websocket.send_json({"error": payload})
            elif kind == "exit":
                exit_code = int(payload) if payload is not None else 0
            if exit_code is not None and {"stdout", "stderr"}.issubset(eof):
                await websocket.send_json({"exit": exit_code})
                clean_exit = True
                return

    async def _pump_input() -> None:
        while True:
            msg = await websocket.receive_json()
            if "stdin_b64" in msg and write_stdin is not None:
                await run_in_threadpool(write_stdin, base64.b64decode(msg["stdin_b64"]))
            elif msg.get("close_stdin") and close_stdin is not None:
                await run_in_threadpool(close_stdin)
            elif "resize" in msg and resize is not None:
                size = msg.get("resize") or {}
                await run_in_threadpool(
                    resize, int(size.get("rows", 24)), int(size.get("cols", 80))
                )

    tasks = {asyncio.create_task(_pump_output()), asyncio.create_task(_pump_input())}
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        with contextlib.suppress(Exception):
            task.result()
    if not clean_exit:
        with contextlib.suppress(Exception):
            await websocket.close()


def _coerce_stdin(value: Any) -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode()
    return str(value).encode()


def _ws_stdin_action(message: MutableMapping[str, Any]) -> tuple[str, Any] | None:
    """Translate a client websocket frame into a stdin/resize action.

    Returns ("write", bytes) to forward stdin, ("close", None) to half-close
    stdin, ("resize", (rows, cols)) to resize a PTY, or None for frames that
    carry no payload.
    """
    raw = message.get("bytes")
    if raw is not None:
        return ("write", bytes(raw))
    text = message.get("text")
    if text is None:
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return ("write", text.encode())
    if not isinstance(payload, dict):
        return None
    kind = payload.get("type")
    if (
        payload.get("close_stdin")
        or payload.get("eof")
        or kind in {"close_stdin", "stdin_close", "eof"}
    ):
        return ("close", None)
    if "resize" in payload:
        resize = payload["resize"]
        if isinstance(resize, dict):
            r, c = resize.get("rows"), resize.get("cols")
        elif isinstance(resize, (list, tuple)) and len(resize) == 2:
            r, c = resize
        else:
            r = c = None
        if isinstance(r, int) and isinstance(c, int) and r > 0 and c > 0:
            return ("resize", (r, c))
    if "stdin" in payload:
        return ("write", _coerce_stdin(payload["stdin"]))
    if "stdin_b64" in payload:
        return ("write", base64.b64decode(payload["stdin_b64"]))
    if kind == "stdin" and "data" in payload:
        return ("write", _coerce_stdin(payload["data"]))
    return None


async def _ws_exec_request(websocket: WebSocket) -> ExecRequest | None:
    """Build an ExecRequest from query params, or from an initial JSON message."""
    params = websocket.query_params
    cmd = list(params.getlist("cmd")) or list(params.getlist("arg"))
    if cmd:
        timeout = params.get("timeout")
        return ExecRequest(
            cmd=cmd,
            workdir=params.get("workdir"),
            cwd=params.get("cwd"),
            timeout=float(timeout) if timeout else None,
            tty=params.get("tty", "").lower() in {"1", "true", "yes"},
        )
    message = await websocket.receive()
    if message.get("type") == "websocket.disconnect":
        return None
    raw = message.get("text")
    if raw is None and message.get("bytes") is not None:
        raw = bytes(message["bytes"]).decode("utf-8", "replace")
    if not raw:
        return None
    payload = json.loads(raw)
    if isinstance(payload, dict) and isinstance(payload.get("exec"), dict):
        payload = payload["exec"]
    if not isinstance(payload, dict):
        return None
    return ExecRequest(**payload)


async def _exec_ws(
    supervisor: Supervisor, record: VMRecord, request: ExecRequest, websocket: WebSocket
) -> None:
    try:
        process = await supervisor.start_exec(record, request)
    except HTTPException as exc:
        await websocket.send_json({"error": exc.detail})
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return
    except Exception as exc:
        await websocket.send_json({"error": str(exc)})
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
        return

    events = _spawn_exec_readers(process)
    write_stdin = getattr(process, "write_stdin", None)
    close_stdin = getattr(process, "close_stdin", None)
    resize = getattr(process, "resize", None)
    clean_exit = False

    async def _pump_output() -> None:
        nonlocal clean_exit
        eof: set[str] = set()
        exit_code: int | None = None
        while True:
            kind, payload = await run_in_threadpool(events.get)
            if kind in {"stdout", "stderr"}:
                await websocket.send_json({"stream": kind, "data": payload})
            elif kind.endswith("_eof"):
                eof.add(kind.removesuffix("_eof"))
            elif kind == "error":
                await websocket.send_json({"error": payload})
            elif kind == "exit":
                exit_code = int(payload) if payload is not None else 0
            if exit_code is not None and {"stdout", "stderr"}.issubset(eof):
                await websocket.send_json({"exit": exit_code})
                clean_exit = True
                return

    async def _pump_input() -> None:
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                return
            action = _ws_stdin_action(message)
            if action is None:
                continue
            verb, data = action
            if verb == "close":
                if close_stdin is not None:
                    await run_in_threadpool(close_stdin)
            elif verb == "resize":
                if resize is not None:
                    rows, cols = data
                    with contextlib.suppress(Exception):
                        await run_in_threadpool(resize, rows, cols)
            elif write_stdin is not None:
                await run_in_threadpool(write_stdin, data)

    out_task = asyncio.create_task(_pump_output())
    in_task = asyncio.create_task(_pump_input())
    done, pending = await asyncio.wait({out_task, in_task}, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        with contextlib.suppress(Exception):
            task.result()

    if not clean_exit:
        # Client went away (or the stream broke) before the process finished; reap it.
        kill = getattr(process, "kill", None)
        if kill is not None:
            with contextlib.suppress(Exception):
                await run_in_threadpool(kill)

    with contextlib.suppress(Exception):
        await websocket.close()
