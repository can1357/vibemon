"""FastAPI supervisor for vmon sandboxes.

The core SDK intentionally does not import this module; ``vmon serve`` lazy-loads it
so installing ``vmon`` does not require FastAPI unless the HTTP API is used.
"""

from __future__ import annotations

import asyncio
import base64
import builtins
import contextlib
import hashlib
import ipaddress
import json
import math
import os
import queue
import re
import secrets
import threading
import time
from collections.abc import AsyncIterator, MutableMapping
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, WebSocket, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask
from starlette.concurrency import run_in_threadpool

from .core import Engine, EngineError, VMRecord
from .mesh import HttpxTransport, Mesh, MeshError, NodeCaps, NodeState, default_advertise
from .wsframe import encode_frame, read_frame


class SandboxCreate(BaseModel):
    image: str | None = None
    template: str | None = None
    dockerfile: str | None = None
    context: str = "."
    name: str | None = None
    cpus: int = 1
    memory: int = 512
    disk_mb: int = 1024
    timeout: float | None = 300
    timeout_secs: int | None = None
    workdir: str | None = None
    env: dict[str, str] | None = None
    secrets: list[dict[str, str]] | None = None
    volumes: dict[str, str] | None = None
    tags: dict[str, str] | None = None
    fs_dir: str | None = None
    block_network: bool = False
    ports: list[int] | None = None
    egress_allow: list[str] | None = None
    egress_allow_domains: list[str] | None = None
    inbound_cidr_allowlist: list[str] | None = None
    readiness_probe: Any | None = None
    pool_size: int = 0


class ExecRequest(BaseModel):
    cmd: list[str] = Field(default_factory=list)
    workdir: str | None = None
    cwd: str | None = None
    env: dict[str, str] | None = None
    timeout: float | None = None
    tty: bool = False


class SnapshotRequest(BaseModel):
    name: str | None = None


class SnapshotTemplateRequest(BaseModel):
    snapshot: str
    stop: bool = False


class ExtendRequest(BaseModel):
    secs: int


class NetworkRequest(BaseModel):
    block_network: bool | None = None
    cidr_allow: list[str] | None = None
    domain_allow: list[str] | None = None


def _bad_request(message: str) -> HTTPException:
    return HTTPException(status.HTTP_400_BAD_REQUEST, detail=message)


def _require_positive_int(name: str, value: int) -> None:
    if value <= 0:
        raise _bad_request(f"{name} must be positive")


def _require_non_negative_int(name: str, value: int) -> None:
    if value < 0:
        raise _bad_request(f"{name} must be non-negative")


def _require_non_negative_float(name: str, value: float | None) -> None:
    if value is not None and (not math.isfinite(float(value)) or value < 0):
        raise _bad_request(f"{name} must be non-negative")


def _validate_ports(ports: list[int] | None) -> None:
    for port in ports or ():
        if port <= 0 or port > 65535:
            raise _bad_request("ports must be TCP port numbers from 1 to 65535")


def _validate_cidrs(name: str, cidrs: list[str] | None) -> None:
    for cidr in cidrs or ():
        try:
            ipaddress.ip_network(cidr, strict=False)
        except ValueError as exc:
            raise _bad_request(f"{name} entries must be valid CIDR networks") from exc


def _validate_domains(name: str, domains: list[str] | None) -> None:
    for domain in domains or ():
        if not domain.strip():
            raise _bad_request(f"{name} entries must be non-empty")


def _validate_create_request(request: SandboxCreate) -> None:
    _require_positive_int("cpus", request.cpus)
    _require_positive_int("memory", request.memory)
    _require_positive_int("disk_mb", request.disk_mb)
    _require_non_negative_int("pool_size", request.pool_size)
    _require_non_negative_float("timeout", request.timeout)
    if request.timeout_secs is not None:
        _require_non_negative_int("timeout_secs", request.timeout_secs)
    if request.block_network and request.ports:
        raise _bad_request("ports cannot be exposed when block_network=True")
    _validate_ports(request.ports)
    _validate_cidrs("egress_allow", request.egress_allow)
    _validate_cidrs("inbound_cidr_allowlist", request.inbound_cidr_allowlist)
    _validate_domains("egress_allow_domains", request.egress_allow_domains)


def _validate_exec_request(request: ExecRequest) -> None:
    if not request.cmd:
        raise _bad_request("exec cmd must not be empty")
    if not request.cmd[0]:
        raise _bad_request("exec cmd[0] must not be empty")
    _require_non_negative_float("timeout", request.timeout)


def _validate_extend_request(request: ExtendRequest) -> None:
    _require_positive_int("secs", request.secs)


def _validate_network_request(request: NetworkRequest) -> None:
    if (
        request.block_network is None
        and request.cidr_allow is None
        and request.domain_allow is None
    ):
        raise _bad_request("network request must set at least one field")
    _validate_cidrs("cidr_allow", request.cidr_allow)
    _validate_domains("domain_allow", request.domain_allow)


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

    @staticmethod
    def _model_dict(model: BaseModel) -> dict[str, Any]:
        if hasattr(model, "model_dump"):
            return model.model_dump(exclude_none=True)  # pydantic v2
        return model.dict(exclude_none=True)  # pydantic v1

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
        record = await self._run(self._engine.create, self._model_dict(request))
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
        result = await self._run(self._engine.extend, record.name, secs)
        record.timeout = float(secs)
        record.touch()
        return result

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


def _tokens_match(supplied: str | None, expected: str) -> bool:
    return supplied is not None and secrets.compare_digest(supplied, expected)


def _ws_bearer_token(websocket: WebSocket) -> str | None:
    """Extract a bearer token from the Authorization header or a token query param."""
    header = websocket.headers.get("authorization")
    if header:
        scheme, _, value = header.partition(" ")
        if scheme.lower() == "bearer" and value:
            return value.strip()
    params = websocket.query_params
    return params.get("token") or params.get("access_token")


def _request_bearer_token(request: Request) -> str | None:
    header = request.headers.get("authorization")
    if not header:
        return None
    scheme, _, value = header.partition(" ")
    return value.strip() if scheme.lower() == "bearer" and value else None


def _request_connect_token(request: Request) -> str | None:
    token = request.query_params.get("token") or request.query_params.get("access_token")
    if token:
        return token
    header = request.headers.get("authorization")
    if header:
        scheme, _, value = header.partition(" ")
        if scheme.lower() == "bearer" and value:
            return value.strip()
    return None


def _ws_connect_token(websocket: WebSocket) -> str | None:
    token = websocket.query_params.get("token") or websocket.query_params.get("access_token")
    if token:
        return token
    return _ws_bearer_token(websocket)


def _target_query(items: list[tuple[str, str]]) -> str:
    return urlencode([(key, value) for key, value in items if key not in {"token", "access_token"}])


def _decode_chunked_http_body(body: bytes) -> bytes:
    chunks: list[bytes] = []
    rest = body
    while rest:
        size_line, sep, rest = rest.partition(b"\r\n")
        if not sep:
            return body
        try:
            size = int(size_line.split(b";", 1)[0], 16)
        except ValueError:
            return body
        if size == 0:
            return b"".join(chunks)
        chunks.append(rest[:size])
        rest = rest[size + 2 :]
    return b"".join(chunks)


async def _proxy_http_request(request: Request, target: tuple[str, int], rest: str) -> Response:
    # The SDK/network layer already owns guest TCP forwarding, so the REST proxy relays
    # HTTP over the exposed localhost tunnel instead of opening guest-facing sockets here.
    host, port = target
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except OSError as exc:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY, detail=f"tunnel connect failed: {exc}"
        ) from exc
    path = "/" + rest.lstrip("/")
    query = _target_query(list(request.query_params.multi_items()))
    if query:
        path += "?" + query
    body = await request.body()
    headers = [
        f"{request.method} {path} HTTP/1.1",
        f"Host: {host}:{port}",
        "Connection: close",
        f"Content-Length: {len(body)}",
    ]
    for key, value in request.headers.items():
        lower = key.lower()
        if lower in {"host", "connection", "content-length", "authorization"}:
            continue
        headers.append(f"{key}: {value}")
    writer.write(("\r\n".join(headers) + "\r\n\r\n").encode("latin-1") + body)
    await writer.drain()
    raw = await reader.read()
    writer.close()
    with contextlib.suppress(Exception):
        await writer.wait_closed()
    head, sep, response_body = raw.partition(b"\r\n\r\n")
    if not sep:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail="invalid tunnel HTTP response")
    lines = head.decode("iso-8859-1", "replace").split("\r\n")
    try:
        status_code = int(lines[0].split(" ", 2)[1])
    except Exception as exc:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY, detail="invalid tunnel HTTP status"
        ) from exc
    response_headers: dict[str, str] = {}
    chunked = False
    for line in lines[1:]:
        key, _, value = line.partition(":")
        if not key:
            continue
        lower = key.lower()
        if lower == "transfer-encoding" and "chunked" in value.lower():
            chunked = True
            continue
        if lower in {"connection", "content-length", "transfer-encoding"}:
            continue
        response_headers[key] = value.strip()
    if chunked:
        response_body = _decode_chunked_http_body(response_body)
    return Response(
        b"" if request.method == "HEAD" else response_body,
        status_code=status_code,
        headers=response_headers,
    )


_SANDBOX_PATH_RE = re.compile(r"^/v1/sandboxes/(?:from_id/)?([^/]+)(?:/.*)?$")
_RESERVED_SANDBOX_SEGMENTS = {"from_id"}
_HOP_HEADERS = {"host", "connection", "content-length", "transfer-encoding"}


def _filter_resp_headers(headers: MutableMapping[str, str]) -> dict[str, str]:
    return {
        key: value
        for key, value in headers.items()
        if key.lower() not in {"connection", "content-length", "transfer-encoding"}
    }


async def _proxy_to_peer(request: Request, peer_url: str, token: str) -> Response:
    import httpx

    headers = {
        key: value for key, value in request.headers.items() if key.lower() not in _HOP_HEADERS
    }
    headers["Authorization"] = f"Bearer {token}"
    headers["X-Vmon-Mesh-Hop"] = "1"
    body = await request.body()
    target = peer_url.rstrip("/") + request.url.path
    if request.url.query:
        target += f"?{request.url.query}"
    client = request.app.state.mesh_http
    req = client.build_request(request.method, target, content=body, headers=headers)
    try:
        resp = await client.send(req, stream=True)
    except httpx.HTTPError as exc:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "sandbox owner unreachable"
        ) from exc
    return StreamingResponse(
        resp.aiter_raw(),
        status_code=resp.status_code,
        headers=_filter_resp_headers(resp.headers),
        background=BackgroundTask(resp.aclose),
    )


def _sandbox_id_in_path(path: str) -> str | None:
    match = _SANDBOX_PATH_RE.match(path)
    if match is None:
        return None
    sid = match.group(1)
    return None if sid in _RESERVED_SANDBOX_SEGMENTS else sid


def _engine_has(supervisor: Supervisor, sandbox_id: str) -> bool:
    try:
        supervisor._engine.get(sandbox_id)
    except EngineError:
        return False
    return True


async def _scatter_locate(mesh: Mesh, sandbox_id: str) -> str | None:
    for peer in mesh.peers():
        try:
            response = await asyncio.to_thread(
                mesh.transport.get, peer.advertise, f"/v1/mesh/locate/{sandbox_id}"
            )
        except Exception:
            continue
        owner = response.get("owner")
        if isinstance(owner, str) and owner:
            mesh.record_owner(sandbox_id, owner)
            return owner
    return None


def _require_bearer(request: Request, expected_token: str | None) -> None:
    if expected_token is None:
        return
    if not _tokens_match(_request_bearer_token(request), expected_token):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="unauthorized",
            headers={"WWW-Authenticate": "Bearer"},
        )


def _mesh_http_status(exc: MeshError) -> int:
    return {
        "unauthorized": status.HTTP_401_UNAUTHORIZED,
        "unreachable": status.HTTP_502_BAD_GATEWAY,
        "conflict": status.HTTP_409_CONFLICT,
    }.get(exc.code, status.HTTP_400_BAD_REQUEST)


def _split_http_authority(url: str) -> tuple[str, int]:
    parsed = urlsplit(url)
    if parsed.scheme == "https":
        raise MeshError("ws across https peer unsupported", code="unreachable")
    if parsed.scheme and parsed.scheme != "http":
        raise MeshError("unsupported peer URL scheme", code="invalid")
    if not parsed.hostname:
        raise MeshError("invalid peer URL", code="invalid")
    return parsed.hostname, parsed.port or 80


def _ws_build_path(rest: str, websocket: WebSocket, *, preserve_query: bool = False) -> str:
    path = rest if preserve_query and rest.startswith("/") else "/" + rest.lstrip("/")
    if preserve_query:
        query = websocket.url.query
    else:
        query = _target_query(list(websocket.query_params.multi_items()))
    return path + (f"?{query}" if query else "")


async def _proxy_websocket(
    websocket: WebSocket,
    target: tuple[str, int],
    rest: str,
    *,
    extra_headers: dict[str, str] | None = None,
    preserve_query: bool = False,
) -> None:
    # Minimal RFC 6455 client relay: FastAPI decodes the public WebSocket, then
    # this bridge re-encodes frames to the sandbox's localhost tunnel.
    host, port = target
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except OSError:
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="tunnel connect failed")
        return
    key = base64.b64encode(os.urandom(16)).decode()
    path = _ws_build_path(rest, websocket, preserve_query=preserve_query)
    request_headers = [
        f"GET {path} HTTP/1.1",
        f"Host: {host}:{port}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        "Sec-WebSocket-Version: 13",
    ]
    for header, value in (extra_headers or {}).items():
        request_headers.append(f"{header}: {value}")
    request = "\r\n".join(request_headers) + "\r\n\r\n"
    try:
        writer.write(request.encode("latin-1"))
        await writer.drain()
        response = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=10.0)
    except OSError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, TimeoutError:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="websocket upgrade failed")
        return
    expected = base64.b64encode(
        hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
    ).decode()
    if b" 101 " not in response.split(b"\r\n", 1)[0]:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="websocket upgrade failed")
        return
    if expected.encode() not in response:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        await websocket.close(
            code=status.WS_1011_INTERNAL_ERROR, reason="websocket accept mismatch"
        )
        return
    await websocket.accept()

    async def _client_to_guest() -> None:
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                writer.write(encode_frame(8))
                await writer.drain()
                return
            if message.get("bytes") is not None:
                writer.write(encode_frame(2, bytes(message["bytes"])))
                await writer.drain()
            elif message.get("text") is not None:
                writer.write(encode_frame(1, str(message["text"]).encode()))
                await writer.drain()

    async def _guest_to_client() -> None:
        while True:
            opcode, payload = await read_frame(reader)
            if opcode == 1:
                await websocket.send_text(payload.decode("utf-8", "replace"))
            elif opcode == 2:
                await websocket.send_bytes(payload)
            elif opcode == 8:
                await websocket.close()
                return
            elif opcode == 9:
                writer.write(encode_frame(10, payload))
                await writer.drain()

    tasks = {asyncio.create_task(_client_to_guest()), asyncio.create_task(_guest_to_client())}
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        with contextlib.suppress(Exception):
            task.result()
    writer.close()
    with contextlib.suppress(Exception):
        await writer.wait_closed()


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


def _web_dir() -> Path:
    return Path(__file__).resolve().parent / "web"


def _mount_web_ui(app: FastAPI) -> None:
    """Serve the built React UI from ``vmon/web`` with an SPA fallback.

    No-op (with a stub index) when the build dir is absent, so ``vmon serve``
    still works without the UI having been built. API routes (/v1, /healthz,
    /metrics) are registered first and take precedence; the catch-all only
    handles GET requests for client-side routes.
    """
    web = _web_dir()
    index = web / "index.html"
    if not index.is_file():
        return
    # Static assets (hashed bundles) at /assets/*.
    assets = web / "assets"
    if assets.is_dir():
        app.mount("/assets", StaticFiles(directory=str(assets)), name="web-assets")
    # SPA fallback: any non-API GET returns index.html; the client router
    # decides what to render. Hard 404 for missing files outside /assets.
    api_prefixes = ("/v1", "/healthz", "/metrics")

    @app.get("/{path:path}")
    async def _spa(path: str) -> Response:
        if ("/" + path).startswith(api_prefixes):
            raise HTTPException(status.HTTP_404_NOT_FOUND)
        candidate = (web / path).resolve()
        try:
            candidate.relative_to(web)
        except ValueError:
            raise HTTPException(status.HTTP_404_NOT_FOUND) from None
        if candidate.is_file():
            return FileResponse(str(candidate))
        return FileResponse(str(index))


def create_app(
    *,
    token: str | None = None,
    idle_timeout: float = 300.0,
    host: str = "127.0.0.1",
    port: int = 8000,
) -> FastAPI:
    supervisor = Supervisor(Engine(), idle_poll=max(1.0, min(float(idle_timeout), 30.0)))
    expected_token = token if token is not None else os.environ.get("VMON_API_TOKEN")
    mesh = Mesh(
        supervisor._engine,
        advertise=default_advertise(host, port),
        token=expected_token or "",
        transport=HttpxTransport(expected_token or ""),
    )
    bearer = HTTPBearer(auto_error=False)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        import httpx

        app.state.mesh_http = httpx.AsyncClient(timeout=None)
        mesh.load()
        if mesh.enabled:
            ensure_mesh_heartbeat()
        app.state.reaper_task = asyncio.create_task(supervisor.reap_forever())
        try:
            yield
        finally:
            stop = getattr(app.state, "mesh_stop", None)
            if stop is not None:
                stop.set()
            task = getattr(app.state, "reaper_task", None)
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            await app.state.mesh_http.aclose()
            close = getattr(mesh.transport, "close", None)
            if close is not None:
                close()

    app = FastAPI(title="vmon sandbox server", version="1", lifespan=lifespan)
    app.state.supervisor = supervisor
    app.state.mesh = mesh

    def ensure_mesh_heartbeat() -> None:
        thread = getattr(app.state, "mesh_thread", None)
        if thread is not None and thread.is_alive():
            return
        stop = threading.Event()
        app.state.mesh_stop = stop
        app.state.mesh_thread = threading.Thread(
            target=mesh.run_heartbeat, args=(stop,), name="vmon-mesh-hb", daemon=True
        )
        app.state.mesh_thread.start()

    @app.exception_handler(RequestValidationError)
    async def _validation_exception_handler(
        _request: Request, _exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse({"detail": "invalid request"}, status_code=status.HTTP_400_BAD_REQUEST)

    async def require_auth(
        credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
    ) -> None:
        if expected_token is None:
            return
        if (
            credentials is None
            or credentials.scheme.lower() != "bearer"
            or not _tokens_match(credentials.credentials, expected_token)
        ):
            supervisor.count("auth_failed")
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                detail="unauthorized",
                headers={"WWW-Authenticate": "Bearer"},
            )

    def parse_tag_filters(values: list[str] | None) -> dict[str, str] | None:
        if not values:
            return None
        parsed: dict[str, str] = {}
        for value in values:
            key, sep, tag_value = value.partition(":")
            if not sep or not key:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="tag filters must be k:v")
            parsed[key] = tag_value
        return parsed

    def require_connect_token(record: VMRecord, supplied: str | None) -> None:
        expected = supervisor.connect_token(record, create=False)
        if expected is None or not _tokens_match(supplied, expected):
            supervisor.count("auth_failed")
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                detail="invalid connect token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    @app.middleware("http")
    async def _mesh_router(request: Request, call_next: Any) -> Response:
        if not mesh.enabled or request.headers.get("x-vmon-mesh-hop"):
            return await call_next(request)
        sandbox_id = _sandbox_id_in_path(request.url.path)
        if sandbox_id is None or _engine_has(supervisor, sandbox_id):
            return await call_next(request)
        owner = mesh.owner_of(sandbox_id) or await _scatter_locate(mesh, sandbox_id)
        if owner is None or owner == mesh.node_id:
            return await call_next(request)
        peer_url = mesh.peer_url(owner)
        if peer_url is None:
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "sandbox owner unreachable")
        _require_bearer(request, expected_token)
        return await _proxy_to_peer(request, peer_url, expected_token or "")

    async def proxy_ws_owner(websocket: WebSocket, sandbox_id: str) -> bool:
        if not mesh.enabled or websocket.headers.get("x-vmon-mesh-hop"):
            return False
        if _engine_has(supervisor, sandbox_id):
            return False
        owner = mesh.owner_of(sandbox_id) or await _scatter_locate(mesh, sandbox_id)
        if owner is None or owner == mesh.node_id:
            return False
        url = mesh.peer_url(owner)
        if url is None:
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="owner unreachable")
            return True
        try:
            target = _split_http_authority(url)
        except MeshError as exc:
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason=exc.message)
            return True
        await _proxy_websocket(
            websocket,
            target,
            websocket.url.path,
            extra_headers={
                "Authorization": f"Bearer {expected_token or ''}",
                "X-Vmon-Mesh-Hop": "1",
            },
            preserve_query=True,
        )
        return True

    async def events_stream(request: Request):
        subscriber = supervisor.subscribe_events()
        try:
            while True:
                if await request.is_disconnected():
                    return
                try:
                    payload = await asyncio.wait_for(subscriber.get(), timeout=15.0)
                except TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                yield _sse(payload)
        finally:
            supervisor.unsubscribe_events(subscriber)

    @app.get("/healthz")
    async def healthz() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/metrics", dependencies=[Depends(require_auth)])
    async def metrics() -> PlainTextResponse:
        return PlainTextResponse(supervisor.metrics_text(), media_type="text/plain; version=0.0.4")

    @app.post("/v1/mesh/setup", dependencies=[Depends(require_auth)])
    async def mesh_setup(body: dict[str, Any]) -> dict[str, Any]:
        caps = None
        if body.get("max_vcpus") is not None or body.get("max_mem_mib") is not None:
            caps = NodeCaps(
                int(body.get("max_vcpus") or mesh.caps.vcpus),
                int(body.get("max_mem_mib") or mesh.caps.mem_mib),
            )
        try:
            blob = mesh.setup(
                str(body.get("advertise") or mesh._default_advertise),
                region=str(body.get("region") or ""),
                caps=caps,
            )
        except MeshError as exc:
            raise HTTPException(_mesh_http_status(exc), detail=exc.message) from exc
        ensure_mesh_heartbeat()
        return {"blob": blob, "node_id": mesh.node_id, "advertise": mesh.advertise}

    @app.post("/v1/mesh/join", dependencies=[Depends(require_auth)])
    async def mesh_join(body: dict[str, Any]) -> dict[str, Any]:
        try:
            status_view = mesh.join(
                str(body.get("blob") or ""),
                advertise=str(body["advertise"]) if body.get("advertise") else None,
                region=str(body.get("region") or ""),
            )
        except MeshError as exc:
            raise HTTPException(_mesh_http_status(exc), detail=exc.message) from exc
        ensure_mesh_heartbeat()
        return status_view

    @app.post("/v1/mesh/leave", dependencies=[Depends(require_auth)])
    async def mesh_leave() -> dict[str, bool]:
        mesh.leave()
        return {"ok": True}

    @app.get("/v1/mesh/status", dependencies=[Depends(require_auth)])
    async def mesh_status() -> dict[str, Any]:
        return mesh.status()

    @app.post("/v1/mesh/members", dependencies=[Depends(require_auth)])
    async def mesh_members(body: dict[str, Any]) -> dict[str, Any]:
        try:
            return mesh.register(NodeState.from_wire(body))
        except MeshError as exc:
            raise HTTPException(_mesh_http_status(exc), detail=exc.message) from exc

    @app.post("/v1/mesh/heartbeat", dependencies=[Depends(require_auth)])
    async def mesh_heartbeat(body: dict[str, Any]) -> dict[str, Any]:
        try:
            state = NodeState.from_wire(dict(body.get("state") or {}))
            known = list(body.get("known") or [])
            return mesh.heartbeat(state, known)
        except MeshError as exc:
            raise HTTPException(_mesh_http_status(exc), detail=exc.message) from exc

    @app.post("/v1/mesh/depart", dependencies=[Depends(require_auth)])
    async def mesh_depart(body: dict[str, Any]) -> dict[str, bool]:
        mesh.depart(str(body.get("node_id") or ""))
        return {"ok": True}

    @app.get("/v1/mesh/locate/{sandbox_id}", dependencies=[Depends(require_auth)])
    async def mesh_locate(sandbox_id: str) -> dict[str, str]:
        if _engine_has(supervisor, sandbox_id):
            return {"owner": mesh.node_id}
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="unknown sandbox")

    @app.post("/v1/sandboxes", dependencies=[Depends(require_auth)])
    async def create_sandbox(request: Request, body: SandboxCreate) -> JSONResponse:
        _validate_create_request(body)
        body_dict = supervisor._model_dict(body)
        if (
            mesh.enabled
            and not request.headers.get("x-vmon-mesh-hop")
            and not mesh.pinned_local(body_dict)
        ):
            name = body_dict.get("name")
            if isinstance(name, str) and (
                _engine_has(supervisor, name)
                or mesh.owner_of(name)
                or await _scatter_locate(mesh, name)
            ):
                raise HTTPException(
                    status.HTTP_409_CONFLICT, detail=f"sandbox {name!r} already exists"
                )
            tried: set[str] = set()
            for _ in range(2):
                owner = mesh.place(body_dict)
                if owner == mesh.node_id or owner in tried:
                    break
                url = mesh.peer_url(owner)
                tried.add(owner)
                if url is None:
                    mesh.mark_unhealthy(owner)
                    continue
                mesh.note_inflight(1)
                try:
                    view = await asyncio.to_thread(
                        mesh.transport.post, url, "/v1/sandboxes", body_dict
                    )
                except MeshError:
                    mesh.mark_unhealthy(owner)
                    continue
                finally:
                    mesh.note_inflight(-1)
                sid = str(view.get("id") or view.get("name") or "")
                if sid:
                    mesh.record_owner(sid, owner)
                return JSONResponse(view, status_code=status.HTTP_201_CREATED)
        record = await supervisor.create(body)
        return JSONResponse(record.view(), status_code=status.HTTP_201_CREATED)

    @app.get("/v1/sandboxes", dependencies=[Depends(require_auth)])
    async def list_sandboxes(
        request: Request,
        tag: list[str] | None = Query(None),
    ) -> dict[str, list[dict[str, Any]]]:
        rows: dict[str, dict[str, Any]] = {}
        for record in supervisor.list(tags=parse_tag_filters(tag)):
            view = record.view()
            if mesh.enabled:
                view["node"] = mesh.node_id
            rows[str(view["id"])] = view
        if mesh.enabled and not request.headers.get("x-vmon-mesh-hop"):
            query = urlencode([("tag", value) for value in tag or []])
            path = "/v1/sandboxes" + (f"?{query}" if query else "")
            now = time.time()
            for peer in mesh.peers():
                if not peer.healthy(now, mesh.interval):
                    continue
                try:
                    response = await asyncio.to_thread(mesh.transport.get, peer.advertise, path)
                except Exception:
                    continue
                for item in response.get("sandboxes") or []:
                    if not isinstance(item, dict) or "id" not in item:
                        continue
                    item = dict(item)
                    item["node"] = peer.node_id
                    rows[str(item["id"])] = item
        return {"sandboxes": list(rows.values())}

    @app.post("/v1/run", dependencies=[Depends(require_auth)])
    async def run_gateway(request: Request, detach: bool = False) -> Response:
        params = dict(await request.json())
        params["detach"] = detach or bool(params.get("detach"))
        if (
            mesh.enabled
            and not request.headers.get("x-vmon-mesh-hop")
            and not mesh.pinned_local(params)
        ):
            owner = mesh.place(params)
            if owner != mesh.node_id:
                url = mesh.peer_url(owner)
                if url is None:
                    raise HTTPException(
                        status.HTTP_503_SERVICE_UNAVAILABLE, "sandbox owner unreachable"
                    )
                return await _proxy_to_peer(request, url, expected_token or "")
        if params["detach"]:
            return JSONResponse(await supervisor._run(supervisor._engine.run, params))
        return StreamingResponse(
            _creator_sse(supervisor, "run", params, request), media_type="text/event-stream"
        )

    @app.post("/v1/restore", dependencies=[Depends(require_auth)])
    async def restore_gateway(request: Request) -> Response:
        params = dict(await request.json())
        if mesh.enabled and not request.headers.get("x-vmon-mesh-hop"):
            owner = mesh.place(params)
            if owner != mesh.node_id:
                url = mesh.peer_url(owner)
                if url is None:
                    raise HTTPException(
                        status.HTTP_503_SERVICE_UNAVAILABLE, "sandbox owner unreachable"
                    )
                return await _proxy_to_peer(request, url, expected_token or "")
        if params.get("detach"):
            return JSONResponse(await supervisor._run(supervisor._engine.restore, params))
        return StreamingResponse(
            _creator_sse(supervisor, "restore", params, request), media_type="text/event-stream"
        )

    @app.post("/v1/fork", dependencies=[Depends(require_auth)])
    async def fork_gateway(request: Request) -> dict[str, Any]:
        params = dict(await request.json())
        if mesh.enabled and not request.headers.get("x-vmon-mesh-hop"):
            owner = mesh.place(params)
            if owner != mesh.node_id:
                url = mesh.peer_url(owner)
                if url is None:
                    raise HTTPException(
                        status.HTTP_503_SERVICE_UNAVAILABLE, "sandbox owner unreachable"
                    )
                response = await asyncio.to_thread(mesh.transport.post, url, "/v1/fork", params)
                for clone in response.get("clones") or []:
                    if isinstance(clone, dict) and clone.get("name"):
                        mesh.record_owner(str(clone["name"]), owner)
                return response
        clones = await supervisor.fork(params)
        return {"clones": clones}

    @app.get("/v1/sandboxes/from_id/{sandbox_id}", dependencies=[Depends(require_auth)])
    async def get_sandbox_from_id(sandbox_id: str) -> dict[str, Any]:
        return supervisor.get(sandbox_id).view()

    @app.get("/v1/sandboxes/{sandbox_id}", dependencies=[Depends(require_auth)])
    async def get_sandbox(sandbox_id: str) -> dict[str, Any]:
        return supervisor.get(sandbox_id).view()

    @app.delete("/v1/sandboxes/{sandbox_id}", dependencies=[Depends(require_auth)])
    async def delete_sandbox(sandbox_id: str) -> dict[str, Any]:
        record = await supervisor.terminate(sandbox_id)
        return record.view()

    @app.post("/v1/sandboxes/{sandbox_id}/pause", dependencies=[Depends(require_auth)])
    async def pause_sandbox(sandbox_id: str) -> dict[str, Any]:
        return await supervisor.pause(sandbox_id)

    @app.post("/v1/sandboxes/{sandbox_id}/resume", dependencies=[Depends(require_auth)])
    async def resume_sandbox(sandbox_id: str) -> dict[str, Any]:
        return await supervisor.resume(sandbox_id)

    @app.post("/v1/sandboxes/{sandbox_id}/stop", dependencies=[Depends(require_auth)])
    async def stop_sandbox(sandbox_id: str) -> dict[str, Any]:
        result = await supervisor.stop(sandbox_id)
        mesh.forget_owner(sandbox_id)
        return result

    @app.delete("/v1/sandboxes/{sandbox_id}/remove", dependencies=[Depends(require_auth)])
    async def remove_sandbox(sandbox_id: str) -> dict[str, Any]:
        result = await supervisor.remove(sandbox_id)
        mesh.forget_owner(sandbox_id)
        return result

    @app.get("/v1/sandboxes/{sandbox_id}/logs", dependencies=[Depends(require_auth)])
    async def logs_sandbox(request: Request, sandbox_id: str, follow: bool = False) -> Response:
        if follow:
            return StreamingResponse(
                _logs_sse(supervisor, sandbox_id, request), media_type="text/event-stream"
            )
        return PlainTextResponse(await supervisor.logs(sandbox_id))

    @app.post("/v1/sandboxes/{sandbox_id}/snapshot_template", dependencies=[Depends(require_auth)])
    async def snapshot_template_sandbox(
        sandbox_id: str, body: SnapshotTemplateRequest
    ) -> dict[str, str]:
        return await supervisor.snapshot_template(sandbox_id, body.snapshot, body.stop)

    @app.post("/v1/sandboxes/{sandbox_id}/extend", dependencies=[Depends(require_auth)])
    async def extend_sandbox(sandbox_id: str, body: ExtendRequest) -> dict[str, Any]:
        _validate_extend_request(body)
        record = supervisor.get(sandbox_id, require_running=True)
        return await supervisor.extend(record, body.secs)

    @app.post("/v1/sandboxes/{sandbox_id}/exec", dependencies=[Depends(require_auth)])
    async def exec_sandbox(sandbox_id: str, body: ExecRequest) -> StreamingResponse:
        _validate_exec_request(body)
        record = supervisor.get(sandbox_id, require_running=True)
        return StreamingResponse(
            _exec_sse(supervisor, record, body), media_type="text/event-stream"
        )

    @app.websocket("/v1/sandboxes/{sandbox_id}/exec/ws")
    async def exec_sandbox_ws(websocket: WebSocket, sandbox_id: str) -> None:
        if expected_token is not None and not _tokens_match(
            _ws_bearer_token(websocket), expected_token
        ):
            supervisor.count("auth_failed")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="unauthorized")
            return
        if await proxy_ws_owner(websocket, sandbox_id):
            return
        await websocket.accept()
        try:
            record = supervisor.get(sandbox_id, require_running=True)
        except HTTPException as exc:
            with contextlib.suppress(Exception):
                await websocket.send_json({"error": exc.detail})
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        try:
            request = await _ws_exec_request(websocket)
        except Exception as exc:  # malformed query/init frame -> report and close
            with contextlib.suppress(Exception):
                await websocket.send_json({"error": f"invalid exec request: {exc}"})
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        if request is None:
            await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)
            return
        try:
            _validate_exec_request(request)
        except HTTPException as exc:
            await websocket.send_json({"error": exc.detail})
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        await _exec_ws(supervisor, record, request, websocket)

    @app.websocket("/v1/shell/ws")
    async def shell_gateway_ws(websocket: WebSocket) -> None:
        if expected_token is not None and not _tokens_match(
            _ws_bearer_token(websocket), expected_token
        ):
            supervisor.count("auth_failed")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="unauthorized")
            return
        accepted = False
        params = _query_shell_params(websocket)
        if not params:
            await websocket.accept()
            accepted = True
            try:
                params = dict(await websocket.receive_json())
            except Exception as exc:
                await websocket.send_json({"error": f"invalid shell request: {exc}"})
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
                return
        ref = str(params.get("ref") or "")
        if ref and not accepted and await proxy_ws_owner(websocket, ref):
            return
        if (
            mesh.enabled
            and not accepted
            and not websocket.headers.get("x-vmon-mesh-hop")
            and not mesh.pinned_local(params)
        ):
            owner = mesh.place(params)
            if owner != mesh.node_id:
                url = mesh.peer_url(owner)
                if url is None:
                    await websocket.close(
                        code=status.WS_1011_INTERNAL_ERROR, reason="owner unreachable"
                    )
                    return
                try:
                    target = _split_http_authority(url)
                except MeshError as exc:
                    await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason=exc.message)
                    return
                await _proxy_websocket(
                    websocket,
                    target,
                    websocket.url.path,
                    extra_headers={
                        "Authorization": f"Bearer {expected_token or ''}",
                        "X-Vmon-Mesh-Hop": "1",
                    },
                    preserve_query=True,
                )
                return
        await _shell_ws(supervisor, params, websocket, accepted=accepted)

    @app.get("/v1/sandboxes/{sandbox_id}/files", dependencies=[Depends(require_auth)])
    async def get_file(sandbox_id: str, path: str) -> Response:
        record = supervisor.get(sandbox_id, require_running=True)
        data = await supervisor.read_file(record, path)
        return Response(data, media_type="application/octet-stream")

    @app.put("/v1/sandboxes/{sandbox_id}/files", dependencies=[Depends(require_auth)])
    async def put_file(sandbox_id: str, path: str, request: Request) -> dict[str, bool]:
        record = supervisor.get(sandbox_id, require_running=True)
        await supervisor.write_file(record, path, await request.body())
        return {"ok": True}

    @app.delete("/v1/sandboxes/{sandbox_id}/files", dependencies=[Depends(require_auth)])
    async def delete_file(sandbox_id: str, path: str, recursive: bool = False) -> dict[str, bool]:
        record = supervisor.get(sandbox_id, require_running=True)
        await supervisor.delete_file(record, path, recursive=recursive)
        return {"ok": True}

    @app.get("/v1/sandboxes/{sandbox_id}/fs/list", dependencies=[Depends(require_auth)])
    async def list_files(sandbox_id: str, path: str) -> dict[str, list[dict[str, Any]]]:
        record = supervisor.get(sandbox_id, require_running=True)
        entries = await supervisor.list_files(record, path)
        return {"entries": entries}

    @app.get("/v1/sandboxes/{sandbox_id}/fs/stat", dependencies=[Depends(require_auth)])
    async def stat_file(sandbox_id: str, path: str) -> dict[str, Any]:
        record = supervisor.get(sandbox_id, require_running=True)
        return await supervisor.stat_file(record, path)

    @app.get("/v1/sandboxes/{sandbox_id}/network", dependencies=[Depends(require_auth)])
    async def get_network(sandbox_id: str) -> dict[str, Any]:
        record = supervisor.get(sandbox_id)
        return supervisor.network_policy(record)

    @app.put("/v1/sandboxes/{sandbox_id}/network", dependencies=[Depends(require_auth)])
    async def put_network(sandbox_id: str, body: NetworkRequest) -> dict[str, Any]:
        _validate_network_request(body)
        record = supervisor.get(sandbox_id, require_running=True)
        return await supervisor.set_network_policy(record, body)

    @app.get("/v1/sandboxes/{sandbox_id}/tunnels", dependencies=[Depends(require_auth)])
    async def get_tunnels(sandbox_id: str) -> dict[str, Any]:
        record = supervisor.get(sandbox_id, require_running=True)
        token = supervisor.connect_token(record)
        tunnels = {
            str(port): [host, target_port]
            for port, (host, target_port) in supervisor.tunnels(record).items()
        }
        return {"tunnels": tunnels, "connect_token": token}

    @app.api_route(
        "/v1/sandboxes/{sandbox_id}/ports/{port:int}/{rest:path}",
        methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
        dependencies=[Depends(require_auth)],
    )
    async def proxy_http_port(sandbox_id: str, port: int, rest: str, request: Request) -> Response:
        record = supervisor.get(sandbox_id, require_running=True)
        require_connect_token(record, _request_connect_token(request))
        target = supervisor.tunnel_target(record, port)
        return await _proxy_http_request(request, target, rest)

    @app.websocket("/v1/sandboxes/{sandbox_id}/ports/{port:int}/ws/{rest:path}")
    async def proxy_ws_port(websocket: WebSocket, sandbox_id: str, port: int, rest: str) -> None:
        if expected_token is not None and not _tokens_match(
            _ws_bearer_token(websocket), expected_token
        ):
            supervisor.count("auth_failed")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="unauthorized")
            return
        if await proxy_ws_owner(websocket, sandbox_id):
            return
        try:
            record = supervisor.get(sandbox_id, require_running=True)
            require_connect_token(record, _ws_connect_token(websocket))
            target = supervisor.tunnel_target(record, port)
        except HTTPException as exc:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=str(exc.detail))
            return
        await _proxy_websocket(websocket, target, rest)

    @app.get("/v1/events", dependencies=[Depends(require_auth)])
    async def events(request: Request) -> StreamingResponse:
        return StreamingResponse(events_stream(request), media_type="text/event-stream")

    @app.post("/v1/sandboxes/{sandbox_id}/snapshot", dependencies=[Depends(require_auth)])
    async def snapshot_sandbox(sandbox_id: str, body: SnapshotRequest) -> dict[str, str]:
        record = supervisor.get(sandbox_id, require_running=True)
        image = await supervisor.snapshot(record, body.name)
        return {"image": image}

    _mount_web_ui(app)

    return app


def serve(
    *,
    host: str = "127.0.0.1",
    port: int = 8000,
    token: str | None = None,
    idle_timeout: float = 300.0,
) -> None:
    """Run the daemon **and** the HTTP/web gateway over a single shared engine.

    ``vmon serve`` is the same single-owner process as ``vmond``: it takes the
    ``vmond.lock``, serves the local Unix socket (so the ``vmon`` CLI reaches this
    process) on a background thread bound to the app's :class:`~vmon.core.Engine`,
    and runs uvicorn for the REST/web API. Exactly one owner per ``$VMON_HOME``.
    """
    try:
        import uvicorn
    except ModuleNotFoundError as exc:
        if exc.name == "uvicorn":
            raise RuntimeError(
                "vmon serve requires the server extra: pip install 'vmon[server]'"
            ) from exc
        raise

    resolved_token = token or os.environ.get("VMON_API_TOKEN")
    if not resolved_token:
        raise RuntimeError("vmon serve requires --token or VMON_API_TOKEN")
    app = create_app(token=resolved_token, idle_timeout=idle_timeout, host=host, port=port)

    from .daemon import Daemon, acquire_owner_lock, daemon_paths, parse_tcp_addr, release_owner_lock

    paths = daemon_paths()
    paths["home"].mkdir(parents=True, exist_ok=True)
    lock_fd = acquire_owner_lock(paths["lock"])
    if lock_fd is None:
        raise RuntimeError("a local vmond daemon is already running; run `vmon daemon stop` first")
    engine = app.state.supervisor._engine
    daemon = Daemon(
        engine,
        sock_path=paths["sock"],
        tcp_addr=parse_tcp_addr(os.environ.get("VMON_DAEMON_TCP")),
        token=resolved_token,
    )
    ready = threading.Event()
    threading.Thread(
        target=daemon.serve_forever, kwargs={"ready": ready}, name="vmond-serve", daemon=True
    ).start()
    if not ready.wait(5) or not paths["sock"].exists():
        daemon.shutdown()
        release_owner_lock(lock_fd)
        raise RuntimeError(f"failed to bind the local vmond socket at {paths['sock']}")
    paths["pid"].write_text(f"{os.getpid()}\n", encoding="utf-8")
    try:
        uvicorn.run(app, host=host, port=port)
    finally:
        daemon.shutdown()
        for path in (paths["sock"], paths["pid"]):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        release_owner_lock(lock_fd)
