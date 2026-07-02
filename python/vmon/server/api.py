"""Public sandbox, run, restore, fork, and control-plane API routes."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import secrets
import time
from typing import Any
from urllib.parse import urlencode

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, WebSocket, status
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse

from ..core import VMRecord
from ..mesh import MeshError, request_template_key
from . import leases, migration, proxy, record_sync, templates
from .auth import (
    _bearer_token_authorized,
    _request_connect_token,
    _ws_bearer_token,
    _ws_connect_token,
)
from .auth import (
    require_connect_token as _require_connect_token,
)
from .mesh_routes import IdempotencyHandlers
from .models import (
    ExecRequest,
    ExtendRequest,
    NetworkRequest,
    SandboxCreate,
    SnapshotRequest,
    SnapshotTemplateRequest,
    _apply_ha_defaults,
    _validate_arch_param,
    _validate_create_request,
    _validate_exec_request,
    _validate_extend_request,
    _validate_network_request,
)
from .proxy import _engine_has, _scatter_locate, coded_http, mesh_http_exception
from .runtime import ServerRuntime
from .supervisor import (
    _creator_sse,
    _exec_sse,
    _exec_ws,
    _logs_sse,
    _query_shell_params,
    _shell_ws,
    _ws_exec_request,
)

LOGGER = logging.getLogger(__name__)
_TAG_QUERY = Query(None)


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


def register_api_routes(
    app: FastAPI, ctx: ServerRuntime, require_auth: Any, idem: IdempotencyHandlers
) -> None:
    supervisor = ctx.supervisor
    mesh = ctx.mesh
    expected_token = ctx.expected_token
    outbound_token = ctx.outbound_token or ""
    client_token = ctx.client_token
    _idempotent_sandbox_create = idem.sandbox_create
    _idempotent_run_create = idem.run_create
    _idempotent_restore_create = idem.restore_create

    @app.get("/healthz")
    async def healthz() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/metrics", dependencies=[Depends(require_auth)])
    async def metrics() -> PlainTextResponse:
        return PlainTextResponse(supervisor.metrics_text(), media_type="text/plain; version=0.0.4")

    async def proxy_ws_owner(websocket: WebSocket, sandbox_id: str) -> bool:
        return await proxy.proxy_ws_owner(ctx, websocket, sandbox_id)

    def events_stream(request: Request):
        return proxy.events_stream(ctx, request)

    def require_connect_token(record: VMRecord, supplied: str | None) -> None:
        _require_connect_token(ctx, record, supplied)

    def place_or_http(params: dict[str, Any]) -> str:
        try:
            return mesh.place(params)
        except MeshError as exc:
            raise mesh_http_exception(exc) from exc

    @app.post("/v1/sandboxes", dependencies=[Depends(require_auth)])
    async def create_sandbox(request: Request, body: SandboxCreate) -> JSONResponse:
        _validate_create_request(body)
        body_dict = body.model_dump(exclude_none=True)
        idem_key = str(body_dict.pop("idempotency_key", "") or "").strip()
        body.idempotency_key = None
        if mesh.enabled and not request.headers.get("x-vmon-mesh-hop") and not idem_key:
            idem_key = secrets.token_urlsafe(18)
        if idem_key:
            try:
                view = await _idempotent_sandbox_create(request, idem_key, body_dict)
            except MeshError as exc:
                raise mesh_http_exception(exc) from exc
            return JSONResponse(view, status_code=status.HTTP_201_CREATED)
        if (
            mesh.enabled
            and not request.headers.get("x-vmon-mesh-hop")
            and not mesh.pinned_local(body_dict)
        ):
            name = body_dict.get("name")
            if isinstance(name, str) and (
                _engine_has(supervisor, name)
                or mesh.owner_of(name)
                or await _scatter_locate(mesh, name, ctx.record_store)
            ):
                raise coded_http(
                    status.HTTP_409_CONFLICT, "conflict", f"sandbox {name!r} already exists"
                )
            tried: set[str] = set()
            for _ in range(2):
                owner = place_or_http(body_dict)
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
                except MeshError as exc:
                    if exc.code == "unreachable":
                        mesh.mark_unhealthy(owner)
                        continue
                    raise mesh_http_exception(exc) from exc
                finally:
                    mesh.note_inflight(-1)
                sid = str(view.get("id") or view.get("name") or "")
                if sid:
                    mesh.record_owner(sid, owner, 0)
                return JSONResponse(view, status_code=status.HTTP_201_CREATED)
        key = request_template_key(body_dict)
        local_index_fn = getattr(supervisor._engine, "template_index", None)
        local_index = local_index_fn() if callable(local_index_fn) else {}
        if mesh.enabled and key and key not in local_index:
            provider = mesh.find_template_provider(key)
            if provider is not None:
                peer_url, digest = provider
                try:
                    installed = await templates.pull_template_metadata(
                        request.app.state.mesh_http, peer_url, digest, outbound_token
                    )
                    body.template = str(installed)
                    body.remote_page_url = templates._remote_page_url(peer_url, digest)
                    body.remote_page_token = outbound_token
                    body.remote_page_digest = digest
                except Exception as exc:
                    LOGGER.warning(
                        "lazy pull-warm metadata %s from %s failed: %s", digest, peer_url, exc
                    )
                    try:
                        installed = await templates.pull_template(
                            request.app.state.mesh_http, peer_url, digest, outbound_token
                        )
                        # Warm-restore the pulled snapshot directly: selecting it by path
                        # skips local image preparation on this node.
                        body.template = str(installed)
                    except Exception as full_exc:
                        LOGGER.warning(
                            "pull-warm template %s from %s failed: %s",
                            digest,
                            peer_url,
                            full_exc,
                        )
        local_params = body.model_dump(exclude_none=True)
        local_params.pop("idempotency_key", None)
        if mesh.enabled and local_params.get("fs_dir"):
            raise coded_http(
                status.HTTP_400_BAD_REQUEST,
                "invalid",
                "host-local fs_dir cannot be placed or protected; use a volume",
            )
        try:
            _apply_ha_defaults(
                local_params, mesh_enabled=mesh.enabled, config=getattr(ctx, "config", None)
            )
        except ValueError as exc:
            raise coded_http(status.HTTP_400_BAD_REQUEST, "invalid", str(exc)) from exc
        body = SandboxCreate(**local_params)
        lease_records = await leases.acquire_writable_volume_leases(ctx, local_params, epoch=0)
        try:
            record = await supervisor.create(body)
        except BaseException:
            await leases.release_leases(ctx, lease_records)
            raise
        await asyncio.to_thread(supervisor._engine.record_volume_leases, record.name, lease_records)
        return JSONResponse(record.view(), status_code=status.HTTP_201_CREATED)

    @app.get("/v1/sandboxes", dependencies=[Depends(require_auth)])
    async def list_sandboxes(
        request: Request,
        tag: list[str] | None = _TAG_QUERY,
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
        _validate_arch_param(params)
        idem_key = str(params.pop("idempotency_key", "") or "").strip()
        params["detach"] = detach or bool(params.get("detach"))
        if (
            params["detach"]
            and mesh.enabled
            and not request.headers.get("x-vmon-mesh-hop")
            and not idem_key
        ):
            idem_key = secrets.token_urlsafe(18)
        if params["detach"] and idem_key:
            try:
                return JSONResponse(await _idempotent_run_create(idem_key, params))
            except MeshError as exc:
                raise mesh_http_exception(exc) from exc
        if (
            mesh.enabled
            and not request.headers.get("x-vmon-mesh-hop")
            and not mesh.pinned_local(params)
        ):
            owner = place_or_http(params)
            if owner != mesh.node_id:
                url = mesh.peer_url(owner)
                if url is None:
                    raise HTTPException(
                        status.HTTP_503_SERVICE_UNAVAILABLE, "sandbox owner unreachable"
                    )
                return await proxy._proxy_to_peer(request, url, outbound_token)
        if params["detach"]:
            try:
                _apply_ha_defaults(
                    params, mesh_enabled=mesh.enabled, config=getattr(ctx, "config", None)
                )
            except ValueError as exc:
                raise coded_http(status.HTTP_400_BAD_REQUEST, "invalid", str(exc)) from exc
            result = await supervisor._run(supervisor._engine.run, params)
            sid = str(result.get("name") or "")
            if sid:
                record = supervisor._engine.get(sid)
                record.detail["ha"] = str(params.get("ha") or "off")
                record.detail["restart_policy"] = str(params.get("restart_policy") or "none")
                return JSONResponse(record.view())
            return JSONResponse(result)
        return StreamingResponse(
            _creator_sse(supervisor, "run", params, request), media_type="text/event-stream"
        )

    @app.post("/v1/restore", dependencies=[Depends(require_auth)])
    async def restore_gateway(request: Request) -> Response:
        params = dict(await request.json())
        _validate_arch_param(params)
        idem_key = str(params.pop("idempotency_key", "") or "").strip()
        if (
            params.get("detach")
            and mesh.enabled
            and not request.headers.get("x-vmon-mesh-hop")
            and not idem_key
        ):
            idem_key = secrets.token_urlsafe(18)
        if params.get("detach") and idem_key:
            try:
                return JSONResponse(await _idempotent_restore_create(idem_key, params))
            except MeshError as exc:
                raise mesh_http_exception(exc) from exc
        if mesh.enabled and not request.headers.get("x-vmon-mesh-hop"):
            owner = place_or_http(params)
            if owner != mesh.node_id:
                url = mesh.peer_url(owner)
                if url is None:
                    raise HTTPException(
                        status.HTTP_503_SERVICE_UNAVAILABLE, "sandbox owner unreachable"
                    )
                return await proxy._proxy_to_peer(request, url, outbound_token)
        if params.get("detach"):
            try:
                _apply_ha_defaults(
                    params, mesh_enabled=mesh.enabled, config=getattr(ctx, "config", None)
                )
            except ValueError as exc:
                raise coded_http(status.HTTP_400_BAD_REQUEST, "invalid", str(exc)) from exc
            result = await supervisor._run(supervisor._engine.restore, params)
            sid = str(result.get("name") or "")
            if sid:
                record = supervisor._engine.get(sid)
                record.detail["ha"] = str(params.get("ha") or "off")
                record.detail["restart_policy"] = str(params.get("restart_policy") or "none")
                return JSONResponse(record.view())
            return JSONResponse(result)
        return StreamingResponse(
            _creator_sse(supervisor, "restore", params, request), media_type="text/event-stream"
        )

    @app.post("/v1/fork", dependencies=[Depends(require_auth)])
    async def fork_gateway(request: Request) -> dict[str, Any]:
        params = dict(await request.json())
        _validate_arch_param(params)
        if mesh.enabled and not request.headers.get("x-vmon-mesh-hop"):
            owner = place_or_http(params)
            if owner != mesh.node_id:
                url = mesh.peer_url(owner)
                if url is None:
                    raise HTTPException(
                        status.HTTP_503_SERVICE_UNAVAILABLE, "sandbox owner unreachable"
                    )
                try:
                    response = await asyncio.to_thread(mesh.transport.post, url, "/v1/fork", params)
                except MeshError as exc:
                    raise mesh_http_exception(exc) from exc
                for clone in response.get("clones") or []:
                    if isinstance(clone, dict) and clone.get("name"):
                        mesh.record_owner(str(clone["name"]), owner, 0)
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
        await leases.release_record_volume_leases(ctx, record)
        await record_sync.remove_record(ctx, sandbox_id)
        return record.view()

    @app.post("/v1/sandboxes/{sandbox_id}/pause", dependencies=[Depends(require_auth)])
    async def pause_sandbox(sandbox_id: str) -> dict[str, Any]:
        return await supervisor.pause(sandbox_id)

    @app.post("/v1/sandboxes/{sandbox_id}/resume", dependencies=[Depends(require_auth)])
    async def resume_sandbox(sandbox_id: str) -> dict[str, Any]:
        return await supervisor.resume(sandbox_id)

    @app.post("/v1/sandboxes/{sandbox_id}/stop", dependencies=[Depends(require_auth)])
    async def stop_sandbox(sandbox_id: str) -> dict[str, Any]:
        record = supervisor.get(sandbox_id)
        result = await supervisor.stop(sandbox_id)
        await leases.release_record_volume_leases(ctx, record)
        mesh.forget_owner(sandbox_id)
        return result

    @app.delete("/v1/sandboxes/{sandbox_id}/remove", dependencies=[Depends(require_auth)])
    async def remove_sandbox(sandbox_id: str) -> dict[str, Any]:
        record = supervisor.get(sandbox_id)
        result = await supervisor.remove(sandbox_id)
        await leases.release_record_volume_leases(ctx, record)
        await record_sync.remove_record(ctx, sandbox_id)
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

    @app.get("/v1/sandboxes/{sandbox_id}/metrics", dependencies=[Depends(require_auth)])
    async def metrics_sandbox(sandbox_id: str) -> dict[str, Any]:
        record = supervisor.get(sandbox_id, require_running=True)
        return await supervisor.metrics(record)

    async def _migrate_sandbox_to(sandbox_id: str, target: str) -> dict[str, Any]:
        return await migration.migrate_sandbox_to(ctx, sandbox_id, target)

    @app.post("/v1/sandboxes/{sandbox_id}/migrate", dependencies=[Depends(require_auth)])
    async def migrate_sandbox(sandbox_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Migrate a sandbox to another mesh node (owner-routed here, to the source).

        Offline, snapshot-based: a block-network sandbox with no host-local state is
        checkpointed here, restored on the target, then dropped here once the target
        confirms. A failed target never loses the VM (the source is restored).
        """
        if not mesh.enabled:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="migration requires mesh mode")
        target = str(body.get("target") or "")
        if not target:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT, detail="target node id is required"
            )
        return await _migrate_sandbox_to(sandbox_id, target)

    @app.post("/v1/sandboxes/{sandbox_id}/exec", dependencies=[Depends(require_auth)])
    async def exec_sandbox(sandbox_id: str, body: ExecRequest) -> StreamingResponse:
        _validate_exec_request(body)
        record = supervisor.get(sandbox_id, require_running=True)
        return StreamingResponse(
            _exec_sse(supervisor, record, body), media_type="text/event-stream"
        )

    @app.websocket("/v1/sandboxes/{sandbox_id}/exec/ws")
    async def exec_sandbox_ws(websocket: WebSocket, sandbox_id: str) -> None:
        if not _bearer_token_authorized(_ws_bearer_token(websocket), expected_token, client_token):
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
        if not _bearer_token_authorized(_ws_bearer_token(websocket), expected_token, client_token):
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
                    target = proxy._split_http_authority(url)
                except MeshError as exc:
                    await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason=exc.message)
                    return
                await proxy._proxy_websocket(
                    websocket,
                    target,
                    websocket.url.path,
                    extra_headers={
                        "Authorization": f"Bearer {outbound_token}",
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
        return await proxy._proxy_http_request(request, target, rest)

    @app.websocket("/v1/sandboxes/{sandbox_id}/ports/{port:int}/ws/{rest:path}")
    async def proxy_ws_port(websocket: WebSocket, sandbox_id: str, port: int, rest: str) -> None:
        if not _bearer_token_authorized(_ws_bearer_token(websocket), expected_token, client_token):
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
        await proxy._proxy_websocket(websocket, target, rest)

    @app.get("/v1/events", dependencies=[Depends(require_auth)])
    async def events(request: Request) -> StreamingResponse:
        return StreamingResponse(events_stream(request), media_type="text/event-stream")

    @app.post("/v1/sandboxes/{sandbox_id}/snapshot", dependencies=[Depends(require_auth)])
    async def snapshot_sandbox(sandbox_id: str, body: SnapshotRequest) -> dict[str, str]:
        record = supervisor.get(sandbox_id, require_running=True)
        image = await supervisor.snapshot(record, body.name)
        return {"image": image}
