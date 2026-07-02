"""Mesh membership, replica, and idempotency coordination routes."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request, status

from ..mesh import MeshError, NodeCaps, NodeState, request_template_key
from ..records import CreateRecord
from . import leases, migration, record_sync, templates
from .models import SandboxCreate, _apply_ha_defaults, _validate_create_request
from .proxy import _engine_has, _mesh_http_status, _scatter_locate
from .runtime import ServerRuntime

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class IdempotencyHandlers:
    sandbox_create: Callable[[Request, str, dict[str, Any]], Awaitable[dict[str, Any]]]
    run_create: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]]
    restore_create: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]]


def register_mesh_routes(
    app: FastAPI, ctx: ServerRuntime, require_auth: Any
) -> IdempotencyHandlers:
    supervisor = ctx.supervisor
    mesh = ctx.mesh
    replica_store = ctx.replica_store
    outbound_token = ctx.outbound_token or ""
    record_store = ctx.record_store


    def ensure_mesh_heartbeat() -> None:
        from .reconciler import ensure_mesh_heartbeat as start_mesh_heartbeat

        start_mesh_heartbeat(app, mesh)

    async def _migrate_sandbox_to(sandbox_id: str, target: str) -> dict[str, Any]:
        return await migration.migrate_sandbox_to(ctx, sandbox_id, target)


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

    def _drain_target() -> str | None:
        """The first healthy peer this node can migrate a sandbox to (never self)."""
        for peer in mesh.peers():
            try:
                mesh.migration_target(peer.node_id)
            except MeshError:
                continue
            return peer.node_id
        return None

    @app.post("/v1/mesh/leave", dependencies=[Depends(require_auth)])
    async def mesh_leave(body: dict[str, Any] | None = None) -> dict[str, Any]:
        """Leave the cluster. With ``{"drain": true}`` migrate locally-owned
        sandboxes to peers first; any that cannot move are reported and abandoned."""
        drained: list[dict[str, Any]] = []
        if bool((body or {}).get("drain")) and mesh.enabled:
            for sid in list(supervisor._engine.owned_ids()):
                target = _drain_target()
                if target is None:
                    drained.append(
                        {"sandbox": sid, "status": "skipped", "reason": "no compatible peer"}
                    )
                    continue
                try:
                    await _migrate_sandbox_to(sid, target)
                except Exception as exc:
                    detail = getattr(exc, "detail", str(exc))
                    drained.append({"sandbox": sid, "status": "failed", "reason": str(detail)})
                else:
                    drained.append({"sandbox": sid, "status": "migrated", "target": target})
        mesh.leave()
        return {"ok": True, "drained": drained}

    @app.get("/v1/mesh/status", dependencies=[Depends(require_auth)])
    async def mesh_status() -> dict[str, Any]:
        status_payload = mesh.status()
        status_payload["replicas_held"] = len(app.state.replica_store.list())
        return status_payload

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

    @app.get("/v1/mesh/reachable/{node_id}", dependencies=[Depends(require_auth)])
    async def mesh_reachable(node_id: str) -> dict[str, bool]:
        return {"reachable": mesh.is_peer_healthy(node_id)}

    @app.get("/v1/mesh/locate/{sandbox_id}", dependencies=[Depends(require_auth)])
    async def mesh_locate(sandbox_id: str) -> dict[str, Any]:
        if _engine_has(supervisor, sandbox_id):
            owner, epoch = mesh.authoritative_owner(sandbox_id)
            if owner is None:
                owner, epoch = mesh.node_id, mesh.local_epoch(sandbox_id)
            return {"owner": owner, "epoch": epoch}
        owner, epoch = mesh.authoritative_owner(sandbox_id)
        if owner is not None:
            return {"owner": owner, "epoch": epoch}
        record = record_store.get(sandbox_id)
        if record is not None:
            mesh.record_owner(sandbox_id, record.owner, record.epoch)
            return {"owner": record.owner, "epoch": record.epoch}
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="unknown sandbox")

    @app.post("/v1/mesh/owner", dependencies=[Depends(require_auth)])
    async def mesh_owner(body: dict[str, Any]) -> dict[str, bool]:
        sid = str(body.get("sid") or "")
        node_id = str(body.get("node_id") or "")
        epoch = int(body.get("epoch") or 0)
        if sid and node_id:
            mesh.record_owner(sid, node_id, epoch)
        return {"ok": True}

    @app.post("/v1/mesh/lease/grant", dependencies=[Depends(require_auth)])
    async def mesh_lease_grant(body: dict[str, Any]) -> dict[str, Any]:
        try:
            decision = ctx.lease_manager.vote_grant(
                str(body.get("volume") or ""),
                str(body.get("holder_node") or ""),
                int(body.get("epoch") or 0),
                float(body.get("ttl") or 0),
            )
        except (TypeError, ValueError) as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)) from exc
        return decision.to_wire()

    @app.post("/v1/mesh/lease/renew", dependencies=[Depends(require_auth)])
    async def mesh_lease_renew(body: dict[str, Any]) -> dict[str, Any]:
        try:
            decision = ctx.lease_manager.vote_renew(
                str(body.get("volume") or ""),
                str(body.get("holder_node") or ""),
                int(body.get("epoch") or 0),
                float(body.get("ttl") or 0),
            )
        except (TypeError, ValueError) as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)) from exc
        return decision.to_wire()

    @app.post("/v1/mesh/lease/release", dependencies=[Depends(require_auth)])
    async def mesh_lease_release(body: dict[str, Any]) -> dict[str, Any]:
        try:
            decision = ctx.lease_manager.vote_release(
                str(body.get("volume") or ""),
                str(body.get("holder_node") or ""),
                int(body.get("epoch") or 0),
            )
        except (TypeError, ValueError) as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)) from exc
        payload = decision.to_wire()
        payload["released"] = decision.granted
        return payload

    @app.post("/v1/mesh/migrate/receive", dependencies=[Depends(require_auth)])
    async def mesh_migrate_receive(request: Request, body: dict[str, Any]) -> dict[str, Any]:
        """Target side of a migration: pull the source's checkpoint and restore it."""
        digest = str(body.get("digest") or "")
        source_url = str(body.get("source_url") or "")
        params = dict(body.get("params") or {})
        name = str(params.get("name") or "")
        if not (digest and source_url and name):
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="digest, source_url, and params.name are required",
            )
        if _engine_has(supervisor, name):
            # A single-shot migration carries no idempotency token, so an existing
            # local name is a genuine collision, not a replay: reject it so the
            # source rolls back instead of committing onto an unrelated sandbox.
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                detail=f"sandbox {name!r} already exists on the target node",
            )
        try:
            installed = await templates.pull_template(
                request.app.state.mesh_http, source_url, digest, outbound_token
            )
        except Exception as exc:
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY,
                detail=f"failed to pull migration checkpoint: {exc}",
            ) from exc
        epoch = int(body.get("epoch") or 0)
        lease_records = await leases.acquire_writable_volume_leases(ctx, params, epoch=epoch)
        try:
            record = await supervisor._run(
                supervisor._engine.restore_from_template,
                params,
                str(installed),
                quorum_ok=True,
            )
        except BaseException:
            await leases.release_leases(ctx, lease_records)
            raise
        await asyncio.to_thread(supervisor._engine.record_volume_leases, record.name, lease_records)
        mesh.record_owner(name, mesh.node_id, epoch)
        return record.view()

    @app.post("/v1/mesh/replica/receive", dependencies=[Depends(require_auth)])
    async def mesh_replica_receive(request: Request, body: dict[str, Any]) -> dict[str, Any]:
        digest = str(body.get("digest") or "")
        source_url = str(body.get("source_url") or "")
        source_node = str(body.get("source_node") or "")
        params = dict(body.get("params") or {})
        name = str(params.get("name") or "")
        if not (digest and source_url and source_node and name):
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="digest, source_url, source_node, and params.name are required",
            )
        if _engine_has(supervisor, name):
            return {"ok": True, "skipped": "owner"}
        current = request.app.state.replica_store.get(name)
        if current is not None and current.digest == digest:
            return {"ok": True, "skipped": "current"}
        try:
            installed = await templates.pull_template(
                request.app.state.mesh_http, source_url, digest, outbound_token
            )
        except Exception as exc:
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY,
                detail=f"failed to pull replica checkpoint: {exc}",
            ) from exc
        replica_store.put(
            name,
            digest=digest,
            source_node=source_node,
            snapshot_dir=str(installed),
            params=params,
        )
        return {"ok": True}

    @app.get("/v1/mesh/replica/list", dependencies=[Depends(require_auth)])
    async def mesh_replica_list() -> dict[str, list[str]]:
        return {"sids": replica_store.list()}

    @app.post("/v1/mesh/record/put", dependencies=[Depends(require_auth)])
    async def mesh_record_put(body: dict[str, Any]) -> dict[str, bool]:
        try:
            record = CreateRecord.from_wire(body)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        record_store.put(record)
        mesh.record_owner(record.sid, record.owner, record.epoch)
        return {"ok": True}

    @app.post("/v1/mesh/record/remove", dependencies=[Depends(require_auth)])
    async def mesh_record_remove(body: dict[str, Any]) -> dict[str, bool]:
        sid = str(body.get("sid") or "")
        if sid:
            record_store.drop(sid)
            mesh.forget_owner(sid)
        return {"ok": True}

    def _idem_payload(body: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        key = str(body.get("key") or "").strip()
        params = body.get("params")
        if not key:
            raise MeshError("missing idempotency key")
        if not isinstance(params, dict):
            raise MeshError("missing idempotency params")
        clean = dict(params)
        clean.pop("idempotency_key", None)
        return key, clean

    def _prepare_create_params(params: dict[str, Any], kind: str) -> dict[str, Any]:
        clean = dict(params)
        clean.pop("idempotency_key", None)
        if mesh.enabled and clean.get("fs_dir"):
            raise MeshError(
                "host-local fs_dir cannot be placed or protected; use a volume",
                code="invalid",
            )
        try:
            _apply_ha_defaults(
                clean, mesh_enabled=mesh.enabled, config=getattr(ctx, "config", None)
            )
        except ValueError as exc:
            raise MeshError(str(exc), code="invalid") from exc
        clean["_kind"] = kind
        return clean

    def _view_with_ha(view: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
        out = dict(view)
        out["ha"] = str(params.get("ha") or "off")
        out["restart_policy"] = str(params.get("restart_policy") or "none")
        return out

    async def _commit_create_record(
        *,
        kind: str,
        view: dict[str, Any],
        owner: str,
        epoch: int,
        idem_key: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        sid = str(view.get("id") or view.get("name") or "")
        if not sid:
            return view
        if not mesh.enabled:
            return _view_with_ha(view, params)
        record = record_sync.make_create_record(
            sid=sid,
            owner=owner,
            epoch=epoch,
            idempotency_key=idem_key,
            params=params,
            kind=kind,
        )
        await record_sync.replicate_record(ctx, record, require_quorum=True)
        return record_sync.apply_view_detail(view, record)

    async def _local_sandbox_create_view(
        request: Request, params: dict[str, Any]
    ) -> dict[str, Any]:
        body = SandboxCreate(**params)
        _validate_create_request(body)
        clean = body.model_dump(exclude_none=True)
        clean.pop("idempotency_key", None)
        key = request_template_key(clean)
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
        lease_records = await leases.acquire_writable_volume_leases(
            ctx, body.model_dump(exclude_none=True), epoch=0
        )
        try:
            record = await supervisor.create(body)
        except BaseException:
            await leases.release_leases(ctx, lease_records)
            raise
        await asyncio.to_thread(supervisor._engine.record_volume_leases, record.name, lease_records)
        return record.view()

    async def _worker_sandbox_create(
        request: Request, idem_key: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        params = _prepare_create_params(params, "sandbox")
        existing = supervisor._engine.find_by_idempotency_key(idem_key)
        if existing is not None:
            return existing.view()
        if not mesh.worker_begin(idem_key):
            deadline = time.monotonic() + min(5.0, mesh.create_timeout)
            while time.monotonic() < deadline:
                await asyncio.sleep(0.05)
                existing = supervisor._engine.find_by_idempotency_key(idem_key)
                if existing is not None:
                    return existing.view()
            raise MeshError("idempotent create already in progress", code="conflict")
        try:
            existing = supervisor._engine.find_by_idempotency_key(idem_key)
            if existing is not None:
                return existing.view()
            name = params.get("name")
            if (
                isinstance(name, str)
                and name
                and (
                    _engine_has(supervisor, name)
                    or mesh.owner_of(name)
                    or await _scatter_locate(mesh, name, record_store)
                )
            ):
                raise MeshError(f"sandbox {name!r} already exists", code="conflict")
            view = await _local_sandbox_create_view(request, params)
            sid = str(view.get("id") or view.get("name") or "")
            if sid:
                supervisor._engine.record_idempotency(sid, idem_key)
                recorded = supervisor._engine.get(sid)
                return _view_with_ha(recorded.view(), params)
            return view
        finally:
            mesh.worker_end(idem_key)

    async def _coordinate_sandbox_create(
        request: Request, idem_key: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        params = _prepare_create_params(params, "sandbox")
        existing = supervisor._engine.find_by_idempotency_key(idem_key)
        if existing is not None:
            return await _commit_create_record(
                kind="sandbox",
                view=_view_with_ha(existing.view(), params),
                owner=mesh.node_id,
                epoch=mesh.local_epoch(existing.id),
                idem_key=idem_key,
                params=params,
            )
        tried: set[str] = set()
        for _ in range(max(1, len(mesh.peers()) + 1)):
            pinned = mesh.idem_owner(idem_key)
            owner = pinned or mesh.place(params)
            owner = mesh.idem_pin(idem_key, owner)
            if owner == mesh.node_id:
                view = await _worker_sandbox_create(request, idem_key, params)
                return await _commit_create_record(
                    kind="sandbox",
                    view=view,
                    owner=owner,
                    epoch=0,
                    idem_key=idem_key,
                    params=params,
                )
            peer_url = mesh.peer_url(owner)
            if peer_url is None:
                mesh.mark_unhealthy(owner)
                mesh.idem_unpin(idem_key)
                tried.add(owner)
                continue
            try:
                view = await asyncio.to_thread(
                    mesh.transport.post,
                    peer_url,
                    "/v1/mesh/idem/sandboxes/work",
                    {"key": idem_key, "params": params},
                    timeout=mesh.create_timeout,
                )
            except MeshError as exc:
                if exc.code == "unreachable":
                    mesh.mark_unhealthy(owner)
                    mesh.idem_unpin(idem_key)
                    tried.add(owner)
                    continue
                raise
            return await _commit_create_record(
                kind="sandbox",
                view=view,
                owner=owner,
                epoch=0,
                idem_key=idem_key,
                params=params,
            )
        raise MeshError("no reachable owner for idempotent create", code="unreachable")

    async def _idempotent_sandbox_create(
        request: Request, idem_key: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        if not mesh.enabled:
            return await _worker_sandbox_create(request, idem_key, params)
        if mesh.pinned_local(params):
            prepared = _prepare_create_params(params, "sandbox")
            view = await _worker_sandbox_create(request, idem_key, prepared)
            return await _commit_create_record(
                kind="sandbox",
                view=view,
                owner=mesh.node_id,
                epoch=mesh.local_epoch(str(view.get("id") or view.get("name") or "")),
                idem_key=idem_key,
                params=prepared,
            )
        coordinator = mesh.coordinator_for(idem_key)
        if coordinator == mesh.node_id:
            return await _coordinate_sandbox_create(request, idem_key, params)
        peer_url = mesh.peer_url(coordinator)
        if peer_url is None:
            mesh.mark_unhealthy(coordinator)
            return await _coordinate_sandbox_create(request, idem_key, params)
        try:
            view = await asyncio.to_thread(
                mesh.transport.post,
                peer_url,
                "/v1/mesh/idem/sandboxes/coordinate",
                {"key": idem_key, "params": params},
                timeout=mesh.create_timeout,
            )
            sid = str(view.get("id") or view.get("name") or "")
            if sid and await _scatter_locate(mesh, sid, record_store) is None:
                mesh.record_owner(sid, coordinator, 0)
            return view
        except MeshError as exc:
            if exc.code == "unreachable":
                mesh.mark_unhealthy(coordinator)
                return await _coordinate_sandbox_create(request, idem_key, params)
            raise

    @app.post("/v1/mesh/idem/sandboxes/coordinate", dependencies=[Depends(require_auth)])
    async def mesh_idem_sandbox_coordinate(
        request: Request, body: dict[str, Any]
    ) -> dict[str, Any]:
        try:
            idem_key, params = _idem_payload(body)
            return await _coordinate_sandbox_create(request, idem_key, params)
        except MeshError as exc:
            raise HTTPException(_mesh_http_status(exc), detail=exc.message) from exc

    @app.post("/v1/mesh/idem/sandboxes/work", dependencies=[Depends(require_auth)])
    async def mesh_idem_sandbox_work(request: Request, body: dict[str, Any]) -> dict[str, Any]:
        try:
            idem_key, params = _idem_payload(body)
            return await _worker_sandbox_create(request, idem_key, params)
        except MeshError as exc:
            raise HTTPException(_mesh_http_status(exc), detail=exc.message) from exc

    async def _local_detached_run_view(params: dict[str, Any]) -> dict[str, Any]:
        clean = dict(params)
        clean.pop("idempotency_key", None)
        clean["detach"] = True
        result = await supervisor._run(supervisor._engine.run, clean)
        sid = str(result.get("name") or "")
        if sid:
            record = supervisor._engine.get(sid)
            record.detail["ha"] = str(params.get("ha") or "off")
            record.detail["restart_policy"] = str(params.get("restart_policy") or "none")
            return record.view()
        return result

    async def _worker_run_create(idem_key: str, params: dict[str, Any]) -> dict[str, Any]:
        params = _prepare_create_params(params, "run")
        existing = supervisor._engine.find_by_idempotency_key(idem_key)
        if existing is not None:
            return existing.view()
        if not mesh.worker_begin(idem_key):
            deadline = time.monotonic() + min(5.0, mesh.create_timeout)
            while time.monotonic() < deadline:
                await asyncio.sleep(0.05)
                existing = supervisor._engine.find_by_idempotency_key(idem_key)
                if existing is not None:
                    return existing.view()
            raise MeshError("idempotent create already in progress", code="conflict")
        try:
            existing = supervisor._engine.find_by_idempotency_key(idem_key)
            if existing is not None:
                return existing.view()
            name = params.get("name")
            if (
                isinstance(name, str)
                and name
                and (
                    _engine_has(supervisor, name)
                    or mesh.owner_of(name)
                    or await _scatter_locate(mesh, name, record_store)
                )
            ):
                raise MeshError(f"sandbox {name!r} already exists", code="conflict")
            view = await _local_detached_run_view(params)
            sid = str(view.get("id") or view.get("name") or "")
            if sid:
                supervisor._engine.record_idempotency(sid, idem_key)
                recorded = supervisor._engine.get(sid)
                return _view_with_ha(recorded.view(), params)
            return view
        finally:
            mesh.worker_end(idem_key)

    async def _coordinate_run_create(idem_key: str, params: dict[str, Any]) -> dict[str, Any]:
        params = _prepare_create_params(params, "run")
        existing = supervisor._engine.find_by_idempotency_key(idem_key)
        if existing is not None:
            return await _commit_create_record(
                kind="run",
                view=_view_with_ha(existing.view(), params),
                owner=mesh.node_id,
                epoch=mesh.local_epoch(existing.id),
                idem_key=idem_key,
                params=params,
            )
        for _ in range(max(1, len(mesh.peers()) + 1)):
            pinned = mesh.idem_owner(idem_key)
            owner = pinned or mesh.place(params)
            owner = mesh.idem_pin(idem_key, owner)
            if owner == mesh.node_id:
                view = await _worker_run_create(idem_key, params)
                return await _commit_create_record(
                    kind="run",
                    view=view,
                    owner=owner,
                    epoch=0,
                    idem_key=idem_key,
                    params=params,
                )
            peer_url = mesh.peer_url(owner)
            if peer_url is None:
                mesh.mark_unhealthy(owner)
                mesh.idem_unpin(idem_key)
                continue
            try:
                view = await asyncio.to_thread(
                    mesh.transport.post,
                    peer_url,
                    "/v1/mesh/idem/run/work",
                    {"key": idem_key, "params": params},
                    timeout=mesh.create_timeout,
                )
            except MeshError as exc:
                if exc.code == "unreachable":
                    mesh.mark_unhealthy(owner)
                    mesh.idem_unpin(idem_key)
                    continue
                raise
            return await _commit_create_record(
                kind="run",
                view=view,
                owner=owner,
                epoch=0,
                idem_key=idem_key,
                params=params,
            )
        raise MeshError("no reachable owner for idempotent run", code="unreachable")

    async def _idempotent_run_create(idem_key: str, params: dict[str, Any]) -> dict[str, Any]:
        if not mesh.enabled:
            return await _worker_run_create(idem_key, params)
        if mesh.pinned_local(params):
            prepared = _prepare_create_params(params, "run")
            view = await _worker_run_create(idem_key, prepared)
            return await _commit_create_record(
                kind="run",
                view=view,
                owner=mesh.node_id,
                epoch=mesh.local_epoch(str(view.get("id") or view.get("name") or "")),
                idem_key=idem_key,
                params=prepared,
            )
        coordinator = mesh.coordinator_for(idem_key)
        if coordinator == mesh.node_id:
            return await _coordinate_run_create(idem_key, params)
        peer_url = mesh.peer_url(coordinator)
        if peer_url is None:
            mesh.mark_unhealthy(coordinator)
            return await _coordinate_run_create(idem_key, params)
        try:
            return await asyncio.to_thread(
                mesh.transport.post,
                peer_url,
                "/v1/mesh/idem/run/coordinate",
                {"key": idem_key, "params": params},
                timeout=mesh.create_timeout,
            )
        except MeshError as exc:
            if exc.code == "unreachable":
                mesh.mark_unhealthy(coordinator)
                return await _coordinate_run_create(idem_key, params)
            raise

    @app.post("/v1/mesh/idem/run/coordinate", dependencies=[Depends(require_auth)])
    async def mesh_idem_run_coordinate(body: dict[str, Any]) -> dict[str, Any]:
        try:
            idem_key, params = _idem_payload(body)
            params["detach"] = True
            return await _coordinate_run_create(idem_key, params)
        except MeshError as exc:
            raise HTTPException(_mesh_http_status(exc), detail=exc.message) from exc

    @app.post("/v1/mesh/idem/run/work", dependencies=[Depends(require_auth)])
    async def mesh_idem_run_work(body: dict[str, Any]) -> dict[str, Any]:
        try:
            idem_key, params = _idem_payload(body)
            params["detach"] = True
            return await _worker_run_create(idem_key, params)
        except MeshError as exc:
            raise HTTPException(_mesh_http_status(exc), detail=exc.message) from exc

    async def _local_detached_restore_view(params: dict[str, Any]) -> dict[str, Any]:
        clean = dict(params)
        clean.pop("idempotency_key", None)
        clean["detach"] = True
        result = await supervisor._run(supervisor._engine.restore, clean)
        sid = str(result.get("name") or "")
        if sid:
            record = supervisor._engine.get(sid)
            record.detail["ha"] = str(params.get("ha") or "off")
            record.detail["restart_policy"] = str(params.get("restart_policy") or "none")
            return record.view()
        return result

    async def _worker_restore_create(idem_key: str, params: dict[str, Any]) -> dict[str, Any]:
        params = _prepare_create_params(params, "restore")
        existing = supervisor._engine.find_by_idempotency_key(idem_key)
        if existing is not None:
            return existing.view()
        if not mesh.worker_begin(idem_key):
            deadline = time.monotonic() + min(5.0, mesh.create_timeout)
            while time.monotonic() < deadline:
                await asyncio.sleep(0.05)
                existing = supervisor._engine.find_by_idempotency_key(idem_key)
                if existing is not None:
                    return existing.view()
            raise MeshError("idempotent create already in progress", code="conflict")
        try:
            existing = supervisor._engine.find_by_idempotency_key(idem_key)
            if existing is not None:
                return existing.view()
            name = params.get("name")
            if (
                isinstance(name, str)
                and name
                and (
                    _engine_has(supervisor, name)
                    or mesh.owner_of(name)
                    or await _scatter_locate(mesh, name, record_store)
                )
            ):
                raise MeshError(f"sandbox {name!r} already exists", code="conflict")
            view = await _local_detached_restore_view(params)
            sid = str(view.get("id") or view.get("name") or "")
            if sid:
                supervisor._engine.record_idempotency(sid, idem_key)
                recorded = supervisor._engine.get(sid)
                return _view_with_ha(recorded.view(), params)
            return view
        finally:
            mesh.worker_end(idem_key)

    async def _coordinate_restore_create(idem_key: str, params: dict[str, Any]) -> dict[str, Any]:
        params = _prepare_create_params(params, "restore")
        existing = supervisor._engine.find_by_idempotency_key(idem_key)
        if existing is not None:
            return await _commit_create_record(
                kind="restore",
                view=_view_with_ha(existing.view(), params),
                owner=mesh.node_id,
                epoch=mesh.local_epoch(existing.id),
                idem_key=idem_key,
                params=params,
            )
        for _ in range(max(1, len(mesh.peers()) + 1)):
            pinned = mesh.idem_owner(idem_key)
            owner = pinned or mesh.place(params)
            owner = mesh.idem_pin(idem_key, owner)
            if owner == mesh.node_id:
                view = await _worker_restore_create(idem_key, params)
                return await _commit_create_record(
                    kind="restore",
                    view=view,
                    owner=owner,
                    epoch=0,
                    idem_key=idem_key,
                    params=params,
                )
            peer_url = mesh.peer_url(owner)
            if peer_url is None:
                mesh.mark_unhealthy(owner)
                mesh.idem_unpin(idem_key)
                continue
            try:
                view = await asyncio.to_thread(
                    mesh.transport.post,
                    peer_url,
                    "/v1/mesh/idem/restore/work",
                    {"key": idem_key, "params": params},
                    timeout=mesh.create_timeout,
                )
            except MeshError as exc:
                if exc.code == "unreachable":
                    mesh.mark_unhealthy(owner)
                    mesh.idem_unpin(idem_key)
                    continue
                raise
            return await _commit_create_record(
                kind="restore",
                view=view,
                owner=owner,
                epoch=0,
                idem_key=idem_key,
                params=params,
            )
        raise MeshError("no reachable owner for idempotent restore", code="unreachable")

    async def _idempotent_restore_create(idem_key: str, params: dict[str, Any]) -> dict[str, Any]:
        if not mesh.enabled:
            return await _worker_restore_create(idem_key, params)
        if mesh.pinned_local(params):
            prepared = _prepare_create_params(params, "restore")
            view = await _worker_restore_create(idem_key, prepared)
            return await _commit_create_record(
                kind="restore",
                view=view,
                owner=mesh.node_id,
                epoch=mesh.local_epoch(str(view.get("id") or view.get("name") or "")),
                idem_key=idem_key,
                params=prepared,
            )
        coordinator = mesh.coordinator_for(idem_key)
        if coordinator == mesh.node_id:
            return await _coordinate_restore_create(idem_key, params)
        peer_url = mesh.peer_url(coordinator)
        if peer_url is None:
            mesh.mark_unhealthy(coordinator)
            return await _coordinate_restore_create(idem_key, params)
        try:
            return await asyncio.to_thread(
                mesh.transport.post,
                peer_url,
                "/v1/mesh/idem/restore/coordinate",
                {"key": idem_key, "params": params},
                timeout=mesh.create_timeout,
            )
        except MeshError as exc:
            if exc.code == "unreachable":
                mesh.mark_unhealthy(coordinator)
                return await _coordinate_restore_create(idem_key, params)
            raise

    @app.post("/v1/mesh/idem/restore/coordinate", dependencies=[Depends(require_auth)])
    async def mesh_idem_restore_coordinate(body: dict[str, Any]) -> dict[str, Any]:
        try:
            idem_key, params = _idem_payload(body)
            params["detach"] = True
            return await _coordinate_restore_create(idem_key, params)
        except MeshError as exc:
            raise HTTPException(_mesh_http_status(exc), detail=exc.message) from exc

    @app.post("/v1/mesh/idem/restore/work", dependencies=[Depends(require_auth)])
    async def mesh_idem_restore_work(body: dict[str, Any]) -> dict[str, Any]:
        try:
            idem_key, params = _idem_payload(body)
            params["detach"] = True
            return await _worker_restore_create(idem_key, params)
        except MeshError as exc:
            raise HTTPException(_mesh_http_status(exc), detail=exc.message) from exc


    return IdempotencyHandlers(
        sandbox_create=_idempotent_sandbox_create,
        run_create=_idempotent_run_create,
        restore_create=_idempotent_restore_create,
    )
