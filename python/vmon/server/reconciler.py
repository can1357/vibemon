"""Background replication, HA reconciliation, and app lifespan loops."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import time
from collections.abc import AsyncIterator
from typing import Any

from fastapi import FastAPI

from ..mesh import Mesh
from . import leases, record_sync
from .models import SandboxCreate
from .proxy import _engine_has
from .runtime import ServerRuntime

LOGGER = logging.getLogger(__name__)


async def replicate_forever(ctx: ServerRuntime) -> None:
    supervisor = ctx.supervisor
    mesh = ctx.mesh
    cadence = ctx.config.effective_replicate_sec(mesh_enabled=True)
    last: dict[str, str] = {}

    async def push_replica(
        semaphore: asyncio.Semaphore, peer_id: str, digest: str, params: dict[str, Any]
    ) -> bool:
        url = mesh.peer_url(peer_id)
        if not url:
            return False
        async with semaphore:
            try:
                await asyncio.to_thread(
                    mesh.transport.post,
                    url,
                    "/v1/mesh/replica/receive",
                    {
                        "digest": digest,
                        "source_url": mesh.advertise,
                        "source_node": mesh.node_id,
                        "params": params,
                    },
                    timeout=mesh.create_timeout,
                )
            except Exception:
                LOGGER.exception("failed to replicate sandbox to peer %s", peer_id)
                return False
        return True

    while True:
        await asyncio.sleep(cadence)
        if not mesh.enabled:
            continue
        k = ctx.config.replicas
        semaphore = asyncio.Semaphore(ctx.config.replicate_concurrency)
        owned_ids = list(supervisor._engine.owned_ids())
        owned = set(owned_ids)
        for stale_sid in (set(last) | set(ctx.checkpoint_times)) - owned:
            last.pop(stale_sid, None)
            ctx.checkpoint_times.pop(stale_sid, None)
        for sid in owned_ids:
            digest = ""
            snapshot_dir = ""
            try:
                ha = _ha_for_sid(ctx, sid)
                if ha == "off":
                    last.pop(sid, None)
                    ctx.checkpoint_times.pop(sid, None)
                    continue
                try:
                    prep = await supervisor._run(supervisor._engine.replicate_prepare, sid)
                except Exception as exc:
                    LOGGER.warning("replicate skip %s: %s", sid, exc)
                    continue
                digest = str(prep["digest"])
                snapshot_dir = str(prep["snapshot_dir"])
                params = dict(prep["params"])
                # Saves redundant peer transfer/install only; the per-cadence checkpoint
                # quiesce is inherent — tune via VMON_REPLICATE_SEC.
                if digest == last.get(sid):
                    continue
                targets = mesh.replica_targets(sid, k)
                LOGGER.info("replicate %s digest=%s targets=%s", sid, digest[:12], targets)
                pushes = [push_replica(semaphore, peer_id, digest, params) for peer_id in targets]
                if not pushes:
                    continue
                results = await asyncio.gather(*pushes)
                if any(results):
                    mesh.note_event("replication")
                    ctx.checkpoint_times[sid] = time.time()
                if all(results):
                    last[sid] = digest
            except Exception:
                LOGGER.exception("replication cycle failed for %s", sid)
            finally:
                if digest and snapshot_dir:
                    with contextlib.suppress(Exception):
                        await supervisor._run(
                            supervisor._engine.replicate_cleanup, digest, snapshot_dir
                        )


def _ha_for_sid(ctx: ServerRuntime, sid: str) -> str:
    record = ctx.record_store.get(sid)
    if record is not None:
        return record.ha
    try:
        local = ctx.supervisor._engine.get(sid)
    except Exception:
        return "async"
    return str(local.detail.get("ha") or "async")


def _can_rerun(ctx: ServerRuntime, sid: str) -> bool:
    record = ctx.record_store.get(sid)
    if record is None:
        return False
    return record.restart_policy == "rerun" or "rerun" in record.ha


async def _rerun_from_record(ctx: ServerRuntime, sid: str, dead: str) -> bool:
    record = ctx.record_store.get(sid)
    if record is None or not (record.restart_policy == "rerun" or "rerun" in record.ha):
        return False
    mesh = ctx.mesh
    supervisor = ctx.supervisor
    key = f"rerun:{sid}"
    if not mesh.worker_begin(key):
        return True
    try:
        epoch = mesh.next_epoch(sid)
        await asyncio.to_thread(mesh.broadcast_owner, sid, mesh.node_id, epoch)
        params = dict(record.params)
        kind = str(params.pop("_kind", "sandbox") or "sandbox")
        params["name"] = sid
        params["ha"] = record.ha
        params["restart_policy"] = record.restart_policy
        if kind == "run":
            params["detach"] = True
            result = await supervisor._run(supervisor._engine.run, params)
            created_sid = str(result.get("name") or sid)
        elif kind == "restore":
            params["detach"] = True
            result = await supervisor._run(supervisor._engine.restore, params)
            created_sid = str(result.get("name") or sid)
        else:
            created = await supervisor.create(SandboxCreate(**params))
            created_sid = created.id
        local = supervisor._engine.get(created_sid)
        local.detail["ha"] = record.ha
        local.detail["restart_policy"] = record.restart_policy
        supervisor._engine.record_idempotency(created_sid, record.idempotency_key)
        await record_sync.update_record_owner(
            ctx, created_sid, mesh.node_id, epoch, require_quorum=False
        )
        mesh.note_event("restore")
        LOGGER.info("re-ran orphaned sandbox %s from create record after %s died", sid, dead)
        return True
    finally:
        mesh.worker_end(key)


async def ha_reconcile_forever(ctx: ServerRuntime) -> None:
    supervisor = ctx.supervisor
    mesh = ctx.mesh
    replica_store = ctx.replica_store
    while True:
        await asyncio.sleep(max(1.0, mesh.interval))
        if not mesh.enabled:
            continue
        try:
            await leases.renew_writable_volume_leases_once(ctx)
        except Exception:
            LOGGER.exception("failed to renew writable-volume leases")
        try:
            await record_sync.reconcile_records(ctx)
        except Exception:
            LOGGER.exception("failed to reconcile create records")
        for sid, dead in mesh.drain_orphans():
            retry = False
            try:
                owner = mesh.owner_of(sid)
                if (
                    owner
                    and owner != dead
                    and (owner == mesh.node_id or mesh.is_peer_healthy(owner))
                ):
                    continue  # already restored under a live owner
                if mesh.is_peer_healthy(dead):
                    LOGGER.warning(
                        "deferring restore of %s: prior owner %s is still reachable", sid, dead
                    )
                    retry = True
                    continue
                if mesh.restore_owner(sid, exclude={dead}) != mesh.node_id:
                    # Another live node is elected; keep the pair so this node can
                    # take over if the current electee dies before restoring.
                    retry = True
                    continue
                create_record = ctx.record_store.get(sid)
                ha = create_record.ha if create_record is not None else "async"
                can_rerun = create_record is not None and (
                    create_record.restart_policy == "rerun" or "rerun" in create_record.ha
                )
                if ha == "off":
                    continue
                restore_quorum = ctx.config.restore_quorum_enabled(
                    expected_members=mesh.expected_members()
                )
                if restore_quorum:
                    # Quorum restore needs >=3 nodes to tolerate 1 failure; in a
                    # 2-node cluster, losing one prevents a majority and defers.
                    confirmations = 1
                    for member_id in mesh.live_member_ids():
                        if member_id == mesh.node_id:
                            continue
                        url = mesh.peer_url(member_id)
                        if not url:
                            continue
                        try:
                            resp = await asyncio.to_thread(
                                mesh.transport.get, url, f"/v1/mesh/reachable/{dead}"
                            )
                        except Exception:
                            continue
                        if resp.get("reachable") is False:
                            confirmations += 1
                    if not mesh.restore_quorum_met(confirmations):
                        LOGGER.warning(
                            "deferring restore of %s: quorum not met (%d/%d)",
                            sid,
                            confirmations,
                            mesh.quorum_needed(),
                        )
                        retry = True
                        continue
                if _engine_has(supervisor, sid):
                    continue
                allow_checkpoint = ha in {"async", "async+rerun"}
                replica_ready = (
                    allow_checkpoint
                    and replica_store.holds(sid)
                    and replica_store.secrets_ready(sid)
                )
                if replica_ready:
                    key = f"restore:{sid}"
                    if not mesh.worker_begin(key):
                        retry = True
                        continue
                    try:
                        rec = replica_store.get(sid)
                        if rec is None:
                            LOGGER.warning("cannot auto-restore %s: no local replica", sid)
                            retry = True
                            continue
                        epoch = mesh.next_epoch(sid)
                        lease_records = await leases.acquire_writable_volume_leases(
                            ctx, rec.params, epoch=epoch
                        )
                        try:
                            restored = await supervisor._run(
                                supervisor._engine.restore_from_template,
                                rec.params,
                                rec.snapshot_dir,
                                quorum_ok=restore_quorum,
                            )
                        except BaseException:
                            await leases.release_leases(ctx, lease_records)
                            raise
                        if create_record is not None:
                            restored.detail["ha"] = create_record.ha
                            restored.detail["restart_policy"] = create_record.restart_policy
                        await asyncio.to_thread(
                            supervisor._engine.record_volume_leases, restored.name, lease_records
                        )
                        await asyncio.to_thread(mesh.broadcast_owner, sid, mesh.node_id, epoch)
                        await record_sync.update_record_owner(
                            ctx, sid, mesh.node_id, epoch, require_quorum=False
                        )
                        replica_store.drop(sid)
                        mesh.note_event("restore")
                        LOGGER.info(
                            "restored orphaned sandbox %s from replica after %s died", sid, dead
                        )
                        continue
                    finally:
                        mesh.worker_end(key)
                if can_rerun and await _rerun_from_record(ctx, sid, dead):
                    continue
                if not replica_store.holds(sid):
                    LOGGER.warning("cannot auto-restore %s: no local replica (will retry)", sid)
                elif not replica_store.secrets_ready(sid):
                    LOGGER.warning(
                        "cannot auto-restore %s: replica secrets unavailable after restart",
                        sid,
                    )
                retry = True
            except Exception:
                retry = True
                LOGGER.exception("failed to restore orphaned sandbox %s from %s", sid, dead)
            finally:
                if retry:
                    mesh.requeue_orphan(sid, dead)
        for sid in mesh.fenced_local_ids(list(supervisor._engine.owned_ids())):
            try:
                record = supervisor._engine.get(sid)
                await supervisor._run(supervisor._engine.remove, sid)
                await leases.release_record_volume_leases(ctx, record)
                mesh.forget_local_owner(sid)
                mesh.note_event("fence")
                LOGGER.warning("fenced %s: superseded by a higher-epoch owner", sid)
            except Exception:
                LOGGER.exception("failed to fence superseded sandbox %s", sid)


def ensure_mesh_heartbeat(app: FastAPI, mesh: Mesh) -> None:
    thread = getattr(app.state, "mesh_thread", None)
    if thread is not None and thread.is_alive():
        return
    stop = threading.Event()
    app.state.mesh_stop = stop
    app.state.mesh_thread = threading.Thread(
        target=mesh.run_heartbeat, args=(stop,), name="vmon-mesh-hb", daemon=True
    )
    app.state.mesh_thread.start()


def make_lifespan(ctx: ServerRuntime):
    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        import httpx

        from .. import sandbox as sandbox_mod

        app.state.mesh_http = httpx.AsyncClient(timeout=None)
        app.state.replica_store = ctx.replica_store
        try:
            for warm_image in ctx.config.warm_images:
                await asyncio.to_thread(sandbox_mod.prewarm, warm_image.ref, count=warm_image.count)
            ctx.mesh.load()
            ctx.replica_store.load()
            ctx.record_store.load()
            if ctx.mesh.enabled:
                ensure_mesh_heartbeat(app, ctx.mesh)
                app.state.ha_reconcile_task = asyncio.create_task(ha_reconcile_forever(ctx))
                if ctx.config.effective_replicate_sec(mesh_enabled=True) > 0:
                    app.state.replicate_task = asyncio.create_task(replicate_forever(ctx))
            app.state.reaper_task = asyncio.create_task(ctx.supervisor.reap_forever())
            yield
        finally:
            stop = getattr(app.state, "mesh_stop", None)
            if stop is not None:
                stop.set()
            for task_name in ("reaper_task", "replicate_task", "ha_reconcile_task"):
                task = getattr(app.state, task_name, None)
                if task is not None:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            await asyncio.to_thread(sandbox_mod.shutdown_all_pools)
            await app.state.mesh_http.aclose()
            close = getattr(ctx.mesh.transport, "close", None)
            if close is not None:
                close()

    return lifespan
