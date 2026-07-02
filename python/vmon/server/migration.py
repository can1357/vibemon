"""Offline sandbox migration helper shared by API and mesh leave routes."""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import status

from ..mesh import MeshError
from . import leases, record_sync
from .proxy import coded_http, mesh_http_exception
from .runtime import ServerRuntime


async def migrate_sandbox_to(ctx: ServerRuntime, sandbox_id: str, target: str) -> dict[str, Any]:
    supervisor = ctx.supervisor
    mesh = ctx.mesh

    """Run an offline migration of a locally-owned sandbox to ``target``.

    Checkpoint here, restore on the target, remap ownership cluster-wide, then
    drop the source. Any target-side failure restores the source locally from
    the same checkpoint, so a failed migration never loses the VM. Raises
    :class:`HTTPException` on failure.
    """
    try:
        peer = mesh.migration_target(target)
    except MeshError as exc:
        raise mesh_http_exception(exc) from exc
    prep = await supervisor._run(supervisor._engine.migrate_prepare, sandbox_id)
    digest = str(prep["digest"])
    snapshot_dir = str(prep["snapshot_dir"])
    params = dict(prep["params"])
    epoch = mesh.next_epoch(sandbox_id)
    receive = {
        "digest": digest,
        "source_url": mesh.advertise,
        "params": params,
        "epoch": epoch,
    }
    try:
        view = await asyncio.to_thread(
            mesh.transport.post,
            peer.advertise,
            "/v1/mesh/migrate/receive",
            receive,
            timeout=mesh.create_timeout,
        )
    except Exception as exc:
        await supervisor._run(
            supervisor._engine.migrate_abort, sandbox_id, snapshot_dir, digest, params
        )
        detail = exc.message if isinstance(exc, MeshError) else str(exc)
        raise coded_http(
            status.HTTP_502_BAD_GATEWAY,
            "unreachable",
            f"migration to {target} failed (source restored): {detail}",
        ) from exc
    await asyncio.to_thread(mesh.broadcast_owner, sandbox_id, target, epoch)
    await record_sync.update_record_owner(ctx, sandbox_id, target, epoch, require_quorum=False)
    source_record = supervisor._engine.get(sandbox_id)
    await leases.release_record_volume_leases(ctx, source_record)
    await supervisor._run(supervisor._engine.migrate_commit, sandbox_id, snapshot_dir, digest)
    return view
