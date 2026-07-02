"""Server-side helpers for synchronous mesh create-record replication."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from ..mesh import MeshError
from ..records import CreateRecord, restart_policy_for_ha
from .runtime import ServerRuntime

LOGGER = logging.getLogger(__name__)


def make_create_record(
    *,
    sid: str,
    owner: str,
    epoch: int,
    idempotency_key: str,
    params: dict[str, Any],
    kind: str,
) -> CreateRecord:
    """Build a durable create record from normalized request params."""

    record_params = dict(params)
    record_params.pop("idempotency_key", None)
    record_params["_kind"] = kind
    ha = str(record_params.get("ha") or "off")
    restart_policy = str(record_params.get("restart_policy") or restart_policy_for_ha(ha))
    record_params["restart_policy"] = restart_policy
    return CreateRecord(
        sid=sid,
        params=record_params,
        owner=owner,
        epoch=int(epoch),
        idempotency_key=idempotency_key,
        ha=ha,
        restart_policy=restart_policy,
    )


def apply_view_detail(view: dict[str, Any], record: CreateRecord) -> dict[str, Any]:
    """Surface durability tier fields in a just-created view."""

    out = dict(view)
    out["ha"] = record.ha
    out["restart_policy"] = record.restart_policy
    return out


async def replicate_record(
    ctx: ServerRuntime, record: CreateRecord, *, require_quorum: bool = True
) -> None:
    """Store a create record locally and POST it to enough expected members.

    On meshes of >=3 expected members the create is acked only after a strict
    majority holds the record. A 2-node mesh cannot form a post-failure
    majority, so its tier is documented as weaker: every LIVE peer must ack
    (split-brain safe while both live), and with no live peer the record is
    accepted locally so a survivor keeps serving creates.
    """

    mesh = ctx.mesh
    ctx.record_store.put(record)
    mesh.record_owner(record.sid, record.owner, record.epoch)
    acks = 1
    live_peer_acks_needed = 0
    for peer in mesh.peers():
        if peer.healthy(time.time(), mesh.interval):
            live_peer_acks_needed += 1
        try:
            await asyncio.to_thread(
                mesh.transport.post,
                peer.advertise,
                "/v1/mesh/record/put",
                record.to_wire(),
                timeout=mesh.create_timeout,
            )
        except Exception as exc:
            LOGGER.warning(
                "create-record %s not replicated to %s: %s", record.sid, peer.node_id, exc
            )
            continue
        acks += 1
    expected = mesh.expected_members()
    if expected <= 2:
        needed = 1 + live_peer_acks_needed
    else:
        needed = mesh.quorum_needed()
    if require_quorum and acks < needed:
        raise MeshError(
            f"create record for {record.sid!r} replicated to {acks}/{needed} required members",
            code="record_unreplicated",
        )


async def update_record_owner(
    ctx: ServerRuntime, sid: str, owner: str, epoch: int, *, require_quorum: bool = False
) -> CreateRecord | None:
    """Persist and rebroadcast an ownership change for an existing create record."""

    record = ctx.record_store.update_owner(sid, owner, epoch)
    if record is None:
        return None
    await replicate_record(ctx, record, require_quorum=require_quorum)
    return record


async def reconcile_records(ctx: ServerRuntime) -> None:
    """Best-effort re-push of locally-owned create records to live peers.

    A peer that was partitioned or down during the create never saw the record;
    without anti-entropy it would answer "unknown sid" after the owner dies,
    breaking the acked-create guarantee. Runs each reconcile cycle; records are
    tiny and pushes are idempotent.
    """

    mesh = ctx.mesh
    now = time.time()
    live_peers = [peer for peer in mesh.peers() if peer.healthy(now, mesh.interval)]
    if not live_peers:
        return
    for record in ctx.record_store.list():
        if record.owner != mesh.node_id:
            continue
        for peer in live_peers:
            try:
                await asyncio.to_thread(
                    mesh.transport.post,
                    peer.advertise,
                    "/v1/mesh/record/put",
                    record.to_wire(),
                    timeout=mesh.create_timeout,
                )
            except Exception:
                continue


async def remove_record(ctx: ServerRuntime, sid: str) -> None:
    """Drop a create record locally and best-effort remove it from peers."""

    ctx.record_store.drop(sid)
    ctx.mesh.forget_owner(sid)
    for peer in ctx.mesh.peers():
        try:
            await asyncio.to_thread(
                ctx.mesh.transport.post,
                peer.advertise,
                "/v1/mesh/record/remove",
                {"sid": sid},
                timeout=ctx.mesh.create_timeout,
            )
        except Exception:
            continue
