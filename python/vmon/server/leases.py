"""Server-side orchestration for writable-volume mesh leases."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException, status

from ..lease import DEFAULT_TTL, LeaseRecord, LeaseUnavailable
from ..mesh import Mesh
from .proxy import coded_http
from .runtime import ServerRuntime

LOGGER = logging.getLogger(__name__)


def writable_volume_names(params: Mapping[str, Any]) -> list[str]:
    """Names mounted read-write by at least one mountpoint in create params."""
    volumes = params.get("volumes")
    if not isinstance(volumes, Mapping):
        return []
    writable: dict[str, bool] = {}
    for spec in volumes.values():
        if isinstance(spec, Mapping):
            name = spec.get("name")
            if not name:
                continue
            read_only = bool(spec.get("read_only"))
        else:
            name = spec
            read_only = False
        vname = str(name)
        writable[vname] = writable.get(vname, False) or not read_only
    return sorted(vname for vname, is_writable in writable.items() if is_writable)


def expected_members(mesh: Mesh) -> int:
    """Expected mesh size; falls back to known membership if older Mesh lacks it."""
    return int(getattr(mesh, "_expected_members", len(mesh.peers()) + 1))


def peer_urls(mesh: Mesh) -> dict[str, str]:
    return {peer.node_id: peer.advertise for peer in mesh.peers()}


def _lease_ttl(ctx: ServerRuntime) -> float:
    # ConfigSurface owns the public config surface; use the contract default unless
    # a config field is present in a newer split.
    return float(getattr(ctx.config, "volume_lease_ttl_sec", DEFAULT_TTL))


def _post(ctx: ServerRuntime, url: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
    return ctx.mesh.transport.post(url, path, payload, timeout=ctx.mesh.create_timeout)


def _lease_unavailable(exc: LeaseUnavailable) -> HTTPException:
    return coded_http(status.HTTP_503_SERVICE_UNAVAILABLE, exc.code, exc.message)


async def acquire_writable_volume_leases(
    ctx: ServerRuntime,
    params: Mapping[str, Any],
    *,
    holder_node: str | None = None,
    epoch: int = 0,
) -> list[LeaseRecord]:
    """Acquire majority leases for all writable volumes in *params* before boot."""
    if not ctx.mesh.enabled:
        return []
    volumes = writable_volume_names(params)
    if not volumes:
        return []
    expected = expected_members(ctx.mesh)
    if expected < 3:
        raise coded_http(
            status.HTTP_501_NOT_IMPLEMENTED,
            "unsupported",
            "writable volumes need >=3 nodes on mesh contexts",
        )
    holder = holder_node or ctx.mesh.node_id
    peers = peer_urls(ctx.mesh)
    granted: list[LeaseRecord] = []
    try:
        for volume in volumes:
            lease = await asyncio.to_thread(
                ctx.lease_manager.request_grant,
                volume=volume,
                holder_node=holder,
                epoch=epoch,
                ttl=_lease_ttl(ctx),
                self_node=ctx.mesh.node_id,
                peer_urls=peers,
                expected_members=expected,
                post=lambda url, path, payload: _post(ctx, url, path, payload),
            )
            granted.append(lease)
    except LeaseUnavailable as exc:
        await release_leases(ctx, granted)
        raise _lease_unavailable(exc) from exc
    except BaseException:
        await release_leases(ctx, granted)
        raise
    return granted


async def release_leases(ctx: ServerRuntime, leases: list[LeaseRecord]) -> None:
    """Best-effort release for already-materialized LeaseRecord objects."""
    if not leases:
        return
    peers = peer_urls(ctx.mesh)
    for lease in leases:
        await asyncio.to_thread(
            ctx.lease_manager.request_release,
            volume=lease.volume,
            holder_node=lease.holder,
            epoch=lease.epoch,
            self_node=ctx.mesh.node_id,
            peer_urls=peers,
            post=lambda url, path, payload: _post(ctx, url, path, payload),
        )


async def release_record_volume_leases(ctx: ServerRuntime, record_or_sid: Any) -> None:
    """Release every lease recorded for a sandbox and clear its metadata."""
    try:
        record = (
            ctx.supervisor._engine.get(str(record_or_sid))
            if isinstance(record_or_sid, str)
            else record_or_sid
        )
    except Exception:
        return
    raw = getattr(record, "detail", {}).get("volume_leases")
    if not isinstance(raw, Mapping):
        return
    leases: list[LeaseRecord] = []
    for volume, data in raw.items():
        if not isinstance(data, Mapping):
            continue
        try:
            leases.append(
                LeaseRecord(
                    volume=str(data.get("volume") or volume),
                    holder=str(data["holder"]),
                    epoch=int(data["epoch"]),
                    granted_at=float(data["granted_at"]),
                    ttl=float(data["ttl"]),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    await release_leases(ctx, leases)
    await asyncio.to_thread(ctx.supervisor._engine.clear_volume_leases, record.name)


async def renew_writable_volume_leases_once(ctx: ServerRuntime) -> None:
    """Renew local writable-volume leases, stopping writers that miss the deadline."""
    if not ctx.mesh.enabled:
        return
    peers = peer_urls(ctx.mesh)
    expected = expected_members(ctx.mesh)
    for record in ctx.supervisor._engine.volume_lease_records():
        raw = record.detail.get("volume_leases")
        if not isinstance(raw, Mapping):
            continue
        refreshed: list[LeaseRecord] = []
        should_stop = False
        stop_reason = ""
        for volume in ctx.supervisor._engine.writable_volume_names(record):
            data = raw.get(volume)
            if not isinstance(data, Mapping):
                continue
            holder = str(data.get("holder") or "")
            if holder != ctx.mesh.node_id:
                continue
            epoch = int(data.get("epoch") or 0)
            ttl = float(data.get("ttl") or _lease_ttl(ctx))
            granted_at = float(data.get("granted_at") or 0.0)
            renew_deadline = float(data.get("renew_deadline") or (granted_at + ttl / 2.0))
            try:
                lease = await asyncio.to_thread(
                    ctx.lease_manager.request_renew,
                    volume=volume,
                    holder_node=holder,
                    epoch=epoch,
                    ttl=ttl,
                    self_node=ctx.mesh.node_id,
                    peer_urls=peers,
                    expected_members=expected,
                    post=lambda url, path, payload: _post(ctx, url, path, payload),
                )
            except LeaseUnavailable as exc:
                if time.time() >= renew_deadline:
                    should_stop = True
                    stop_reason = exc.message
                    break
                LOGGER.warning(
                    "lease renew deferred for %s/%s: %s", record.name, volume, exc.message
                )
            else:
                refreshed.append(lease)
        if should_stop:
            LOGGER.warning(
                "stopping %s after writable-volume lease renewal failure: %s",
                record.name,
                stop_reason,
            )
            try:
                await ctx.supervisor._run(ctx.supervisor._engine.stop, record.name)
            finally:
                await release_record_volume_leases(ctx, record)
        elif refreshed:
            await asyncio.to_thread(
                ctx.supervisor._engine.record_volume_leases, record.name, refreshed
            )
