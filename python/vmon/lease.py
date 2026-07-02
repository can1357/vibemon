"""Quorum-fenced leases for mesh writable volumes.

Each node stores only its own vote under ``$VMON_HOME/leases/<volume>.json``
with ``{holder, epoch, granted_at, ttl}``.  A requester may write only after it
has collected persisted grants from a strict majority of the mesh's *expected*
members.

TTL self-fencing is the safety rule that prevents overlapping writers when a
holder is partitioned: for every successful grant or renewal,
``renew_deadline = granted_at + ttl / 2`` and a different holder cannot receive
a vote until ``successor_grant_allowed_at = granted_at + ttl``.  Because
``ttl > 0``, ``renew_deadline < successor_grant_allowed_at``.  A holder that
cannot renew by the deadline must stop its writers before any successor can
activate, while a silent/partitioned holder's old votes age out only after the
full TTL.
"""

from __future__ import annotations

import json
import math
import os
import re
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

STATE = Path(os.environ.get("VMON_HOME", str(Path.home() / ".vmon")))
LEASE_DIR = STATE / "leases"
DEFAULT_TTL = 30.0

_NAME_RE = re.compile(r"^[a-z0-9_][a-z0-9_.-]{0,63}$")


class LeaseUnavailable(RuntimeError):
    """A quorum lease could not be acquired or renewed."""

    code = "lease_unavailable"

    def __init__(self, message: str, *, votes: int = 0, needed: int = 0) -> None:
        super().__init__(message)
        self.message = message
        self.votes = votes
        self.needed = needed


@dataclass(frozen=True)
class LeaseRecord:
    """One node's persisted vote for a writable-volume lease."""

    volume: str
    holder: str
    epoch: int
    granted_at: float
    ttl: float

    @property
    def expires_at(self) -> float:
        return self.granted_at + self.ttl

    @property
    def renew_deadline(self) -> float:
        return self.granted_at + (self.ttl / 2.0)

    def to_disk(self) -> dict[str, Any]:
        return {
            "holder": self.holder,
            "epoch": self.epoch,
            "granted_at": self.granted_at,
            "ttl": self.ttl,
        }

    def to_wire(self) -> dict[str, Any]:
        return {
            "volume": self.volume,
            **self.to_disk(),
            "expires_at": self.expires_at,
            "renew_deadline": self.renew_deadline,
        }

    @classmethod
    def from_disk(cls, volume: str, data: Mapping[str, Any]) -> LeaseRecord:
        return cls(
            volume=volume,
            holder=str(data["holder"]),
            epoch=int(data["epoch"]),
            granted_at=float(data["granted_at"]),
            ttl=_valid_ttl(data["ttl"]),
        )


@dataclass(frozen=True)
class LeaseDecision:
    """The result of one node's local vote."""

    granted: bool
    record: LeaseRecord | None = None
    reason: str = ""

    def to_wire(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"granted": self.granted}
        if self.reason:
            payload["reason"] = self.reason
        if self.record is not None:
            payload["record"] = self.record.to_wire()
        return payload


PostVote = Callable[[str, str, dict[str, Any]], dict[str, Any]]


def _valid_volume(volume: str) -> str:
    value = str(volume or "")
    if not _NAME_RE.fullmatch(value):
        raise ValueError(f"invalid volume name {volume!r}: must match {_NAME_RE.pattern}")
    return value


def _valid_holder(holder: str) -> str:
    value = str(holder or "")
    if not value:
        raise ValueError("lease holder_node must be non-empty")
    return value


def _valid_epoch(epoch: int | str) -> int:
    value = int(epoch)
    if value < 0:
        raise ValueError("lease epoch must be non-negative")
    return value


def _valid_ttl(ttl: float | int | str) -> float:
    value = float(ttl)
    if not math.isfinite(value) or value <= 0.0:
        raise ValueError("lease ttl must be a positive finite number")
    return value


class LeaseManager:
    """Persist local lease votes and collect strict-majority mesh grants."""

    def __init__(
        self,
        root: str | os.PathLike[str] | None = None,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.root = (
            Path(root)
            if root is not None
            else Path(os.environ.get("VMON_HOME", STATE)) / "leases"
        )
        self._clock = clock or time.time
        self._lock = threading.RLock()

    # -- local persisted vote state -------------------------------------

    def current(self, volume: str) -> LeaseRecord | None:
        """Return this node's current vote for *volume*, if any."""
        with self._lock:
            return self._load(_valid_volume(volume))

    def active_votes(self, *, now: float | None = None) -> list[LeaseRecord]:
        """Return unexpired local vote records, useful for assertions/status."""
        at = self._clock() if now is None else float(now)
        with self._lock:
            out: list[LeaseRecord] = []
            if not self.root.is_dir():
                return out
            for path in self.root.glob("*.json"):
                volume = path.stem
                try:
                    record = self._load(volume)
                except Exception:
                    continue
                if record is not None and at < record.expires_at:
                    out.append(record)
            return sorted(out, key=lambda rec: rec.volume)

    def vote_grant(
        self, volume: str, holder_node: str, epoch: int, ttl: float
    ) -> LeaseDecision:
        """Persist a yes vote iff no unexpired conflicting or newer vote exists."""
        volume = _valid_volume(volume)
        holder = _valid_holder(holder_node)
        epoch = _valid_epoch(epoch)
        ttl = _valid_ttl(ttl)
        with self._lock:
            now = self._clock()
            existing = self._load(volume)
            if existing is not None and now < existing.expires_at:
                if epoch < existing.epoch:
                    return LeaseDecision(False, existing, "stale_epoch")
                if existing.holder != holder:
                    return LeaseDecision(False, existing, "conflict")
            record = LeaseRecord(volume, holder, epoch, now, ttl)
            self._store(record)
            return LeaseDecision(True, record)

    def vote_renew(
        self, volume: str, holder_node: str, epoch: int, ttl: float
    ) -> LeaseDecision:
        """Extend a matching vote before its renew deadline."""
        volume = _valid_volume(volume)
        holder = _valid_holder(holder_node)
        epoch = _valid_epoch(epoch)
        ttl = _valid_ttl(ttl)
        with self._lock:
            now = self._clock()
            existing = self._load(volume)
            if existing is None:
                return LeaseDecision(False, None, "missing")
            if existing.holder != holder or existing.epoch != epoch:
                reason = "stale_epoch" if epoch < existing.epoch else "mismatch"
                return LeaseDecision(False, existing, reason)
            if now >= existing.expires_at:
                return LeaseDecision(False, existing, "expired")
            if now > existing.renew_deadline:
                return LeaseDecision(False, existing, "renew_deadline")
            record = LeaseRecord(volume, holder, epoch, now, ttl)
            self._store(record)
            return LeaseDecision(True, record)

    def vote_release(self, volume: str, holder_node: str, epoch: int) -> LeaseDecision:
        """Clear a matching local vote without allowing stale releases to fence successors."""
        volume = _valid_volume(volume)
        holder = _valid_holder(holder_node)
        epoch = _valid_epoch(epoch)
        with self._lock:
            existing = self._load(volume)
            if existing is None:
                return LeaseDecision(True, None)
            if existing.holder == holder and existing.epoch == epoch:
                path = self._path(volume)
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
                return LeaseDecision(True, None)
            reason = "stale_epoch" if epoch < existing.epoch else "mismatch"
            return LeaseDecision(False, existing, reason)

    # -- requester-collected majority operations -------------------------

    def request_grant(
        self,
        *,
        volume: str,
        holder_node: str,
        epoch: int,
        ttl: float,
        self_node: str,
        peer_urls: Mapping[str, str],
        expected_members: int,
        post: PostVote,
    ) -> LeaseRecord:
        """Collect a strict-majority grant from this node and every known peer."""
        return self._request_majority(
            "grant",
            volume=volume,
            holder_node=holder_node,
            epoch=epoch,
            ttl=ttl,
            self_node=self_node,
            peer_urls=peer_urls,
            expected_members=expected_members,
            post=post,
            cleanup_on_failure=True,
        )

    def request_renew(
        self,
        *,
        volume: str,
        holder_node: str,
        epoch: int,
        ttl: float,
        self_node: str,
        peer_urls: Mapping[str, str],
        expected_members: int,
        post: PostVote,
    ) -> LeaseRecord:
        """Renew an existing lease by strict majority."""
        return self._request_majority(
            "renew",
            volume=volume,
            holder_node=holder_node,
            epoch=epoch,
            ttl=ttl,
            self_node=self_node,
            peer_urls=peer_urls,
            expected_members=expected_members,
            post=post,
            cleanup_on_failure=False,
        )

    def request_release(
        self,
        *,
        volume: str,
        holder_node: str,
        epoch: int,
        self_node: str,
        peer_urls: Mapping[str, str],
        post: PostVote,
    ) -> None:
        """Best-effort explicit release on every known member, including self."""
        volume = _valid_volume(volume)
        holder = _valid_holder(holder_node)
        epoch = _valid_epoch(epoch)
        self.vote_release(volume, holder, epoch)
        payload = {"volume": volume, "holder_node": holder, "epoch": epoch}
        for node_id, url in peer_urls.items():
            if node_id == self_node:
                continue
            try:
                post(url, "/v1/mesh/lease/release", payload)
            except Exception:
                continue

    def _request_majority(
        self,
        op: str,
        *,
        volume: str,
        holder_node: str,
        epoch: int,
        ttl: float,
        self_node: str,
        peer_urls: Mapping[str, str],
        expected_members: int,
        post: PostVote,
        cleanup_on_failure: bool,
    ) -> LeaseRecord:
        volume = _valid_volume(volume)
        holder = _valid_holder(holder_node)
        epoch = _valid_epoch(epoch)
        ttl = _valid_ttl(ttl)
        expected = int(expected_members)
        if expected < 1:
            raise ValueError("expected_members must be positive")
        needed = expected // 2 + 1
        payload = {"volume": volume, "holder_node": holder, "epoch": epoch, "ttl": ttl}
        local_decision = (
            self.vote_grant(volume, holder, epoch, ttl)
            if op == "grant"
            else self.vote_renew(volume, holder, epoch, ttl)
        )
        votes = 1 if local_decision.granted else 0
        record = local_decision.record if local_decision.granted else None
        granted_nodes: set[str] = {self_node} if local_decision.granted else set()
        path = f"/v1/mesh/lease/{op}"
        for node_id, url in peer_urls.items():
            if node_id == self_node:
                continue
            try:
                response = post(url, path, payload)
            except Exception:
                continue
            if response.get("granted") is True:
                votes += 1
                granted_nodes.add(node_id)
                if record is None:
                    wire = response.get("record")
                    if isinstance(wire, Mapping):
                        record = LeaseRecord.from_disk(volume, wire)
        if votes >= needed and local_decision.granted and record is not None:
            return record
        if cleanup_on_failure and granted_nodes:
            self.request_release(
                volume=volume,
                holder_node=holder,
                epoch=epoch,
                self_node=self_node,
                peer_urls={node: url for node, url in peer_urls.items() if node in granted_nodes},
                post=post,
            )
        reason = local_decision.reason or "quorum"
        raise LeaseUnavailable(
            f"lease {op} for volume {volume!r} got {votes}/{needed} votes ({reason})",
            votes=votes,
            needed=needed,
        )

    # -- disk helpers ------------------------------------------------------

    def _path(self, volume: str) -> Path:
        return self.root / f"{_valid_volume(volume)}.json"

    def _load(self, volume: str) -> LeaseRecord | None:
        path = self._path(volume)
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        if not isinstance(data, dict):
            raise ValueError(f"invalid lease record in {path!s}")
        return LeaseRecord.from_disk(volume, data)

    def _store(self, record: LeaseRecord) -> None:
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        path = self._path(record.volume)
        tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        tmp.write_text(json.dumps(record.to_disk(), sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp, path)
