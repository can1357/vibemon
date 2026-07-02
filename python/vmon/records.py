"""Durable create-record replication for mesh sandboxes."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .core import state_dir

__all__ = ["ALLOWED_HA", "CreateRecord", "RecordStore", "restart_policy_for_ha"]

ALLOWED_HA = frozenset({"off", "async", "rerun", "async+rerun"})


def restart_policy_for_ha(ha: str) -> str:
    """Return the restart policy implied by a durability tier."""

    return "rerun" if "rerun" in ha else "none"


@dataclass
class CreateRecord:
    """Synchronous mesh metadata for an acknowledged create."""

    sid: str
    params: dict[str, Any]
    owner: str
    epoch: int
    idempotency_key: str
    ha: str
    restart_policy: str
    created_at: float = field(default_factory=time.time)

    def to_wire(self) -> dict[str, Any]:
        """Return a JSON-ready representation for disk and peer replication."""

        return {
            "sid": self.sid,
            "params": dict(self.params),
            "owner": self.owner,
            "epoch": int(self.epoch),
            "idempotency_key": self.idempotency_key,
            "ha": self.ha,
            "restart_policy": self.restart_policy,
            "created_at": float(self.created_at),
        }

    @classmethod
    def from_wire(cls, data: dict[str, Any]) -> "CreateRecord":
        """Build and validate a record from persisted or peer-provided data."""

        sid = data.get("sid")
        params = data.get("params")
        owner = data.get("owner")
        idempotency_key = data.get("idempotency_key")
        ha = data.get("ha") or "off"
        restart_policy = data.get("restart_policy") or restart_policy_for_ha(str(ha))
        if not isinstance(sid, str) or not sid:
            raise ValueError("record sid is required")
        if not isinstance(params, dict):
            raise ValueError("record params must be an object")
        if not isinstance(owner, str) or not owner:
            raise ValueError("record owner is required")
        if not isinstance(idempotency_key, str) or not idempotency_key:
            raise ValueError("record idempotency_key is required")
        ha = str(ha)
        if ha not in ALLOWED_HA:
            raise ValueError(f"invalid ha tier {ha!r}")
        return cls(
            sid=sid,
            params=dict(params),
            owner=owner,
            epoch=int(data.get("epoch") or 0),
            idempotency_key=idempotency_key,
            ha=ha,
            restart_policy=str(restart_policy),
            created_at=float(data.get("created_at") or time.time()),
        )


class RecordStore:
    """Persist create records while keeping secret env material memory-only."""

    def __init__(self, root: Path | None = None) -> None:
        self._root = root or state_dir() / "records"
        self._meta: dict[str, dict[str, Any]] = {}
        self._secrets: dict[str, Any] = {}

    @staticmethod
    def _split_secrets(params: dict[str, Any]) -> tuple[dict[str, Any], Any | None]:
        clean = dict(params)
        return clean, clean.pop("secrets", None)

    def load(self) -> None:
        """Load persisted records, ignoring corrupt or incomplete files."""

        self._meta = {}
        self._secrets = {}
        try:
            paths = list(self._root.glob("*.json"))
        except OSError:
            return
        for path in paths:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                record = CreateRecord.from_wire(data)
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                continue
            clean, _ = self._split_secrets(record.params)
            meta = record.to_wire()
            meta["params"] = clean
            self._meta[record.sid] = meta

    def put(self, record: CreateRecord) -> None:
        """Persist or replace a create record atomically."""

        clean, secrets = self._split_secrets(record.params)
        if secrets is not None:
            self._secrets[record.sid] = secrets
        elif record.sid in self._secrets:
            self._secrets.pop(record.sid, None)
        meta = record.to_wire()
        meta["params"] = clean
        self._meta[record.sid] = meta
        self._root.mkdir(parents=True, exist_ok=True)
        path = self._root / f"{record.sid}.json"
        tmp = self._root / f"{record.sid}.json.tmp"
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(meta, fh, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, path)

    def get(self, sid: str) -> CreateRecord | None:
        """Return a create record, reattaching in-memory secrets if still present."""

        meta = self._meta.get(sid)
        if meta is None:
            return None
        data = dict(meta)
        params = dict(data.get("params") or {})
        if sid in self._secrets:
            params["secrets"] = self._secrets[sid]
        data["params"] = params
        try:
            return CreateRecord.from_wire(data)
        except (TypeError, ValueError):
            return None

    def update_owner(self, sid: str, owner: str, epoch: int) -> CreateRecord | None:
        """Update a record's authoritative owner and persist the replacement."""

        record = self.get(sid)
        if record is None:
            return None
        record.owner = owner
        record.epoch = int(epoch)
        self.put(record)
        return record

    def list(self) -> list[CreateRecord]:
        """Return all valid records in stable sandbox-id order."""

        records = [record for sid in sorted(self._meta) if (record := self.get(sid)) is not None]
        return records

    def drop(self, sid: str) -> None:
        """Remove a create record and any memory-only secret material."""

        self._meta.pop(sid, None)
        self._secrets.pop(sid, None)
        try:
            (self._root / f"{sid}.json").unlink()
        except FileNotFoundError:
            pass
