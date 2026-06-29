"""Persistence for cold HA replica metadata."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .core import state_dir

__all__ = ["ReplicaRecord", "ReplicaStore"]


@dataclass
class ReplicaRecord:
    """Metadata for one held replica checkpoint."""

    sid: str
    digest: str
    source_node: str
    snapshot_dir: str
    params: dict[str, Any]
    needs_secrets: bool = False


class ReplicaStore:
    """Persist replica metadata while keeping restore secrets in memory only."""

    def __init__(self, root: Path | None = None) -> None:
        """Create a store rooted at ``root`` or the default replica state dir."""

        self._root = root or state_dir() / "replicas"
        self._meta: dict[str, dict[str, Any]] = {}
        self._secrets: dict[str, list[Any]] = {}

    def load(self) -> None:
        """Load persisted replica metadata, skipping missing or corrupt files."""

        self._meta = {}
        self._secrets = {}
        try:
            paths = list(self._root.glob("*.json"))
        except OSError:
            return
        for path in paths:
            try:
                with path.open("r", encoding="utf-8") as fh:
                    meta = json.load(fh)
            except OSError, json.JSONDecodeError:
                continue
            if not isinstance(meta, dict):
                continue
            sid = meta.get("sid")
            params = meta.get("params")
            if not isinstance(sid, str) or not isinstance(params, dict):
                continue
            self._meta[sid] = meta

    def put(
        self,
        sid: str,
        *,
        digest: str,
        source_node: str,
        snapshot_dir: str,
        params: dict[str, Any],
    ) -> None:
        """Persist replica metadata for ``sid`` without writing secrets to disk."""

        secrets = params.get("secrets")
        if secrets is not None:
            self._secrets[sid] = secrets
        else:
            self._secrets.pop(sid, None)
        meta = {
            "sid": sid,
            "digest": digest,
            "source_node": source_node,
            "snapshot_dir": snapshot_dir,
            "params": {k: v for k, v in params.items() if k != "secrets"},
            "needs_secrets": secrets is not None,
        }
        self._meta[sid] = meta
        self._root.mkdir(parents=True, exist_ok=True)
        path = self._root / f"{sid}.json"
        tmp = self._root / f"{sid}.json.tmp"
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(meta, fh, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, path)

    def get(self, sid: str) -> ReplicaRecord | None:
        """Return the record for ``sid``, reattaching in-memory secrets if any."""

        meta = self._meta.get(sid)
        if meta is None:
            return None
        params = dict(meta["params"])
        if sid in self._secrets:
            params["secrets"] = self._secrets[sid]
        return ReplicaRecord(
            sid=meta["sid"],
            digest=meta["digest"],
            source_node=meta["source_node"],
            snapshot_dir=meta["snapshot_dir"],
            params=params,
            needs_secrets=bool(meta.get("needs_secrets")),
        )

    def holds(self, sid: str) -> bool:
        """Return whether this store has metadata for ``sid``."""

        return sid in self._meta

    def secrets_ready(self, sid: str) -> bool:
        """Whether ``sid`` can be safely restored without losing secret env.

        True when the replica carries no secrets, or its secrets are still in
        memory. A replica that needed secrets but lost them to a restart returns
        False, so the caller refuses a degraded restore instead of silently
        dropping ``Sandbox._secret_env``.
        """
        meta = self._meta.get(sid)
        if meta is None:
            return False
        if not meta.get("needs_secrets"):
            return True
        return sid in self._secrets

    def list(self) -> list[str]:
        """Return held replica sandbox ids in stable sorted order."""

        return sorted(self._meta)

    def drop(self, sid: str) -> None:
        """Remove held metadata for ``sid`` while leaving its snapshot directory."""

        self._meta.pop(sid, None)
        self._secrets.pop(sid, None)
        try:
            (self._root / f"{sid}.json").unlink()
        except FileNotFoundError:
            pass
