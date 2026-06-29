"""Client-side storage for named cluster contexts."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .core import state_dir

__all__ = [
    "LOCAL",
    "Context",
    "contexts_path",
    "ContextStore",
    "roster_from_status",
]

#: Reserved built-in context name for using the local vmond daemon.
LOCAL = "local"


@dataclass
class Context:
    """A named gateway roster; the bearer token is in-memory only, never persisted."""

    name: str
    endpoints: list[str]
    token: str | None = None
    region: str = ""
    updated: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Return this context as JSON-ready data (token excluded; never written to disk)."""
        return {
            "name": self.name,
            "endpoints": list(self.endpoints),
            "region": self.region,
            "updated": self.updated,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Context:
        """Build a context from tolerant JSON data, defaulting missing fields."""
        endpoints_value = data.get("endpoints", [])
        if isinstance(endpoints_value, list):
            endpoints = [value for value in endpoints_value if isinstance(value, str) and value]
        else:
            endpoints = []

        name_value = data.get("name", "")
        token_value = data.get("token")
        region_value = data.get("region", "")
        updated_value = data.get("updated", 0.0)

        if not isinstance(updated_value, int | float):
            updated_value = 0.0

        return cls(
            name=name_value if isinstance(name_value, str) else "",
            endpoints=endpoints,
            token=token_value if isinstance(token_value, str) else None,
            region=region_value if isinstance(region_value, str) else "",
            updated=float(updated_value),
        )


def contexts_path(home: Path | None = None) -> Path:
    """Return the JSON store path under the given or default vmon state directory."""
    return (home or state_dir()) / "contexts.json"


class ContextStore:
    """Load, persist, and select CLI cluster contexts."""

    def __init__(self, path: Path | None = None) -> None:
        """Create an empty store backed by the given contexts file path."""
        self._path = path or contexts_path()
        self._current: str | None = None
        self._contexts: dict[str, Context] = {}

    def load(self) -> None:
        """Load contexts from disk, replacing memory and tolerating absent/bad JSON."""
        self._current = None
        self._contexts = {}

        try:
            with self._path.open(encoding="utf-8") as file:
                data = json.load(file)
        except OSError, json.JSONDecodeError:
            return

        if not isinstance(data, Mapping):
            return

        current = data.get("current")
        self._current = current if isinstance(current, str) and current else None

        contexts = data.get("contexts", {})
        if not isinstance(contexts, Mapping):
            return

        loaded: dict[str, Context] = {}
        for key, value in contexts.items():
            if not isinstance(key, str) or not isinstance(value, Mapping):
                continue
            context = Context.from_dict(value)
            if not context.name:
                context.name = key
            loaded[context.name] = context
        self._contexts = loaded

    def save(self) -> None:
        """Atomically persist current selection and contexts to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_name(f"{self._path.name}.tmp")
        data = {
            "current": self._current,
            "contexts": {name: ctx.to_dict() for name, ctx in self._contexts.items()},
        }
        with tmp.open("w", encoding="utf-8") as file:
            json.dump(data, file, indent=2)
        os.replace(tmp, self._path)

    def list(self) -> list[Context]:
        """Return all persisted contexts sorted by name."""
        return [self._contexts[name] for name in sorted(self._contexts)]

    def get(self, name: str) -> Context | None:
        """Return a context by name, or None when it does not exist."""
        return self._contexts.get(name)

    def current_name(self) -> str | None:
        """Return the active context name, honoring the VMON_CONTEXT override."""
        override = os.environ.get("VMON_CONTEXT")
        if override:
            return override
        return self._current or None

    def current(self) -> Context | None:
        """Return the active remote context, excluding the built-in local context."""
        name = self.current_name()
        if not name or name == LOCAL:
            return None
        return self.get(name)

    def use(self, name: str) -> None:
        """Select an existing context, or the reserved local context, and persist it."""
        if name != LOCAL and name not in self._contexts:
            raise KeyError(name)
        self._current = name
        self.save()

    def put(self, ctx: Context) -> None:
        """Insert or replace a context by name and persist the store."""
        self._contexts[ctx.name] = ctx
        self.save()

    def remove(self, name: str) -> None:
        """Remove a context by name, clearing it as current when selected."""
        self._contexts.pop(name, None)
        if self._current == name:
            self._current = None
        self.save()


def roster_from_status(status: Mapping[str, Any], *, fallback: str | None = None) -> list[str]:
    """Extract an ordered de-duplicated gateway roster from mesh status data."""
    roster: list[str] = []
    seen: set[str] = set()

    self_status = status.get("self")
    if isinstance(self_status, Mapping):
        advertise = self_status.get("advertise")
        if isinstance(advertise, str) and advertise and advertise not in seen:
            seen.add(advertise)
            roster.append(advertise)

    peers = status.get("peers")
    if isinstance(peers, list):
        for peer in peers:
            if not isinstance(peer, Mapping):
                continue
            advertise = peer.get("advertise")
            if isinstance(advertise, str) and advertise and advertise not in seen:
                seen.add(advertise)
                roster.append(advertise)

    if not roster and fallback:
        return [fallback]
    return roster
