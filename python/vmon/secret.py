"""Secrets passed to a Sandbox at runtime, never written to disk.

A :class:`Secret` carries environment variables injected into in-guest exec
sessions. Values live only in process memory and are handed to the guest agent
per ``exec`` call; ``meta.json`` records secret *names* only (see
``Sandbox.create``), so a sandbox directory never persists secret values.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field


@dataclass
class Secret:
    """A bundle of environment variables injected into the guest at exec time."""

    env: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, values: Mapping[str, object]) -> "Secret":
        """Build a Secret from an explicit name->value mapping."""
        return cls({str(k): str(v) for k, v in values.items()})

    @classmethod
    def from_env(cls, *names: str) -> "Secret":
        """Capture the named variables from the host process environment.

        Missing names are skipped; the host value is never echoed elsewhere.
        """
        return cls({name: os.environ[name] for name in names if name in os.environ})

    def names(self) -> list[str]:
        """Sorted variable names (safe to persist; values are not)."""
        return sorted(self.env)

    def as_env(self) -> dict[str, str]:
        """A copy of the variable map for injection into an exec env."""
        return dict(self.env)


def merge_secrets(secrets: Iterable[object] | None) -> dict[str, str]:
    """Flatten an iterable of :class:`Secret` / mappings into one env dict.

    Later entries win on key collision. Accepts ``Secret`` instances or plain
    mappings so callers can pass either; returns a fresh dict of str->str.
    """
    out: dict[str, str] = {}
    for item in secrets or ():
        if isinstance(item, Secret):
            out.update(item.env)
        elif isinstance(item, Mapping):
            out.update({str(k): str(v) for k, v in item.items()})
        else:
            raise TypeError(f"expected Secret or mapping, got {type(item).__name__}")
    return out
