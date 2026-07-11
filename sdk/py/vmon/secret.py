"""Secrets passed to a Sandbox at runtime, never written to disk.

A :class:`Secret` carries environment variables injected into in-guest exec
sessions. Values live only in process memory and are handed to the guest agent
per ``exec`` call; sandbox metadata records secret *names* only (see
``client.sandboxes.create``), so a sandbox directory never persists secret values.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field


def _coerce_env_name(name: object) -> str:
    text = str(name)
    if not text or "=" in text or "\x00" in text:
        raise ValueError("secret environment names must be non-empty and contain no '=' or NUL")
    return text


def _coerce_env_value(value: object) -> str:
    text = str(value)
    if "\x00" in text:
        raise ValueError("secret environment values must contain no NUL bytes")
    return text


@dataclass(repr=False)
class Secret:
    """A bundle of environment variables injected into sandbox exec sessions."""

    env: dict[str, str] = field(default_factory=dict)
    name: str = "secret"

    def __post_init__(self) -> None:
        self.name = _coerce_env_name(self.name)
        self.env = {_coerce_env_name(k): _coerce_env_value(v) for k, v in self.env.items()}

    def __repr__(self) -> str:
        names = ", ".join(self.names())
        return f"Secret(name={self.name!r}, names=[{names}])"

    @classmethod
    def from_dict(cls, values: Mapping[str, object], *, name: str = "secret") -> Secret:
        """Build a Secret from an explicit name->value mapping."""
        return cls(
            {_coerce_env_name(k): _coerce_env_value(v) for k, v in values.items()}, name=name
        )

    @classmethod
    def from_env(cls, *names: str, name: str = "env") -> Secret:
        """Capture the named variables from the host process environment.

        Missing names are skipped; the host value is never echoed elsewhere.
        """
        wanted = [_coerce_env_name(item) for item in names]
        return cls({item: os.environ[item] for item in wanted if item in os.environ}, name=name)

    def names(self) -> list[str]:
        """Sorted variable names (safe to persist; values are not)."""
        return sorted(self.as_env())

    def as_env(self) -> dict[str, str]:
        """A copy of the variable map for injection into an exec env."""
        return {_coerce_env_name(k): _coerce_env_value(v) for k, v in self.env.items()}

    def to_wire(self) -> dict[str, object]:
        """Return the v1 request shape; values are only sent in-memory."""
        return {"name": self.name, "values": self.as_env()}


def merge_secrets(secrets: Iterable[object] | None) -> dict[str, str]:
    """Flatten an iterable of :class:`Secret` / mappings into one env dict.

    Later entries win on key collision. Accepts ``Secret`` instances or plain
    mappings so callers can pass either; returns a fresh dict of str->str.
    """
    out: dict[str, str] = {}
    for item in secrets or ():
        if isinstance(item, Secret):
            out.update(item.as_env())
        elif isinstance(item, Mapping):
            out.update(Secret.from_dict(item).env)
        else:
            raise TypeError(f"expected Secret or mapping, got {type(item).__name__}")
    return out
