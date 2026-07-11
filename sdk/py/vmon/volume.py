"""Persistent named-volume SDK abstraction for the Rust v1 API."""

from __future__ import annotations

import re

_NAME_RE = re.compile(r"^[a-z0-9_][a-z0-9_.-]{0,63}$")


class Volume:
    """A validated persistent-volume identity used in sandbox create requests."""

    def __init__(self, name: str) -> None:
        if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
            raise ValueError(f"invalid volume name {name!r}: must match {_NAME_RE.pattern}")
        self.name = name

    def to_wire(self, *, read_only: bool = False) -> str | dict[str, object]:
        """Return the v1 create-body volume shape."""
        return {"name": self.name, "read_only": True} if read_only else self.name
