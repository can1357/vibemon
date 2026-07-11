"""Persistent named-volume SDK abstraction for the Rust v1 API."""

from __future__ import annotations

import re
from dataclasses import dataclass

_NAME_RE = re.compile(r"^[a-z0-9_][a-z0-9_.-]{0,63}$")


@dataclass(frozen=True, slots=True)
class S3Mount:
    """An S3 bucket or prefix mounted into a sandbox create request."""

    uri: str
    endpoint: str | None = None
    region: str | None = None
    read_only: bool = False
    access_key: str | None = None
    secret_key: str | None = None
    session_token: str | None = None

    def to_wire(self) -> dict[str, object]:
        """Return the create-body S3 mount shape."""
        wire: dict[str, object] = {"uri": self.uri, "read_only": self.read_only}
        for key, value in (
            ("endpoint", self.endpoint),
            ("region", self.region),
            ("access_key", self.access_key),
            ("secret_key", self.secret_key),
            ("session_token", self.session_token),
        ):
            if value is not None:
                wire[key] = value
        return wire


class Volume:
    """A validated persistent-volume identity used in sandbox create requests."""

    def __init__(self, name: str) -> None:
        if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
            raise ValueError(f"invalid volume name {name!r}: must match {_NAME_RE.pattern}")
        self.name = name

    def to_wire(self, *, read_only: bool = False) -> str | dict[str, object]:
        """Return the v1 create-body volume shape."""
        return {"name": self.name, "read_only": True} if read_only else self.name
