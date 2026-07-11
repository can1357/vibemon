"""Persistent named-volume SDK abstraction for the Rust v1 API."""

from __future__ import annotations

import re

from ._transport import DaemonError, get_transport, path_segment

_NAME_RE = re.compile(r"^[a-z0-9_][a-z0-9_.-]{0,63}$")


class Volume:
    """A persistent named volume mounted into sandboxes by name.

    The Rust daemon owns creation, deletion, locking, and mesh lease policy.
    The Python object is intentionally small: it validates names, carries the
    mount identity used by :meth:`vmon.Sandbox.create`, and exposes the v1
    volume resource helpers.
    """

    def __init__(self, name: str) -> None:
        if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
            raise ValueError(f"invalid volume name {name!r}: must match {_NAME_RE.pattern}")
        self.name = name

    def create(self) -> Volume:
        """Create the server-side volume if needed and return this handle."""
        with get_transport() as transport:
            transport.json("PUT", f"/v1/volumes/{path_segment(self.name)}")
        return self

    def delete(self) -> None:
        """Delete the server-side volume."""
        with get_transport() as transport:
            transport.json("DELETE", f"/v1/volumes/{path_segment(self.name)}")

    def to_wire(self, *, read_only: bool = False) -> str | dict[str, object]:
        """Return the v1 create-body volume shape."""
        return {"name": self.name, "read_only": True} if read_only else self.name

    @classmethod
    def list(cls) -> list[str]:
        """Return names of volumes known to the selected vmon API context."""
        with get_transport() as transport:
            value = transport.json("GET", "/v1/volumes")
        volumes = value.get("volumes") if isinstance(value, dict) else None
        if not isinstance(volumes, list) or not all(isinstance(name, str) for name in volumes):
            raise DaemonError("volume list returned a malformed response", code="protocol")
        return sorted(volumes)
