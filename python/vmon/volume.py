"""Persistent named-volume SDK abstraction for the Rust v1 API."""

from __future__ import annotations

import os
import re
from pathlib import Path
from types import TracebackType

from ._transport import get_transport, path_segment, state_dir

STATE = Path(os.environ.get("VMON_HOME", str(Path.home() / ".vmon")))
VOLUME_DIR = STATE / "volumes"

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
        # Compatibility for callers that used the old local path as a display
        # value. The path is only meaningful for local-daemon contexts.
        self.host_dir = state_dir() / "volumes" / name

    def ensure(self) -> Path:
        """Create the server-side volume if needed and return the legacy path."""
        get_transport().json("PUT", f"/v1/volumes/{path_segment(self.name)}")
        return self.host_dir

    create = ensure

    def delete(self) -> None:
        """Delete the server-side volume."""
        get_transport().json("DELETE", f"/v1/volumes/{path_segment(self.name)}")

    rm = delete

    def acquire(self) -> None:
        """Compatibility no-op: the daemon now enforces writer locks."""
        self.ensure()

    def release(self) -> None:
        """Compatibility no-op: locks are held by daemon-managed sandboxes."""

    def __enter__(self) -> Volume:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()

    @property
    def tag(self) -> str:
        """virtio-fs tag derived from the name (Rust charset ``[a-z0-9_]{1,32}``)."""
        return re.sub(r"[^a-z0-9_]", "_", self.name.lower())[:32] or "vol"

    def to_wire(self, *, read_only: bool = False) -> str | dict[str, object]:
        """Return the v1 create-body volume shape."""
        return {"name": self.name, "read_only": True} if read_only else self.name

    @classmethod
    def list(cls) -> list[str]:
        """Names of volumes known to the selected vmon API context."""
        value = get_transport().json("GET", "/v1/volumes")
        volumes = value.get("volumes") if isinstance(value, dict) else None
        return sorted(str(name) for name in volumes) if isinstance(volumes, list) else []
