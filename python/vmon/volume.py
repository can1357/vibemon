"""Persistent named-volume host-side abstraction for sandbox microVMs.

A :class:`Volume` is a host directory shared into a guest (via virtio-fs) that
persists across sandbox lifetimes. Volumes are excluded from memory snapshots
and re-attached by name. A single-writer ``flock`` guards each volume so that
only one live VM may mutate it at a time.
"""

from __future__ import annotations

import errno
import fcntl
import os
import re
import threading
from pathlib import Path

STATE = Path(os.environ.get("VMON_HOME", str(Path.home() / ".vmon")))
VOLUME_DIR = STATE / "volumes"

_NAME_RE = re.compile(r"^[a-z0-9_][a-z0-9_.-]{0,63}$")


class Volume:
    """A persistent, single-writer named host directory shared into a guest."""

    def __init__(self, name: str) -> None:
        if not _NAME_RE.match(name):
            raise ValueError(
                f"invalid volume name {name!r}: must match {_NAME_RE.pattern}"
            )
        self.name = name
        self.host_dir = VOLUME_DIR / name
        self._lock_fd: int | None = None
        self._guard = threading.Lock()

    def ensure(self) -> Path:
        """Create the host directory (idempotent) and return it."""
        self.host_dir.mkdir(parents=True, exist_ok=True)
        return self.host_dir

    def acquire(self) -> None:
        """Take the exclusive single-writer lock; raise if held by another VM."""
        with self._guard:
            if self._lock_fd is not None:
                return
            self.ensure()
            fd = os.open(str(self.host_dir / ".lock"), os.O_RDWR | os.O_CREAT, 0o644)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                os.close(fd)
                if exc.errno in (errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK):
                    raise RuntimeError(
                        f"volume {self.name!r} is already in use by another VM"
                    ) from exc
                raise
            self._lock_fd = fd

    def release(self) -> None:
        """Release the single-writer lock if held (idempotent)."""
        with self._guard:
            fd = self._lock_fd
            if fd is None:
                return
            self._lock_fd = None
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def __enter__(self) -> Volume:
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()

    @property
    def tag(self) -> str:
        """virtio-fs tag derived from the name (Rust charset ``[a-z0-9_]{1,32}``)."""
        return re.sub(r"[^a-z0-9_]", "_", self.name.lower())[:32] or "vol"

    @classmethod
    def list(cls) -> list[str]:
        """Names of existing volumes under :data:`VOLUME_DIR` (``[]`` if none)."""
        if not VOLUME_DIR.is_dir():
            return []
        return sorted(p.name for p in VOLUME_DIR.iterdir() if p.is_dir())
