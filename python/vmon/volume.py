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
from types import TracebackType

STATE = Path(os.environ.get("VMON_HOME", str(Path.home() / ".vmon")))
VOLUME_DIR = STATE / "volumes"

_NAME_RE = re.compile(r"^[a-z0-9_][a-z0-9_.-]{0,63}$")

_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)


def _ensure_private_dir(path: Path, *, parents: bool) -> None:
    """Create/open ``path`` as a real private directory, rejecting symlinks."""
    try:
        path.mkdir(mode=0o700, parents=parents, exist_ok=True)
    except FileExistsError as exc:
        raise ValueError(f"unsafe volume path {path!s}: not a directory") from exc
    try:
        fd = os.open(path, os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW)
    except OSError as exc:
        raise ValueError(f"unsafe volume path {path!s}: not a real directory") from exc
    try:
        os.fchmod(fd, 0o700)
    finally:
        os.close(fd)


def _open_volume_dir(path: Path) -> int:
    """Open an existing volume directory without following a final symlink."""
    try:
        return os.open(path, os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW)
    except OSError as exc:
        raise ValueError(f"unsafe volume path {path!s}: not a real directory") from exc


class Volume:
    """A persistent, single-writer named host directory shared into a guest."""

    def __init__(self, name: str) -> None:
        if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
            raise ValueError(f"invalid volume name {name!r}: must match {_NAME_RE.pattern}")
        self.name = name
        self.host_dir = VOLUME_DIR / name
        self._lock_fd: int | None = None
        self._guard = threading.Lock()

    def ensure(self) -> Path:
        """Create the host directory (idempotent) and return it."""
        _ensure_private_dir(VOLUME_DIR, parents=True)
        _ensure_private_dir(self.host_dir, parents=False)
        return self.host_dir

    def acquire(self) -> None:
        """Take the exclusive single-writer lock; raise if held by another VM."""
        with self._guard:
            if self._lock_fd is not None:
                return
            self.ensure()
            dir_fd = _open_volume_dir(self.host_dir)
            fd = None
            try:
                fd = os.open(
                    ".lock",
                    os.O_RDWR | os.O_CREAT | _O_NOFOLLOW,
                    0o600,
                    dir_fd=dir_fd,
                )
                os.fchmod(fd, 0o600)
            except OSError as exc:
                if fd is not None:
                    os.close(fd)
                raise ValueError(
                    f"unsafe volume lock for {self.name!r}: not a regular file"
                ) from exc
            finally:
                os.close(dir_fd)
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

    @classmethod
    def list(cls) -> list[str]:
        """Names of existing volumes under :data:`VOLUME_DIR` (``[]`` if none)."""
        if not VOLUME_DIR.is_dir() or VOLUME_DIR.is_symlink():
            return []
        return sorted(
            p.name
            for p in VOLUME_DIR.iterdir()
            if not p.is_symlink() and p.is_dir() and _NAME_RE.fullmatch(p.name)
        )
