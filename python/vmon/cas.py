"""Content-addressed indexes for bootable sandbox templates."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import time
from pathlib import Path

_CHUNK_SIZE = 1024 * 1024
_MARKER_NAME = "agent-ready.json"
_VOLATILE_MARKER_FIELDS = frozenset({"content_digest"})


def _validate_digest(digest: str) -> str:
    if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        raise ValueError("template digest must be a lowercase SHA-256 hex digest")
    return digest


def _regular_files(template_dir: Path) -> list[Path]:
    return sorted(
        (
            path
            for path in template_dir.rglob("*")
            if stat.S_ISREG(path.stat(follow_symlinks=False).st_mode)
        ),
        key=lambda path: path.relative_to(template_dir).as_posix(),
    )


def _marker_bytes(path: Path) -> bytes:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return path.read_bytes()
    if not isinstance(data, dict):
        return path.read_bytes()
    stable = {key: value for key, value in data.items() if key not in _VOLATILE_MARKER_FIELDS}
    return json.dumps(stable, sort_keys=True, separators=(",", ":")).encode("utf-8")


def template_digest(template_dir: Path) -> str:
    """Return a stable SHA-256 digest for a template's bootable content."""
    template_dir = Path(template_dir)
    digest = hashlib.sha256()
    for path in _regular_files(template_dir):
        rel_path = path.relative_to(template_dir).as_posix().encode("utf-8")
        digest.update(rel_path)
        digest.update(b"\0")
        if path.name == _MARKER_NAME and path.parent == template_dir:
            digest.update(_marker_bytes(path))
            continue
        with path.open("rb") as fh:
            try:
                hashlib.file_digest(fh, lambda: digest)
            except AttributeError:
                while chunk := fh.read(_CHUNK_SIZE):
                    digest.update(chunk)
    return digest.hexdigest()


def cas_root() -> Path:
    """Return the on-disk root for template digest pointers."""
    from . import image

    root = image._state() / "cas"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _pointer_path(digest: str) -> Path:
    digest = _validate_digest(digest)
    return cas_root() / f"{digest}.json"


def _template_present(template_dir: Path) -> bool:
    return (
        template_dir.is_dir()
        and (template_dir / "rootfs.img").is_file()
        and (template_dir / _MARKER_NAME).is_file()
    )


def _write_pointer(path: Path, payload: dict[str, str | int]) -> None:
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def index_template(template_dir: Path, digest: str | None = None) -> str:
    """Record a digest -> template directory pointer and return the digest."""
    template_dir = Path(template_dir)
    digest = _validate_digest(digest) if digest is not None else template_digest(template_dir)
    pointer = _pointer_path(digest)
    created_unix = int(time.time())
    try:
        existing = json.loads(pointer.read_text(encoding="utf-8"))
    except OSError, json.JSONDecodeError:
        existing = {}
    if isinstance(existing, dict) and isinstance(existing.get("created_unix"), int):
        created_unix = existing["created_unix"]
    payload: dict[str, str | int] = {
        "template_dir": str(template_dir),
        "tpl_name": template_dir.name,
        "created_unix": created_unix,
    }
    if existing != payload:
        _write_pointer(pointer, payload)
    return digest


def resolve_digest(digest: str) -> Path | None:
    """Resolve a live template directory for *digest*, pruning stale pointers."""
    pointer = _pointer_path(digest)
    try:
        data = json.loads(pointer.read_text(encoding="utf-8"))
    except OSError, json.JSONDecodeError:
        pointer.unlink(missing_ok=True)
        return None
    template_dir_s = data.get("template_dir") if isinstance(data, dict) else None
    if not isinstance(template_dir_s, str):
        pointer.unlink(missing_ok=True)
        return None
    template_dir = Path(template_dir_s)
    if not _template_present(template_dir):
        pointer.unlink(missing_ok=True)
        return None
    return template_dir


def drop_digest(digest: str) -> None:
    """Remove a digest's CAS pointer (the template directory is left untouched).

    Used to un-advertise a transient template (e.g. a migration checkpoint) so
    peers can no longer pull it, without deleting any directory a live VM may
    still reference.
    """
    try:
        _pointer_path(digest).unlink(missing_ok=True)
    except ValueError:
        pass


def list_digests() -> dict[str, str]:
    """Return digest -> template directory for all live CAS pointers."""
    live: dict[str, str] = {}
    for pointer in sorted(cas_root().glob("*.json")):
        digest = pointer.stem
        try:
            template_dir = resolve_digest(digest)
        except ValueError:
            continue
        if template_dir is not None:
            live[digest] = str(template_dir)
    return live
