"""On-demand provisioning of guest boot assets into the Vibemon home directory.

Booting a microVM needs a Linux guest kernel. Linux hosts usually have a distro
kernel under ``/boot``, but macOS/HVF hosts have none, so :func:`ensure_kernel`
downloads a known-good, pinned kernel into ``$VMON_HOME/assets`` on first use
(cached and checksum-verified) instead of requiring a manual ``$VMON_KERNEL`` or
a separate ``just fetch-assets`` step.

Only immutable *guest* assets are provisioned here; host tools (``mkfs.ext4``)
and the host-side VMM binary are located in place, never downloaded.
"""

from __future__ import annotations

import hashlib
import os
import platform
import sys
import urllib.request
from pathlib import Path

# Pinned, checksum-verified guest kernels per host arch, shared with the
# integration suite (`demo/fetch-test-assets.sh`): the firecracker-ci aarch64
# kernel boots cleanly under both KVM and HVF, and the x86_64 quickstart kernel
# is the firecracker default. Each entry is (filename, url, sha256).
_KERNELS: dict[str, tuple[str, str, str]] = {
    "aarch64": (
        "Image-aarch64",
        "https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.12/aarch64/vmlinux-6.1.128",
        "cb1291c66bca75bc11cb9c8357fcef9965bb1786dffcb42a60923c3e0e49f319",
    ),
    "x86_64": (
        "vmlinux-x86_64",
        "https://s3.amazonaws.com/spec.ccfc.min/img/quickstart_guide/x86_64/kernels/vmlinux.bin",
        "ea5e7d5cf494a8c4ba043259812fc018b44880d70bcbbfc4d57d2760631b1cd6",
    ),
}


def _home() -> Path:
    """The Vibemon home directory (``$VMON_HOME`` or ``~/.vmon``)."""
    return Path(os.environ.get("VMON_HOME") or str(Path.home() / ".vmon")).expanduser()


def assets_dir() -> Path:
    """Directory holding auto-provisioned guest assets (``$VMON_HOME/assets``)."""
    d = _home() / "assets"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _normalize_arch(arch: str) -> str:
    normalized = arch.lower().replace("-", "_")
    return {
        "arm64": "aarch64",
        "amd64": "x86_64",
        "x64": "x86_64",
    }.get(normalized, normalized)


def _host_arch() -> str:
    return _normalize_arch(platform.machine())


def ensure_kernel(arch: str | None = None) -> Path:
    """Return a guest kernel for *arch*, downloading it on first use.

    The kernel is cached at ``$VMON_HOME/assets/<name>`` and verified against a
    pinned SHA-256; a cached file with the right digest is reused without any
    network round-trip.

    # Errors
    Raises ``RuntimeError`` if *arch* has no pinned kernel, the download fails,
    or the downloaded bytes do not match the pinned checksum.
    """
    arch = _normalize_arch(arch) if arch else _host_arch()
    try:
        name, url, sha256 = _KERNELS[arch]
    except KeyError:
        raise RuntimeError(
            f"no pinned guest kernel for arch {arch!r}; set VMON_KERNEL=/path/to/Image-or-bzImage"
        ) from None
    dest = assets_dir() / name
    if dest.is_file() and _sha256(dest) == sha256:
        return dest
    print(f"vmon: downloading guest kernel {name} (one-time)...", file=sys.stderr, flush=True)
    _download_verified(url, dest, sha256)
    return dest


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _download_verified(url: str, dest: Path, sha256: str) -> None:
    tmp = dest.with_name(f"{dest.name}.part.{os.getpid()}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "vmon"})
        with urllib.request.urlopen(req) as resp, open(tmp, "wb") as fh:
            while chunk := resp.read(1 << 20):
                fh.write(chunk)
        got = _sha256(tmp)
        if got != sha256:
            raise RuntimeError(f"checksum mismatch for {url}: expected {sha256}, got {got}")
        os.replace(tmp, dest)
    except OSError as e:
        raise RuntimeError(f"failed to download guest kernel from {url}: {e}") from e
    finally:
        Path(tmp).unlink(missing_ok=True)
