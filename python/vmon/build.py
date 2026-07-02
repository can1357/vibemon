"""Daemonless Dockerfile builders for local vmon image flows."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import tarfile
import tempfile
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path

from .client import DaemonError

_BUILD_TIMEOUT_SECS = 30 * 60


def _state() -> Path:
    return Path(os.environ.get("VMON_HOME") or str(Path.home() / ".vmon")).expanduser()


def _builds_dir() -> Path:
    return _state() / "builds"


def _normalize_arch(arch: str | None) -> str | None:
    if arch is None:
        return None
    machine = arch.strip().lower().replace("-", "_")
    if machine in {"x86_64", "amd64", "x64"}:
        return "amd64"
    if machine in {"aarch64", "arm64"}:
        return "arm64"
    return machine


def _platform_arg(arch: str | None) -> str | None:
    normalized = _normalize_arch(arch)
    return f"linux/{normalized}" if normalized else None


def _unsupported_message() -> str:
    message = (
        "Dockerfile builds require buildah (rootless) or docker buildx with an OCI builder; "
        "install buildah or Docker with buildx enabled"
    )
    if platform.system() == "Darwin":
        message += "; on macOS builds need a Linux host or a docker CLI with a running builder"
    return message


def _detect_backend() -> tuple[str, str]:
    buildah = shutil.which("buildah")
    # buildah is Linux-native; on macOS prefer a docker CLI backed by a running builder.
    if buildah and platform.system() != "Darwin":
        return "buildah", buildah
    docker = shutil.which("docker")
    if docker:
        return "docker", docker
    raise DaemonError(_unsupported_message(), code="unsupported")


def _run(cmd: Sequence[str]) -> None:
    try:
        subprocess.run(list(cmd), check=True, timeout=_BUILD_TIMEOUT_SECS)
    except FileNotFoundError as exc:
        raise DaemonError(_unsupported_message(), code="unsupported") from exc
    except subprocess.TimeoutExpired as exc:
        raise DaemonError(f"{cmd[0]} Dockerfile build timed out", code="timeout") from exc
    except subprocess.CalledProcessError as exc:
        hint = ""
        if platform.system() == "Darwin":
            hint = "; Dockerfile builds need a Linux host or a docker CLI with a running builder"
        raise DaemonError(
            f"{cmd[0]} Dockerfile build failed with exit code {exc.returncode}{hint}",
            code="failed",
        ) from exc


def _build_with_buildah(
    buildah: str, dockerfile: Path, context: Path, tag: str, layout: Path, arch: str | None
) -> None:
    build_cmd = [buildah, "build", "--format", "oci"]
    if platform_value := _platform_arg(arch):
        build_cmd.extend(["--platform", platform_value])
    build_cmd.extend(["-f", os.fspath(dockerfile), "-t", tag, os.fspath(context)])
    _run(build_cmd)
    _run([buildah, "push", tag, f"oci:{layout}:{tag}"])
    _tag_oci_layout(layout, tag)


def _build_with_docker(
    docker: str, dockerfile: Path, context: Path, tag: str, layout: Path, arch: str | None
) -> None:
    archive = layout.with_suffix(".tar")
    build_cmd = [docker, "buildx", "build"]
    if platform_value := _platform_arg(arch):
        build_cmd.extend(["--platform", platform_value])
    build_cmd.extend(
        [
            "-f",
            os.fspath(dockerfile),
            "-t",
            tag,
            "-o",
            f"type=oci,dest={archive}",
            os.fspath(context),
        ]
    )
    _run(build_cmd)
    layout.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive) as tar:
        tar.extractall(layout, filter="data")
    _tag_oci_layout(layout, tag)


def _tag_oci_layout(layout: Path, tag: str) -> None:
    index_path = layout / "index.json"
    data = json.loads(index_path.read_text(encoding="utf-8"))
    manifests = data.get("manifests")
    if not isinstance(manifests, list) or not manifests:
        return
    manifest = manifests[0]
    if not isinstance(manifest, dict):
        return
    annotations = manifest.setdefault("annotations", {})
    if not isinstance(annotations, dict):
        annotations = {}
        manifest["annotations"] = annotations
    annotations["org.opencontainers.image.ref.name"] = tag
    annotations["io.containerd.image.name"] = tag
    index_path.write_text(json.dumps(data, separators=(",", ":")) + "\n", encoding="utf-8")


def _digest_layout(layout: Path) -> str:
    hasher = hashlib.sha256()
    for root, dirs, files in os.walk(layout):
        dirs.sort()
        files.sort()
        root_path = Path(root)
        for directory in dirs:
            rel = (root_path / directory).relative_to(layout).as_posix()
            hasher.update(f"dir\0{rel}\0".encode())
        for filename in files:
            file_path = root_path / filename
            rel = file_path.relative_to(layout).as_posix()
            hasher.update(f"file\0{rel}\0".encode())
            with file_path.open("rb") as fh:
                for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                    hasher.update(chunk)
            hasher.update(b"\0")
    return hasher.hexdigest()


def _oci_ref(layout: Path, tag: str) -> str:
    return f"oci:{layout.resolve()}:{tag}"


def build_image(dockerfile: Path, context: Path, tag: str, *, arch: str | None = None) -> str:
    """Build *dockerfile* into a content-addressed OCI layout and return its skopeo ref."""
    if not tag.strip():
        raise ValueError("image tag must not be empty")

    backend, binary = _detect_backend()
    builds = _builds_dir()
    builds.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".tmp-", dir=builds) as tmp_s:
        tmp = Path(tmp_s)
        layout = tmp / "layout"
        if backend == "buildah":
            _build_with_buildah(binary, Path(dockerfile), Path(context), tag, layout, arch)
        else:
            _build_with_docker(binary, Path(dockerfile), Path(context), tag, layout, arch)
        digest = _digest_layout(layout)
        final = builds / digest
        if final.exists():
            if not final.is_dir():
                final.unlink()
                shutil.move(os.fspath(layout), os.fspath(final))
        else:
            shutil.move(os.fspath(layout), os.fspath(final))
        with suppress(OSError):
            os.utime(final, None)
        return _oci_ref(final, tag)


def _layout_from_ref(ref: str) -> Path | None:
    if not ref.startswith("oci:"):
        return None
    builds = _builds_dir().resolve()
    body = ref.removeprefix("oci:")
    prefix = f"{builds}{os.sep}"
    if not body.startswith(prefix):
        return None
    digest = body[len(prefix) :].split(":", 1)[0]
    if not digest or any(ch not in "0123456789abcdef" for ch in digest):
        return None
    return builds / digest


def prune_build_layouts(keep_ref: str | None = None, *, keep_recent: int = 3) -> None:
    """Best-effort pruning for old local build layouts after a template consumes one."""
    with suppress(Exception):
        builds = _builds_dir()
        keep_layout = _layout_from_ref(keep_ref) if keep_ref else None
        keep = keep_layout.resolve() if keep_layout is not None else None
        entries = [
            path for path in builds.iterdir() if path.is_dir() and not path.name.startswith(".")
        ]
        entries.sort(key=lambda path: path.stat().st_mtime, reverse=True)
        protected = {path.resolve() for path in entries[: max(0, keep_recent)]}
        if keep is not None:
            protected.add(keep)
        for path in entries:
            if path.resolve() not in protected:
                shutil.rmtree(path, ignore_errors=True)
