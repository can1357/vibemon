"""Template archive distribution and lazy pull helpers."""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import socket
import tarfile
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from fastapi import Depends, FastAPI, Response, status
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask

from ..core import state_dir
from .proxy import coded_http


def _template_archive(template_dir: Path) -> Path:
    fd, archive_s = tempfile.mkstemp(prefix="vmon-template-", suffix=".tar.gz")
    os.close(fd)
    archive = Path(archive_s)
    try:
        with tarfile.open(archive, "w:gz") as tar:
            tar.add(template_dir, arcname=template_dir.name)
    except Exception:
        archive.unlink(missing_ok=True)
        raise
    return archive


def _template_metadata_archive(template_dir: Path) -> Path:
    def metadata_filter(info: tarfile.TarInfo) -> tarfile.TarInfo | None:
        name = Path(info.name).name
        if name.startswith("memory.") and name.endswith(".bin"):
            return None
        return info

    fd, archive_s = tempfile.mkstemp(prefix="vmon-template-meta-", suffix=".tar.gz")
    os.close(fd)
    archive = Path(archive_s)
    try:
        with tarfile.open(archive, "w:gz") as tar:
            tar.add(template_dir, arcname=template_dir.name, filter=metadata_filter)
    except Exception:
        archive.unlink(missing_ok=True)
        raise
    return archive


def _extracted_template_root(root: Path) -> Path:
    if (root / "agent-ready.json").is_file():
        return root
    for child in root.iterdir():
        if child.is_dir() and (child / "agent-ready.json").is_file():
            return child
    raise ValueError("template archive did not contain an agent-ready marker")


def _template_name_from_marker(template_dir: Path, digest: str) -> str:
    try:
        data = json.loads((template_dir / "agent-ready.json").read_text(encoding="utf-8"))
    except OSError:
        data = {}
    except json.JSONDecodeError:
        data = {}
    raw = data.get("tpl_name") if isinstance(data, dict) else None
    name = raw if isinstance(raw, str) and raw else template_dir.name
    safe = Path(name).name
    return safe if safe and safe not in {".", ".."} else f"tpl-{digest[:12]}"


def _install_pulled_template(archive: Path, digest: str) -> Path:
    from .. import cas

    templates = state_dir() / "templates"
    templates.mkdir(parents=True, exist_ok=True)
    extract_root = Path(tempfile.mkdtemp(prefix=".pull-extract-", dir=templates))
    try:
        with tarfile.open(archive, "r:gz") as tar:
            tar.extractall(extract_root, filter="data")
        source = _extracted_template_root(extract_root)
        actual = cas.template_digest(source)
        if actual != digest:
            raise ValueError("pulled template digest mismatch")
        target = templates / _template_name_from_marker(source, digest)
        if target.exists():
            if target.is_dir() and cas.template_digest(target) == digest:
                cas.index_template(target, digest)
                return target
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        os.replace(source, target)
        cas.index_template(target, digest)
        return target
    finally:
        archive.unlink(missing_ok=True)
        shutil.rmtree(extract_root, ignore_errors=True)


def _marker_digest(template_dir: Path) -> str | None:
    try:
        data = json.loads((template_dir / "agent-ready.json").read_text(encoding="utf-8"))
    except OSError:
        return None
    except json.JSONDecodeError:
        return None
    value = data.get("content_digest") if isinstance(data, dict) else None
    return value if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) else None


def _remote_page_url(peer_url: str, digest: str) -> str:
    parsed = urlsplit(peer_url.rstrip("/"))
    netloc = parsed.netloc
    if parsed.scheme == "http" and parsed.hostname:
        port = parsed.port or 80
        try:
            addr = str(socket.getaddrinfo(parsed.hostname, port, type=socket.SOCK_STREAM)[0][4][0])
        except OSError:
            pass
        else:
            host = f"[{addr}]" if ":" in addr else addr
            netloc = f"{host}:{port}" if parsed.port is not None else host
    return f"{parsed.scheme}://{netloc}/v1/templates/{digest}/pages"


def _install_lazy_template_stub(archive: Path, digest: str, peer_url: str) -> Path:
    templates = state_dir() / "templates"
    templates.mkdir(parents=True, exist_ok=True)
    extract_root = Path(tempfile.mkdtemp(prefix=".pull-meta-", dir=templates))
    try:
        with tarfile.open(archive, "r:gz") as tar:
            tar.extractall(extract_root, filter="data")
        source = _extracted_template_root(extract_root)
        if _marker_digest(source) != digest:
            raise ValueError("pulled template metadata digest mismatch")
        generation = (source / "current-generation").read_text(encoding="utf-8").strip()
        if not generation.isdecimal() or not (source / f"vmstate.{generation}.bin").is_file():
            raise ValueError("pulled template metadata is missing snapshot state")
        if not (source / "rootfs.img").is_file():
            raise ValueError("pulled template metadata is missing rootfs.img")
        target = templates / f"remote-{_template_name_from_marker(source, digest)}"
        if target.exists():
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        metadata = {
            "peer_url": peer_url.rstrip("/"),
            "digest": digest,
            "page_url": _remote_page_url(peer_url, digest),
        }
        (source / "remote-page.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        os.replace(source, target)
        return target
    finally:
        archive.unlink(missing_ok=True)
        shutil.rmtree(extract_root, ignore_errors=True)


async def pull_template_metadata(client: Any, peer_url: str, digest: str, token: str) -> Path:
    """Fetch a metadata-only remote-template stub for lazy page-in restore."""
    archive_dir = state_dir()
    archive_dir.mkdir(parents=True, exist_ok=True)
    fd, archive_s = tempfile.mkstemp(
        prefix=".template-meta-pull-", suffix=".tar.gz", dir=archive_dir
    )
    os.close(fd)
    archive = Path(archive_s)
    headers = {"Authorization": f"Bearer {token}", "X-Vmon-Mesh-Hop": "1"}
    try:
        url = f"{peer_url.rstrip('/')}/v1/templates/{digest}/metadata"
        async with client.stream("GET", url, headers=headers) as response:
            if response.status_code == status.HTTP_404_NOT_FOUND:
                raise FileNotFoundError(f"unknown template digest {digest}")
            response.raise_for_status()
            with archive.open("wb") as fh:
                async for chunk in response.aiter_bytes():
                    if chunk:
                        fh.write(chunk)
        return await asyncio.to_thread(_install_lazy_template_stub, archive, digest, peer_url)
    except Exception:
        archive.unlink(missing_ok=True)
        raise


async def pull_template(client: Any, peer_url: str, digest: str, token: str) -> Path:
    """Fetch, verify, install, and index a peer's content-addressed template.

    ``client`` is an httpx-like async client (``app.state.mesh_http`` in the
    server; a fake stream client in tests) exposing
    ``.stream(method, url, headers=...)`` as an async context manager. Integrity
    is enforced in ``_install_pulled_template``: a payload whose content does not
    hash to ``digest`` is rejected and nothing is installed.
    """
    archive_dir = state_dir()
    archive_dir.mkdir(parents=True, exist_ok=True)
    fd, archive_s = tempfile.mkstemp(prefix=".template-pull-", suffix=".tar.gz", dir=archive_dir)
    os.close(fd)
    archive = Path(archive_s)
    headers = {"Authorization": f"Bearer {token}", "X-Vmon-Mesh-Hop": "1"}
    try:
        url = f"{peer_url.rstrip('/')}/v1/templates/{digest}"
        async with client.stream("GET", url, headers=headers) as response:
            if response.status_code == status.HTTP_404_NOT_FOUND:
                raise FileNotFoundError(f"unknown template digest {digest}")
            response.raise_for_status()
            with archive.open("wb") as fh:
                async for chunk in response.aiter_bytes():
                    if chunk:
                        fh.write(chunk)
        return await asyncio.to_thread(_install_pulled_template, archive, digest)
    except Exception:
        archive.unlink(missing_ok=True)
        raise


def _template_memory_page(template_dir: Path, page: int) -> bytes:
    if page < 0:
        raise FileNotFoundError("negative template page")
    generation = (template_dir / "current-generation").read_text(encoding="utf-8").strip()
    if not generation.isdecimal():
        raise FileNotFoundError("template has invalid snapshot generation")
    memory = template_dir / f"memory.{generation}.bin"
    offset = page * 4096
    with memory.open("rb") as fh:
        fh.seek(offset)
        data = fh.read(4096)
    if len(data) != 4096:
        raise FileNotFoundError(f"template page {page} is outside the memory image")
    return data


def register_template_routes(app: FastAPI, require_auth: Any) -> None:
    @app.get("/v1/templates/{digest}/metadata", dependencies=[Depends(require_auth)])
    async def template_metadata_blob(digest: str) -> FileResponse:
        from .. import cas

        try:
            template_dir = cas.resolve_digest(digest)
        except ValueError:
            template_dir = None
        if template_dir is None:
            raise coded_http(status.HTTP_404_NOT_FOUND, "not_found", "unknown template")
        archive = await asyncio.to_thread(_template_metadata_archive, template_dir)
        return FileResponse(
            str(archive),
            media_type="application/gzip",
            filename=f"{digest}.metadata.tar.gz",
            background=BackgroundTask(archive.unlink),
        )

    @app.get("/v1/templates/{digest}/pages/{page}", dependencies=[Depends(require_auth)])
    async def template_memory_page(digest: str, page: int) -> Response:
        from .. import cas

        try:
            template_dir = cas.resolve_digest(digest)
        except ValueError:
            template_dir = None
        if template_dir is None:
            raise coded_http(status.HTTP_404_NOT_FOUND, "not_found", "unknown template")
        try:
            data = await asyncio.to_thread(_template_memory_page, template_dir, page)
        except (FileNotFoundError, OSError) as exc:
            raise coded_http(status.HTTP_404_NOT_FOUND, "not_found", str(exc)) from exc
        return Response(data, media_type="application/octet-stream")

    @app.get("/v1/templates/{digest}", dependencies=[Depends(require_auth)])
    async def template_blob(digest: str) -> FileResponse:
        from .. import cas

        try:
            template_dir = cas.resolve_digest(digest)
        except ValueError:
            template_dir = None
        if template_dir is None:
            raise coded_http(status.HTTP_404_NOT_FOUND, "not_found", "unknown template")
        archive = await asyncio.to_thread(_template_archive, template_dir)
        return FileResponse(
            str(archive),
            media_type="application/gzip",
            filename=f"{digest}.tar.gz",
            background=BackgroundTask(archive.unlink),
        )

