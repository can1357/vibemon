"""Static web UI mounting."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Response, status
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


def _web_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "web"


def mount_web_ui(app: FastAPI) -> None:
    """Serve the built React UI from ``vmon/web`` with an SPA fallback.

    No-op (with a stub index) when the build dir is absent, so ``vmon serve``
    still works without the UI having been built. API routes (/v1, /healthz,
    /metrics) are registered first and take precedence; the catch-all only
    handles GET requests for client-side routes.
    """
    web = _web_dir()
    index = web / "index.html"
    if not index.is_file():
        return
    # Static assets (hashed bundles) at /assets/*.
    assets = web / "assets"
    if assets.is_dir():
        app.mount("/assets", StaticFiles(directory=str(assets)), name="web-assets")
    # SPA fallback: any non-API GET returns index.html; the client router
    # decides what to render. Hard 404 for missing files outside /assets.
    api_prefixes = ("/v1", "/healthz", "/metrics")

    @app.get("/{path:path}")
    async def _spa(path: str) -> Response:
        if ("/" + path).startswith(api_prefixes):
            raise HTTPException(status.HTTP_404_NOT_FOUND)
        candidate = (web / path).resolve()
        try:
            candidate.relative_to(web)
        except ValueError:
            raise HTTPException(status.HTTP_404_NOT_FOUND) from None
        if candidate.is_file():
            return FileResponse(str(candidate))
        return FileResponse(str(index))
