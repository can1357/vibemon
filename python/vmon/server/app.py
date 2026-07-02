"""FastAPI application factory and uvicorn entry point."""

from __future__ import annotations

import os
import threading
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer

from ..config import ServeConfig, resolve_serve_config
from ..core import Engine
from ..lease import LeaseManager
from ..mesh import HttpxTransport, Mesh, default_advertise
from ..records import RecordStore
from ..replica import ReplicaStore
from . import api, mesh_routes, proxy, templates, web
from .auth import _primary_token, make_require_auth
from .reconciler import make_lifespan
from .runtime import ServerRuntime
from .supervisor import Supervisor


def _apply_home(config: ServeConfig) -> None:
    """Apply the resolved home to legacy modules that cache VMON_HOME at import."""

    home = config.home.expanduser()
    os.environ["VMON_HOME"] = str(home)
    from .. import lease as lease_mod
    from .. import net as net_mod
    from .. import vmm as vmm_mod
    from .. import volume as volume_mod

    vmm_mod.STATE = home
    volume_mod.STATE = home
    volume_mod.VOLUME_DIR = home / "volumes"
    lease_mod.STATE = home
    lease_mod.LEASE_DIR = home / "leases"
    net_mod.STATE = home
    net_mod.LEASE_DIR = home / "network"
    net_mod.LEASE_FILE = net_mod.LEASE_DIR / "leases.json"


def create_app(
    *,
    config: ServeConfig | None = None,
    token: str | None = None,
    idle_timeout: float | None = None,
    host: str | None = None,
    port: int | None = None,
) -> FastAPI:
    if config is None:
        config = resolve_serve_config(
            {
                "token": token,
                "idle_timeout": idle_timeout,
                "host": host,
                "port": port,
            }
        )
    _apply_home(config)
    supervisor = Supervisor(Engine(), idle_poll=max(1.0, min(float(config.idle_timeout), 30.0)))
    expected_token = config.token
    outbound_token = _primary_token(expected_token)
    client_token = config.client_token
    mesh = Mesh(
        supervisor._engine,
        advertise=default_advertise(config.host, config.port),
        token=outbound_token,
        transport=HttpxTransport(outbound_token),
        heartbeat_sec=config.mesh_heartbeat_sec,
        reap_sec=config.mesh_reap_sec,
        idem_ttl_sec=config.mesh_idem_ttl_sec,
        create_timeout_sec=config.mesh_create_timeout_sec,
        placement_weights=config.placement_weights(),
    )
    bearer = HTTPBearer(auto_error=False)
    replica_store = ReplicaStore()
    record_store = RecordStore()
    lease_manager = LeaseManager(config.home / "leases")
    ctx = ServerRuntime(
        supervisor=supervisor,
        mesh=mesh,
        replica_store=replica_store,
        record_store=record_store,
        lease_manager=lease_manager,
        expected_token=expected_token,
        outbound_token=outbound_token,
        client_token=client_token,
        bearer=bearer,
        config=config,
    )

    app = FastAPI(title="vmon sandbox server", version="1", lifespan=make_lifespan(ctx))
    app.state.supervisor = supervisor
    app.state.mesh = mesh
    app.state.record_store = record_store
    app.state.lease_manager = lease_manager
    app.state.config = config
    app.state.checkpoint_times = ctx.checkpoint_times

    @app.exception_handler(RequestValidationError)
    async def _validation_exception_handler(
        _request: Request, _exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse({"detail": "invalid request"}, status_code=status.HTTP_400_BAD_REQUEST)

    require_auth = make_require_auth(ctx)
    proxy.register_proxy_middleware(app, ctx, require_auth)
    idem = mesh_routes.register_mesh_routes(app, ctx, require_auth)
    templates.register_template_routes(app, require_auth)
    api.register_api_routes(app, ctx, require_auth, idem)
    web.mount_web_ui(app)

    return app


def serve(
    *,
    config: ServeConfig | None = None,
    host: str | None = None,
    port: int | None = None,
    token: str | None = None,
    idle_timeout: float | None = None,
    tls_cert: str | None = None,
    tls_key: str | None = None,
) -> None:
    """Run the daemon **and** the HTTP/web gateway over a single shared engine.

    ``vmon serve`` is the same single-owner process as ``vmond``: it takes the
    ``vmond.lock``, serves the local Unix socket (so the ``vmon`` CLI reaches this
    process) on a background thread bound to the app's :class:`~vmon.core.Engine`,
    and runs uvicorn for the REST/web API. Exactly one owner per ``$VMON_HOME``.
    """
    try:
        import uvicorn
    except ModuleNotFoundError as exc:
        if exc.name == "uvicorn":
            raise RuntimeError(
                "vmon serve requires the server extra: pip install 'vmon[server]'"
            ) from exc
        raise

    # Surface vmon.* application logs (reconciler decisions, lease/fence events)
    # alongside uvicorn's access log; without this a gateway is operationally mute.
    import logging

    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
        )

    if config is None:
        config = resolve_serve_config(
            {
                "host": host,
                "port": port,
                "token": token,
                "idle_timeout": idle_timeout,
                "tls_cert": tls_cert,
                "tls_key": tls_key,
            }
        )
    if not config.token:
        raise RuntimeError("vmon serve requires --token or VMON_API_TOKEN")
    if bool(config.tls_cert) != bool(config.tls_key):
        raise RuntimeError("vmon serve TLS requires both tls_cert and tls_key")
    app = create_app(config=config)

    from ..daemon import Daemon, acquire_owner_lock, daemon_paths, release_owner_lock

    paths = daemon_paths()
    paths["home"].mkdir(parents=True, exist_ok=True)
    lock_fd = acquire_owner_lock(paths["lock"])
    if lock_fd is None:
        raise RuntimeError("a local vmond daemon is already running; run `vmon daemon stop` first")
    engine = app.state.supervisor._engine
    daemon = Daemon(engine, sock_path=paths["sock"])
    ready = threading.Event()
    threading.Thread(
        target=daemon.serve_forever, kwargs={"ready": ready}, name="vmond-serve", daemon=True
    ).start()
    if not ready.wait(5) or not paths["sock"].exists():
        daemon.shutdown()
        release_owner_lock(lock_fd)
        raise RuntimeError(f"failed to bind the local vmond socket at {paths['sock']}")
    paths["pid"].write_text(f"{os.getpid()}\n", encoding="utf-8")
    try:
        uvicorn_kwargs: dict[str, Any] = {"host": config.host, "port": config.port}
        if config.tls_cert and config.tls_key:
            uvicorn_kwargs["ssl_certfile"] = config.tls_cert
            uvicorn_kwargs["ssl_keyfile"] = config.tls_key
        uvicorn.run(app, **uvicorn_kwargs)
    finally:
        daemon.shutdown()
        for path in (paths["sock"], paths["pid"]):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        release_owner_lock(lock_fd)
