"""Root client and resource namespaces for the Python SDK."""

from __future__ import annotations

import builtins
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from ._endpoint import GrpcStubs, decode_view, dump_json
from .driver import (
    DEFAULT_TIMEOUT,
    Driver,
    MeshDriver,
    parse_dsn,
)
from .errors import ProtocolError, TransportError
from .sandbox import (
    Sandbox,
    _clone_create_extra,
    _coerce_cmd,
    _drop_none,
    _secret_wire,
    _volume_wire,
)
from .secret import Secret
from .v1 import api_pb2
from .volume import Volume

if TYPE_CHECKING:
    from .models import MeshNode, MeshStatus, PoolStats, ServerInfo
    from .process import EventStream, Process
    from .remote import RemoteFunction


class Client:
    """Root object for one logical vmon mesh connection."""

    def __init__(self, driver: Driver) -> None:
        self.driver: Driver = driver
        self.sandboxes = SandboxAPI(self)
        self.snapshots = SnapshotAPI(self)
        self.volumes = VolumeAPI(self)
        self.pools = PoolAPI(self)
        self.mesh = MeshAPI(self)
        self._closed = False

    def health(self) -> dict[str, Any]:
        """Return the selected daemon's health document."""
        response = self.driver.request("GET", "/healthz")
        try:
            value = response.json()
        except ValueError as exc:
            raise ProtocolError("health check returned a malformed response") from exc
        if not isinstance(value, Mapping) or not isinstance(value.get("ok"), bool):
            raise ProtocolError("health check returned a malformed response")
        return dict(value)

    def info(self) -> ServerInfo:
        """Return typed server version and capability information."""
        value, _ = self._view(
            lambda stubs: stubs.system.Info(api_pb2.InfoRequest()),
            error="server info returned a non-object response",
        )
        from .models import ServerInfo

        return ServerInfo.from_dict(value)

    def metrics(self) -> str:
        """Return the daemon's Prometheus metrics document."""
        return self.driver.request("GET", "/metrics").text

    def events(self) -> EventStream:
        """Open the daemon's engine event stream."""
        responses, endpoint = self.driver.call(
            lambda stubs: stubs.system.Events(api_pb2.EventsRequest()),
            stream=True,
        )
        from .process import EventStream

        return EventStream(responses, endpoint)

    def shell(
        self,
        *cmd: str | Iterable[str],
        ref: str | None = None,
        image: str | None = None,
        cpus: int = 1,
        memory: int = 512,
        disk_mb: int = 1024,
        timeout: float | None = 300,
    ) -> Process:
        """Start a streaming interactive shell in an ephemeral sandbox."""
        argv = _coerce_cmd(cmd) if cmd else ["/bin/sh"]
        body = _drop_none(
            {
                "ref": ref,
                "image": image,
                "cmd": argv,
                "cpus": int(cpus),
                "mem": int(memory),
                "disk_mb": int(disk_mb),
                "timeout": timeout,
                "tty": True,
            }
        )
        from .process import open_process

        def starter(
            make_inputs: Callable[[], Iterable[api_pb2.ExecInput]],
        ) -> tuple[Any, str]:
            return self.driver.call(
                lambda stubs: stubs.sandbox.Shell(make_inputs()),
                stream=True,
            )

        return open_process(
            starter,
            api_pb2.ExecInput(shell_params_json=dump_json(body)),
            timeout=timeout,
            tty=True,
            consume_ready=True,
            thread_name="vmon-shell",
        )

    def function(self, function: Callable[..., Any], **sandbox_kwargs: Any) -> RemoteFunction:
        """Bind a source-available Python function to this client."""
        from .remote import RemoteFunction

        return RemoteFunction(function, client=self, **sandbox_kwargs)

    def close(self) -> None:
        """Close the underlying driver idempotently."""
        if not self._closed:
            self._closed = True
            self.driver.close()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _view(
        self,
        fn: Callable[[GrpcStubs], api_pb2.JsonView],
        *,
        endpoint: str | None = None,
        error: str = "vmon API returned a non-object response",
    ) -> tuple[dict[str, Any], str | None]:
        view, chosen = self.driver.call(fn, endpoint=endpoint)
        return decode_view(view.json, error), chosen


class SandboxAPI:
    """Create, fetch, reference, and list sandboxes for a client."""

    def __init__(self, client: Client) -> None:
        self._client = client

    def create(
        self,
        *,
        image: str | None = None,
        template: str | None = None,
        dockerfile: str | None = None,
        context: str | None = None,
        name: str | None = None,
        cpus: int = 1,
        memory: int = 512,
        disk_mb: int = 1024,
        timeout: float | None = 300,
        timeout_secs: int | None = None,
        workdir: str | None = None,
        env: dict[str, str] | None = None,
        secrets: Iterable[Secret | Mapping[str, object]] | None = None,
        volumes: Mapping[str, Any] | None = None,
        tags: dict[str, str] | None = None,
        fs_dir: str | None = None,
        block_network: bool = False,
        ports: Sequence[int] | None = None,
        egress_allow: Sequence[str] | None = None,
        egress_allow_domains: Sequence[str] | None = None,
        inbound_cidr_allowlist: Sequence[str] | None = None,
        readiness_probe: Any = None,
        pool_size: int = 0,
        ha: str | None = None,
        arch: str | None = None,
        idempotency_key: str | None = None,
        command: Sequence[str] | None = None,
    ) -> Sandbox:
        """Create a sandbox using the stable ``SandboxCreate`` request fields."""
        secret_items = builtins.list(secrets) if secrets is not None else None
        body = _drop_none(
            {
                "image": image,
                "template": template,
                "dockerfile": dockerfile,
                "context": context,
                "name": name,
                "cpus": int(cpus),
                "memory": int(memory),
                "disk_mb": int(disk_mb),
                "timeout": timeout,
                "timeout_secs": timeout_secs,
                "workdir": workdir,
                "env": {str(key): str(value) for key, value in (env or {}).items()}
                if env is not None
                else None,
                "secrets": _secret_wire(secret_items),
                "volumes": _volume_wire(volumes),
                "tags": {str(key): str(value) for key, value in (tags or {}).items()}
                if tags is not None
                else None,
                "fs_dir": fs_dir,
                "block_network": bool(block_network),
                "ports": [int(port) for port in ports] if ports is not None else None,
                "egress_allow": list(egress_allow) if egress_allow is not None else None,
                "egress_allow_domains": list(egress_allow_domains)
                if egress_allow_domains is not None
                else None,
                "inbound_cidr_allowlist": list(inbound_cidr_allowlist)
                if inbound_cidr_allowlist is not None
                else None,
                "readiness_probe": readiness_probe,
                "pool_size": int(pool_size),
                "ha": ha,
                "arch": arch,
                "idempotency_key": idempotency_key,
                "command": [str(part) for part in command] if command is not None else None,
            }
        )
        value, endpoint = self._client._view(
            lambda stubs: stubs.sandbox.Create(
                api_pb2.CreateSandboxRequest(spec_json=dump_json(body))
            ),
            error="create returned a non-object response",
        )
        sandbox = Sandbox(self._client, value, endpoint=endpoint)
        sandbox._bind_defaults(workdir=workdir, env=env, tags=tags, secrets=secret_items)
        return sandbox

    def get(self, sandbox_id: str) -> Sandbox:
        """Fetch a sandbox and pin it to the responding endpoint."""
        value, endpoint = self._client._view(
            lambda stubs: stubs.sandbox.Get(api_pb2.SandboxRef(id=sandbox_id)),
            error="inspect returned a non-object response",
        )
        return Sandbox(self._client, value, endpoint=endpoint)

    def ref(self, sandbox_id: str) -> Sandbox:
        """Create a bound sandbox reference without performing I/O."""
        return Sandbox(self._client, {"id": sandbox_id, "name": sandbox_id})

    def list(
        self,
        tags: Mapping[str, str] | None = None,
        node: str | None = None,
    ) -> builtins.list[Sandbox]:
        """List and merge sandboxes across every live roster endpoint."""
        tag_items = [f"{key}={value}" for key, value in sorted(tags.items())] if tags else []
        roster = self._client.driver.endpoints()
        live = [entry for entry in roster if entry.healthy] or roster
        if len(live) <= 1:
            hint = live[0].url if live else None
            response, endpoint = self._fetch(tag_items, hint)
            return self._views(response, endpoint, node)

        results: builtins.list[tuple[api_pb2.ListSandboxesResponse, str] | TransportError] = []
        with ThreadPoolExecutor(max_workers=len(live)) as executor:
            futures = [executor.submit(self._fetch, tag_items, entry.url) for entry in live]
            for future in futures:
                try:
                    results.append(future.result())
                except TransportError as exc:
                    results.append(exc)

        merged: dict[str, Sandbox] = {}
        last_error: TransportError | None = None
        succeeded = False
        for result in results:
            if isinstance(result, TransportError):
                last_error = result
                continue
            succeeded = True
            response, endpoint = result
            for sandbox in self._views(response, endpoint, node):
                merged.setdefault(sandbox.id, sandbox)
        if not succeeded and last_error is not None:
            raise last_error
        return list(merged.values())

    def _fetch(
        self, tag_items: Sequence[str], endpoint: str | None
    ) -> tuple[api_pb2.ListSandboxesResponse, str]:
        return self._client.driver.call(
            lambda stubs: stubs.sandbox.List(api_pb2.ListSandboxesRequest(tags=tag_items)),
            endpoint=endpoint,
        )

    def _views(
        self,
        response: api_pb2.ListSandboxesResponse,
        endpoint: str | None,
        node: str | None,
    ) -> builtins.list[Sandbox]:
        sandboxes: builtins.list[Sandbox] = []
        for payload in response.sandboxes_json:
            row = decode_view(payload, "sandbox list returned a malformed response")
            if not isinstance(row.get("name") or row.get("id"), str):
                raise ProtocolError("sandbox list returned a malformed response")
            if node is not None and row.get("node") != node:
                continue
            sandboxes.append(Sandbox(self._client, row, endpoint=endpoint))
        return sandboxes


class SnapshotAPI:
    """List, restore, and fork VM snapshots."""

    def __init__(self, client: Client) -> None:
        self._client = client

    def list(self) -> builtins.list[str]:
        """Return snapshot names known to the selected daemon."""
        response, _ = self._client.driver.call(
            lambda stubs: stubs.snapshots.List(api_pb2.ListSnapshotsRequest())
        )
        return list(response.snapshots)

    def restore(self, snapshot: str, **kwargs: Any) -> Sandbox:
        """Restore one sandbox from a named snapshot."""
        kwargs = dict(kwargs)
        if kwargs.get("secrets") is not None:
            kwargs["secrets"] = builtins.list(kwargs["secrets"])
        body = self._body(kwargs)
        value, endpoint = self._client._view(
            lambda stubs: stubs.snapshots.Restore(
                api_pb2.RestoreSnapshotRequest(name=snapshot, body_json=dump_json(body))
            ),
            error="restore returned a non-object response",
        )
        sandbox = Sandbox(self._client, value, endpoint=endpoint)
        sandbox._bind_defaults(
            workdir=kwargs.get("workdir"),
            env=kwargs.get("env"),
            tags=kwargs.get("tags"),
            secrets=kwargs.get("secrets"),
        )
        return sandbox

    def fork(self, snapshot: str, count: int = 1, **kwargs: Any) -> builtins.list[Sandbox]:
        """Create copy-on-write clones from a named snapshot."""
        kwargs = dict(kwargs)
        if kwargs.get("secrets") is not None:
            kwargs["secrets"] = builtins.list(kwargs["secrets"])
        count = int(count)
        if count < 1:
            raise ValueError("fork count must be >= 1")
        body = {"count": count, **self._body(kwargs)}
        value, endpoint = self._client._view(
            lambda stubs: stubs.snapshots.Fork(
                api_pb2.ForkSnapshotRequest(name=snapshot, body_json=dump_json(body))
            ),
            error="fork returned a non-object response",
        )
        rows = value.get("clones")
        if not isinstance(rows, list) or len(rows) != count:
            raise ProtocolError("fork returned an unexpected clone count")
        clones: builtins.list[Sandbox] = []
        for row in rows:
            if not isinstance(row, Mapping) or not isinstance(
                row.get("name") or row.get("id"), str
            ):
                raise ProtocolError("fork returned a malformed clone")
            sandbox = Sandbox(self._client, row, endpoint=endpoint)
            sandbox._bind_defaults(
                workdir=kwargs.get("workdir"),
                env=kwargs.get("env"),
                tags=kwargs.get("tags"),
                secrets=kwargs.get("secrets"),
            )
            clones.append(sandbox)
        return clones

    @staticmethod
    def _body(kwargs: Mapping[str, Any]) -> dict[str, Any]:
        extra = _clone_create_extra(kwargs)
        if "volumes" in extra:
            extra["volumes"] = _volume_wire(extra["volumes"])
        if "secrets" in extra:
            extra["secrets"] = _secret_wire(extra["secrets"])
        return extra


class VolumeAPI:
    """Manage persistent named volumes."""

    def __init__(self, client: Client) -> None:
        self._client = client

    def list(self) -> builtins.list[Volume]:
        """Return all named volumes sorted by name."""
        response, _ = self._client.driver.call(
            lambda stubs: stubs.volumes.List(api_pb2.ListVolumesRequest())
        )
        return [Volume(name) for name in sorted(response.volumes)]

    def create(self, name: str) -> Volume:
        """Create a named volume if absent and return its value object."""
        volume = Volume(name)
        self._client.driver.call(
            lambda stubs: stubs.volumes.Create(api_pb2.VolumeRef(name=volume.name))
        )
        return volume

    def delete(self, volume: str | Volume) -> None:
        """Delete a named volume if present."""
        name = volume.name if isinstance(volume, Volume) else Volume(volume).name
        self._client.driver.call(lambda stubs: stubs.volumes.Delete(api_pb2.VolumeRef(name=name)))


class Pool:
    """A bound warm-pool resource managed through its owning client."""

    def __init__(
        self,
        api: PoolAPI,
        reference: str,
        count: int,
        stats: Mapping[str, Any] | None = None,
    ) -> None:
        self.ref = reference
        self.count = int(count)
        self._api = api
        self._stats = dict(stats or {})
        self._deleted = False

    def stats(self) -> PoolStats:
        """Refresh and return this pool's typed server statistics."""
        if not self._deleted:
            for pool in self._api.list():
                if pool.ref == self.ref:
                    self.count = pool.count
                    self._stats = dict(pool._stats)
                    break
        from .models import PoolStats

        return PoolStats.from_dict(self._stats)

    def delete(self) -> None:
        """Delete this pool idempotently."""
        if not self._deleted:
            self._api.delete(self.ref)
            self._deleted = True

    def __enter__(self) -> Pool:
        return self

    def __exit__(self, *exc: object) -> None:
        self.delete()


class PoolAPI:
    """Inspect and resize server-owned warm pools."""

    def __init__(self, client: Client) -> None:
        self._client = client

    def list(self) -> builtins.list[Pool]:
        """Return every warm pool with its latest statistics."""
        value, _ = self._client._view(
            lambda stubs: stubs.pools.List(api_pb2.ListPoolsRequest()),
            error="pool inventory returned a malformed response",
        )
        pools: builtins.list[Pool] = []
        for reference, stats in value.items():
            if not isinstance(reference, str) or not isinstance(stats, Mapping):
                raise ProtocolError("pool inventory returned a malformed response")
            count_value = stats.get("size", stats.get("count", 0))
            try:
                count = int(count_value)
            except (TypeError, ValueError) as exc:
                raise ProtocolError("pool inventory returned a malformed response") from exc
            pools.append(Pool(self, reference, count, stats))
        return pools

    def set(self, ref: str, count: int, **template: Any) -> Pool:
        """Set a warm pool's desired size and return its bound resource."""
        count = int(count)
        if count < 0:
            raise ValueError("pool size must be >= 0")
        body = {
            "size": count,
            **{key: value for key, value in template.items() if value is not None},
        }
        value, _ = self._client._view(
            lambda stubs: stubs.pools.Set(
                api_pb2.PoolSetRequest(reference=ref, body_json=dump_json(body))
            ),
            error="pool update returned a malformed response",
        )
        return Pool(self, ref, count, value)

    def delete(self, ref: str) -> None:
        """Delete one warm pool by portable image or template reference."""
        self._client.driver.call(lambda stubs: stubs.pools.Delete(api_pb2.PoolRef(reference=ref)))

    def clear(self) -> None:
        """Delete every warm pool visible to this client."""
        for pool in self.list():
            pool.delete()


class MeshAPI:
    """Inspect the mesh roster reported by the server."""

    def __init__(self, client: Client) -> None:
        self._client = client

    def status(self) -> MeshStatus:
        """Return typed mesh self, peer, and replica state."""
        value, _ = self._client._view(
            lambda stubs: stubs.system.MeshStatus(api_pb2.MeshStatusRequest()),
            error="mesh status returned a malformed response",
        )
        from .models import MeshStatus

        return MeshStatus.from_dict(value)

    def nodes(self) -> builtins.list[MeshNode]:
        """Return the self node followed by advertised peers."""
        status = self.status()
        return [status.self_node, *status.peers]


def connect(
    dsn: str | None = None,
    *,
    token: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    discover: bool = True,
) -> Client:
    """Create a client from a DSN without performing a network request."""
    config = parse_dsn(dsn)
    timeout_override = None if timeout == DEFAULT_TIMEOUT else timeout
    discover_override = None if discover else False
    driver = MeshDriver(
        config,
        token=token,
        timeout=timeout_override,
        discover=discover_override,
    )
    return Client(driver)
