"""Tolerant typed views over open vmon API response objects."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

from .errors import ProtocolError


def _raw(data: Mapping[str, Any] | None) -> dict[str, Any]:
    return dict(data or {})


def _string(value: object, default: str = "") -> str:
    return value if isinstance(value, str) else default


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _int(value: object, default: int = 0) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else default


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _float(value: object, default: float = 0.0) -> float:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    return default


def _optional_float(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None


@dataclass(frozen=True, slots=True)
class SandboxInfo:
    """A typed sandbox lifecycle view with unknown response fields preserved in ``raw``."""

    id: str
    name: str = ""
    status: str = ""
    pid: int | None = None
    source: str | None = None
    created_at: float = 0.0
    last_active: float = 0.0
    expires_at: float | None = None
    terminated_at: float | None = None
    error: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    returncode: int | None = None
    node: str | None = None
    raw: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SandboxInfo:
        """Parse known sandbox fields while tolerating additions from newer servers."""
        identifier = _string(data.get("id")) or _string(data.get("name"))
        tags_value = data.get("tags")
        tags = (
            {
                str(key): str(value)
                for key, value in tags_value.items()
                if isinstance(key, str) and isinstance(value, str)
            }
            if isinstance(tags_value, Mapping)
            else {}
        )
        return cls(
            id=identifier,
            name=_string(data.get("name"), identifier),
            status=_string(data.get("status")),
            pid=_optional_int(data.get("pid")),
            source=_optional_string(data.get("source")),
            created_at=_float(data.get("created_at")),
            last_active=_float(data.get("last_active")),
            expires_at=_optional_float(data.get("expires_at")),
            terminated_at=_optional_float(data.get("terminated_at")),
            error=_optional_string(data.get("error")),
            tags=tags,
            returncode=_optional_int(data.get("returncode")),
            node=_optional_string(data.get("node")),
            raw=_raw(data),
        )


@dataclass(frozen=True, slots=True)
class ExecExit:
    """The terminal code and optional signal from a streaming process."""

    code: int
    signal: int | None = None
    raw: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ExecExit:
        """Parse a process exit envelope."""
        return cls(
            code=_int(data.get("exit"), -1),
            signal=_optional_int(data.get("signal")),
            raw=_raw(data),
        )


@dataclass(frozen=True, slots=True)
class FileInfo:
    """Metadata for a guest filesystem entry."""

    ok: bool = False
    name: str = ""
    type: str = ""
    size: int = 0
    mode: int = 0
    mtime: int = 0
    raw: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FileInfo:
        """Parse file metadata while preserving unknown fields."""
        return cls(
            ok=data.get("ok") is True,
            name=_string(data.get("name")),
            type=_string(data.get("type")),
            size=_int(data.get("size")),
            mode=_int(data.get("mode")),
            mtime=_int(data.get("mtime")),
            raw=_raw(data),
        )


@dataclass(frozen=True, slots=True)
class MeshNode:
    """A mesh member's identity, advertised API URL, and placement region."""

    node_id: str
    advertise: str
    region: str = ""
    raw: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MeshNode:
        """Parse the stable mesh-node identity fields."""
        return cls(
            node_id=_string(data.get("node_id")),
            advertise=_string(data.get("advertise")),
            region=_string(data.get("region")),
            raw=_raw(data),
        )


@dataclass(frozen=True, slots=True)
class MeshStatus:
    """Typed mesh membership with raw status fields retained for advanced placement data."""

    self_node: MeshNode
    peers: list[MeshNode] = field(default_factory=list)
    replicas_held: int = 0
    raw: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MeshStatus:
        """Parse the nested ``self`` node and peer roster from mesh status."""
        self_value = data.get("self")
        self_data = self_value if isinstance(self_value, Mapping) else data
        peers_value = data.get("peers")
        peers = (
            [MeshNode.from_dict(peer) for peer in peers_value if isinstance(peer, Mapping)]
            if isinstance(peers_value, list)
            else []
        )
        return cls(
            self_node=MeshNode.from_dict(self_data),
            peers=peers,
            replicas_held=_int(data.get("replicas_held")),
            raw=_raw(data),
        )


@dataclass(frozen=True, slots=True)
class PoolStats:
    """A warm pool's desired size and cumulative acquisition counters."""

    ready: int = 0
    hits: int = 0
    misses: int = 0
    size: int = 0
    raw: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PoolStats:
        """Parse warm-pool counters while tolerating server additions."""
        return cls(
            ready=_int(data.get("ready", data.get("ready_count"))),
            hits=_int(data.get("hits")),
            misses=_int(data.get("misses")),
            size=_int(data.get("size", data.get("count"))),
            raw=_raw(data),
        )


@dataclass(frozen=True, slots=True)
class ServerInfo:
    """Daemon version, host platform, backend, and advertised capabilities."""

    version: str = ""
    platform: str = ""
    arch: str = ""
    backend: str = ""
    capabilities: dict[str, bool] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ServerInfo:
        """Parse stable server information while retaining the full response."""
        version = data.get("version")
        if not isinstance(version, str) or not version:
            raise ProtocolError("server info response did not include a version")
        for field_name in ("platform", "arch", "backend"):
            value = data.get(field_name)
            if value is not None and not isinstance(value, str):
                raise ProtocolError(f"server info {field_name} field must be a string")
        capabilities_value = data.get("capabilities")
        if capabilities_value is None:
            capabilities: dict[str, bool] = {}
        elif isinstance(capabilities_value, Mapping) and all(
            isinstance(key, str) and isinstance(value, bool)
            for key, value in capabilities_value.items()
        ):
            capabilities = dict(capabilities_value)
        else:
            raise ProtocolError("server info capabilities field must be a boolean object")
        return cls(
            version=version,
            platform=_string(data.get("platform")),
            arch=_string(data.get("arch")),
            backend=_string(data.get("backend")),
            capabilities=capabilities,
            raw=_raw(data),
        )


@dataclass(frozen=True, slots=True)
class Health:
    """Daemon health state with the original response retained."""

    ok: bool
    raw: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Health:
        """Parse a health response, requiring its boolean status."""
        ok = data.get("ok")
        if not isinstance(ok, bool):
            raise ProtocolError("health response did not include a boolean ok field")
        return cls(ok=ok, raw=_raw(data))


@dataclass(frozen=True, slots=True, eq=False)
class SandboxMetrics(Mapping[str, Any]):
    """Open sandbox runtime metrics exposed as a read-only mapping."""

    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SandboxMetrics:
        """Preserve every runtime metric supplied by the daemon."""
        return cls(raw=_raw(data))

    def __getitem__(self, key: str) -> Any:
        return self.raw[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.raw)

    def __len__(self) -> int:
        return len(self.raw)


@dataclass(frozen=True, slots=True)
class SandboxNetworkPolicy:
    """A sandbox's egress policy and allowlists."""

    block_network: bool | None = None
    cidr_allow: tuple[str, ...] = ()
    domain_allow: tuple[str, ...] = ()
    raw: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SandboxNetworkPolicy:
        """Parse network-policy fields and reject malformed allowlists."""
        block_network = data.get("block_network")
        if block_network is not None and not isinstance(block_network, bool):
            raise ProtocolError("network policy block_network field must be boolean")
        return cls(
            block_network=block_network,
            cidr_allow=_string_tuple(data.get("cidr_allow"), "cidr_allow"),
            domain_allow=_string_tuple(data.get("domain_allow"), "domain_allow"),
            raw=_raw(data),
        )

    def to_dict(self) -> dict[str, object]:
        """Return the request body for replacing this network policy."""
        value: dict[str, object] = {
            "cidr_allow": list(self.cidr_allow),
            "domain_allow": list(self.domain_allow),
        }
        if self.block_network is not None:
            value["block_network"] = self.block_network
        return value


@dataclass(frozen=True, slots=True)
class TunnelTarget:
    """One daemon-side TCP target for an exposed guest port."""

    host: str
    port: int


@dataclass(frozen=True, slots=True, eq=False)
class TunnelSet(Mapping[int, TunnelTarget]):
    """Sandbox tunnels plus the short-lived port-proxy token."""

    tunnels: dict[int, TunnelTarget] = field(default_factory=dict)
    connect_token: str | None = None
    raw: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TunnelSet:
        """Parse every advertised tunnel and reject incomplete targets."""
        token = data.get("connect_token")
        if token is not None and not isinstance(token, str):
            raise ProtocolError("tunnel response connect_token field must be a string")
        raw_tunnels = data.get("tunnels", {})
        if not isinstance(raw_tunnels, Mapping):
            raise ProtocolError("tunnel response tunnels field must be an object")
        tunnels: dict[int, TunnelTarget] = {}
        for raw_guest_port, raw_target in raw_tunnels.items():
            if isinstance(raw_guest_port, bool) or not isinstance(raw_guest_port, int | str):
                raise ProtocolError("tunnel response included an invalid guest port")
            if not isinstance(raw_target, Mapping):
                raise ProtocolError("tunnel response included an invalid target")
            host = raw_target.get("host")
            raw_host_port = raw_target.get("port")
            if (
                not isinstance(host, str)
                or isinstance(raw_host_port, bool)
                or not isinstance(raw_host_port, int)
            ):
                raise ProtocolError("tunnel response included an invalid target")
            try:
                guest_port = int(raw_guest_port)
            except ValueError as exc:
                raise ProtocolError("tunnel response included an invalid guest port") from exc
            tunnels[guest_port] = TunnelTarget(host=host, port=raw_host_port)
        return cls(tunnels=tunnels, connect_token=token, raw=_raw(data))

    def __getitem__(self, port: int) -> TunnelTarget:
        return self.tunnels[port]

    def __iter__(self) -> Iterator[int]:
        return iter(self.tunnels)

    def __len__(self) -> int:
        return len(self.tunnels)


@dataclass(frozen=True, slots=True, eq=False)
class EventRecord(Mapping[str, Any]):
    """One open-ended daemon lifecycle event."""

    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EventRecord:
        """Preserve an event document for forward-compatible consumers."""
        return cls(raw=_raw(data))

    def __getitem__(self, key: str) -> Any:
        return self.raw[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.raw)

    def __len__(self) -> int:
        return len(self.raw)


def _string_tuple(value: object, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ProtocolError(f"network policy {field_name} field must be a string array")
    return tuple(value)
