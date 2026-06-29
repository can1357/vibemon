"""Cluster membership, gossip, and placement policy for meshed gateways."""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import os
import platform
import secrets
import socket
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .core import state_dir

_CPU_BASELINE_CACHE: str | None = None


class MeshError(Exception):
    """A mesh operation failed with a stable machine-readable code."""

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        known = {"invalid", "unauthorized", "unreachable", "ambiguous", "conflict"}
        self.message = message
        self.code = code or (message if message in known else "invalid")


@dataclass
class NodeCaps:
    """Advertised hard capacity for placement decisions."""

    vcpus: int
    mem_mib: int


@dataclass
class NodeState:
    """A gossiped snapshot of one mesh member's placement state."""

    node_id: str
    advertise: str
    region: str = ""
    caps: NodeCaps = field(default_factory=lambda: NodeCaps(1, 2048))
    committed_vcpus: int = 0
    committed_mem_mib: int = 0
    inflight: int = 0
    templates: list[str] = field(default_factory=list)
    pools: dict[str, int] = field(default_factory=dict)
    owned: list[str] = field(default_factory=list)
    owned_epochs: dict[str, int] = field(default_factory=dict)
    ts: float = 0.0
    last_seen: float = 0.0
    backend: str = ""
    arch: str = ""
    cpu_baseline: str = ""
    template_index: dict[str, str] = field(default_factory=dict)

    @property
    def free_vcpus(self) -> int:
        """Return currently uncommitted vCPU capacity."""
        return max(0, self.caps.vcpus - self.committed_vcpus)

    @property
    def free_mem_mib(self) -> int:
        """Return currently uncommitted memory capacity in MiB."""
        return max(0, self.caps.mem_mib - self.committed_mem_mib)

    def healthy(self, now: float, interval: float) -> bool:
        """Return whether this peer has been seen within three heartbeat windows."""
        return self.last_seen > 0.0 and now - self.last_seen < 3.0 * interval

    def to_wire(self) -> dict[str, Any]:
        """Serialize this state for mesh HTTP gossip."""
        return {
            "node_id": self.node_id,
            "advertise": self.advertise,
            "region": self.region,
            "backend": self.backend,
            "arch": self.arch,
            "cpu_baseline": self.cpu_baseline,
            "vcpus": self.caps.vcpus,
            "mem_mib": self.caps.mem_mib,
            "committed_vcpus": self.committed_vcpus,
            "committed_mem_mib": self.committed_mem_mib,
            "inflight": self.inflight,
            "templates": list(self.templates),
            "template_index": dict(self.template_index),
            "pools": dict(self.pools),
            "owned": list(self.owned),
            "owned_epochs": dict(self.owned_epochs),
            "ts": self.ts,
            "last_seen": self.last_seen,
            "free_vcpus": self.free_vcpus,
            "free_mem_mib": self.free_mem_mib,
        }

    @classmethod
    def from_wire(cls, data: dict[str, Any]) -> NodeState:
        """Deserialize a gossiped node state from JSON-compatible data."""
        caps_raw: dict[str, Any] = data["caps"] if isinstance(data.get("caps"), dict) else {}
        caps = NodeCaps(
            vcpus=int(data.get("vcpus") or caps_raw.get("vcpus") or 1),
            mem_mib=int(data.get("mem_mib") or caps_raw.get("mem_mib") or 2048),
        )
        pools_raw: dict[str, Any] = data["pools"] if isinstance(data.get("pools"), dict) else {}
        template_index_raw: dict[str, Any] = (
            data["template_index"] if isinstance(data.get("template_index"), dict) else {}
        )
        owned_epochs_raw = dict(data.get("owned_epochs") or {})
        return cls(
            node_id=str(data.get("node_id") or ""),
            advertise=str(data.get("advertise") or ""),
            region=str(data.get("region") or ""),
            backend=str(data.get("backend") or ""),
            arch=str(data.get("arch") or ""),
            cpu_baseline=str(data.get("cpu_baseline") or ""),
            caps=caps,
            committed_vcpus=int(data.get("committed_vcpus") or 0),
            committed_mem_mib=int(data.get("committed_mem_mib") or 0),
            inflight=int(data.get("inflight") or 0),
            templates=[str(item) for item in data.get("templates") or []],
            pools={str(key): int(value) for key, value in pools_raw.items()},
            template_index={
                str(key): str(value) for key, value in template_index_raw.items() if key and value
            },
            owned=[str(item) for item in data.get("owned") or []],
            owned_epochs={str(key): int(value) for key, value in owned_epochs_raw.items()},
            ts=float(data.get("ts") or 0.0),
            last_seen=float(data.get("last_seen") or 0.0),
        )


class MeshTransport(Protocol):
    """Unary inter-node HTTP transport used by mesh membership and placement."""

    def post(
        self, base_url: str, path: str, payload: dict[str, Any], *, timeout: float | None = None
    ) -> dict[str, Any]: ...

    def get(self, base_url: str, path: str, *, timeout: float | None = None) -> dict[str, Any]: ...


class HttpxTransport:
    """Synchronous httpx transport for server-side node-to-node calls."""

    def __init__(self, token: str, timeout: float = 5.0) -> None:
        import httpx

        self._client = httpx.Client(timeout=timeout)
        self._token = token

    def post(
        self, base_url: str, path: str, payload: dict[str, Any], *, timeout: float | None = None
    ) -> dict[str, Any]:
        """POST a JSON payload to a peer and return its JSON object response."""
        return self._request("POST", base_url, path, payload=payload, timeout=timeout)

    def get(self, base_url: str, path: str, *, timeout: float | None = None) -> dict[str, Any]:
        """GET a JSON object response from a peer."""
        return self._request("GET", base_url, path, timeout=timeout)

    def _request(
        self,
        method: str,
        base_url: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        import httpx

        url = base_url.rstrip("/") + path
        headers = {
            "Authorization": f"Bearer {self._token}",
            "X-Vmon-Mesh-Hop": "1",
        }
        # A longer per-call deadline for slow create/fork dispatch must not slow
        # heartbeat failure detection, so it overrides the short client default
        # only for this request.
        kwargs: dict[str, Any] = {"json": payload, "headers": headers}
        if timeout is not None:
            kwargs["timeout"] = timeout
        try:
            response = self._client.request(method, url, **kwargs)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
            # Connection never established: the request provably did not run, so a
            # caller may safely reroute it to another node.
            raise MeshError("peer unreachable", code="unreachable") from exc
        except httpx.HTTPError as exc:
            # Bytes were on the wire with no clean response (read timeout, reset,
            # protocol error): the peer MAY have committed, so the outcome is
            # unknown and the caller must NOT reroute — it duplicates committed work.
            raise MeshError("peer response ambiguous", code="ambiguous") from exc
        if response.status_code < 200 or response.status_code >= 300:
            code = _mesh_code_for_status(response.status_code)
            detail = _response_detail(response)
            raise MeshError(detail or "mesh peer rejected request", code=code)
        if not response.content:
            return {}
        data = response.json()
        if not isinstance(data, dict):
            raise MeshError("mesh peer returned non-object JSON")
        return data

    def close(self) -> None:
        """Close idle HTTP connections held by the transport."""
        self._client.close()


def _mesh_code_for_status(status: int) -> str:
    if status in {401, 403}:
        return "unauthorized"
    if status == 409:
        return "conflict"
    if status >= 500:
        # A 5xx response means the peer handled the request and may have
        # committed before failing, so a mutating caller must not reroute it.
        return "ambiguous"
    return "invalid"


def _response_detail(response: Any) -> str:
    with contextlib.suppress(Exception):
        data = response.json()
        detail = data.get("detail") if isinstance(data, dict) else None
        if isinstance(detail, str):
            return detail
    return getattr(response, "text", "")


def new_node_id() -> str:
    """Return a new compact random node id."""
    return secrets.token_hex(8)


def probe_caps() -> NodeCaps:
    """Probe this host's default advertised capacity."""
    return NodeCaps(os.cpu_count() or 1, _total_mem_mib())


def probe_compat() -> tuple[str, str]:
    """Probe this host's snapshot-restore compatibility class as ``(backend, arch)``.

    Snapshots are backend- and arch-specific (a KVM snapshot restores only on a
    KVM build; an arm64 image won't boot on x86), so placement must match both.
    For x86/KVM this is necessary but not sufficient; ``cpu_baseline`` gates
    the finer CPUID/XSAVE restore surface.
    """
    from .vmm import _arch

    backend = "hvf" if platform.system() == "Darwin" else "kvm"
    return backend, _arch()


def probe_cpu_baseline() -> str:
    """Probe this host's restore-relevant CPU feature baseline."""
    global _CPU_BASELINE_CACHE
    if _CPU_BASELINE_CACHE is not None:
        return _CPU_BASELINE_CACHE
    from .vmm import _arch, find_binary

    arch = _arch()
    if arch != "x86_64":
        baseline = f"arch:{arch}"
    else:
        # An x86 host with a working KVM probe bakes a precise CPUID/XSAVE surface
        # into its snapshots; a failed probe must NOT masquerade as a generic arch
        # class, or fine-grained restore gating silently turns off.
        baseline = "unknown:x86_64"
        with contextlib.suppress(Exception):
            proc = subprocess.run(
                [find_binary(), "--print-cpu-baseline"],
                capture_output=True,
                text=True,
                check=True,
                timeout=5,
            )
            if out := proc.stdout.strip():
                baseline = out
    _CPU_BASELINE_CACHE = baseline
    return baseline


def cpu_baseline_covers(have: str, need: str) -> bool:
    """Return whether candidate ``have`` covers ingress ``need``'s restore surface.

    ``need`` is the snapshot-baking ingress baseline; ``have`` is a candidate's.
    Callers arch-filter first. An empty or arch-token ``need`` (aarch64's single
    class) imposes no fine-grained gate. A real x86 CPUID/XSAVE ``need`` must be a
    strict subset of a JSON ``have``: a candidate advertising no comparable
    surface (empty, arch-token, or ``unknown:`` ``have``) cannot be verified and
    is rejected. An ``unknown:`` ``need`` (a failed x86 probe) is never trusted,
    so a ref-bound restore from such an ingress strands to the local node rather
    than gamble on an unverifiable surface.
    """
    if not need:
        return True
    if need.startswith("unknown:"):
        return False
    if need.startswith("arch:"):
        # An arch-token need (aarch64 single class) imposes no fine-grained gate;
        # any same-arch candidate (callers arch-filter first) is fine, including a
        # pre-upgrade peer with no advertised baseline. Only a DIFFERENT arch
        # token is incompatible.
        return not have.startswith("arch:") or have == need
    if not have or have.startswith(("arch:", "unknown:")):
        return False
    try:
        have_obj = json.loads(have)
        need_obj = json.loads(need)
    except json.JSONDecodeError:
        return have == need
    if not isinstance(have_obj, dict) or not isinstance(need_obj, dict):
        return False
    for key, need_val in need_obj.items():
        if key not in have_obj:
            return False
        if key == "v":
            if have_obj[key] != need_val:
                return False
            continue
        try:
            have_int = int(have_obj[key])
            need_int = int(need_val)
        except TypeError, ValueError:
            return False
        if have_int & need_int != need_int:
            return False
    return True


def _total_mem_mib() -> int:
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        return int(pages * page_size // 1_048_576)
    except ValueError, OSError, AttributeError:
        pass
    with contextlib.suppress(Exception):
        proc = subprocess.run(
            ["sysctl", "-n", "hw.memsize"],
            capture_output=True,
            text=True,
            check=True,
        )
        return int(int(proc.stdout.strip()) // 1_048_576)
    return (os.cpu_count() or 1) * 2048


def default_advertise(host: str, port: int) -> str:
    """Return the URL peers should use for a bound host and port."""
    if host in {"0.0.0.0", "127.0.0.1", "::", "localhost"}:
        host = _primary_ip()
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{port}"


def _primary_ip() -> str:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as sock:
        try:
            sock.connect(("8.8.8.8", 80))
            return str(sock.getsockname()[0])
        except OSError:
            return "127.0.0.1"


def encode_blob(url: str, token: str) -> str:
    """Encode an advertise URL and cluster token into a join blob."""
    raw = json.dumps({"url": url, "token": token}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_blob(blob: str) -> tuple[str, str]:
    """Decode a mesh join blob into its advertise URL and cluster token."""
    try:
        padded = blob + "=" * (-len(blob) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
        url = data["url"]
        token = data["token"]
        if not isinstance(url, str) or not isinstance(token, str) or not url or not token:
            raise ValueError
        return url, token
    except Exception as exc:
        raise MeshError("invalid join blob") from exc


def _weight(name: str, default: float) -> float:
    with contextlib.suppress(ValueError):
        return float(os.environ.get(f"VMON_MESH_W_{name}", str(default)))
    return default


W_WARM = _weight("WARM", 1000.0)
W_FREE = _weight("FREE", 100.0)
W_LOCAL = _weight("LOCAL", 50.0)
W_REGION = _weight("REGION", 30.0)
W_INFLIGHT = _weight("INFLIGHT", 80.0)


def template_key(
    ref: str | None,
    *,
    disk_mb: int,
    memory: int,
    cpus: int,
    fs_slots: int,
    host_slot: bool,
    nic_slot: bool,
    tap_slot: bool = False,
) -> str:
    """Return the request-derivable identity for a bootable warm template."""
    from .image import _normalize_reference

    normalized = _normalize_reference(ref)
    if normalized is None:
        raise ValueError("template key requires an image reference")
    return (
        f"{normalized}|d{int(disk_mb)}|m{int(memory)}|c{int(cpus)}"
        f"|s{int(fs_slots)}|h{int(host_slot)}|n{int(nic_slot)}|t{int(tap_slot)}"
    )


def _volume_count(value: Any) -> int:
    if isinstance(value, dict):
        return len(value)
    if isinstance(value, list | tuple):
        return len(value)
    return 0


def request_template_key(req: dict[str, Any]) -> str | None:
    """Return the cross-node warm-template key for a request, if pull-warm applies.

    Pull-warm covers the plain block-network template and the Linux TAP networked
    flavor: after a pull the create rewrites to ``template=<pulled dir>`` (block
    network) or warm-restores the tap-slot snapshot onto a per-sandbox TAP
    (networked). Requests with ``fs_dir``/volumes still need a local image to
    rebind/enumerate devices on restore, so they fall back to a local build.
    """
    ref = req.get("image")
    if (
        not ref
        or req.get("template")
        or req.get("dockerfile")
        or req.get("context") not in (None, ".")
        or req.get("fs_dir") is not None
        or _volume_count(req.get("volumes"))
    ):
        return None
    block_network = bool(req.get("block_network"))
    # macOS user-net networked warm is host-local (single-node); only the Linux
    # TAP flavor is cross-node pull-warm. A networked request therefore keys to
    # the TAP flavor on Linux and is not pull-warm on macOS.
    if not block_network and platform.system() == "Darwin":
        return None
    try:
        return template_key(
            ref,
            disk_mb=int(req.get("disk_mb") or 1024),
            memory=int(req.get("memory") or req.get("mem") or 512),
            cpus=int(req.get("cpus") or 1),
            fs_slots=0,
            host_slot=False,
            nic_slot=False,
            tap_slot=not block_network,
        )
    except TypeError, ValueError:
        return None


def pool_ref(req: dict[str, Any]) -> str | None:
    """Return the image/template ref a request would use for a warm pool."""
    return str(req.get("template") or req.get("image") or "") or None


def pool_eligible(req: dict[str, Any]) -> bool:
    """Return whether a create request can claim a warm-pool clone.

    Pooled clones are generic block-network forks. A caller-requested ``name`` is
    fine (the clone is renamed on claim) and a prewarmed pool serves requests
    that omit ``pool_size``, so neither gates eligibility. Named volumes and
    host-dir (``fs_dir``) shares need a restore-with-rebind the pool cannot
    serve, so they remain ineligible.
    """
    return bool(req.get("block_network")) and not req.get("volumes") and not req.get("fs_dir")


def score_node(
    node: NodeState, req: dict[str, Any], *, ingress_id: str, ingress_region: str
) -> float:
    """Score one candidate node for a placement request."""
    ref = pool_ref(req)
    key = request_template_key(req)
    pool_warm = 1.0 if pool_eligible(req) and ref and node.pools.get(ref, 0) > 0 else 0.0
    template_warm = 1.0 if key and key in node.template_index else 0.0
    warm = max(pool_warm, template_warm)
    free_frac = node.free_vcpus / node.caps.vcpus if node.caps.vcpus else 0.0
    local = 1.0 if node.node_id == ingress_id else 0.0
    region = 1.0 if ingress_region and node.region == ingress_region else 0.0
    inflight_frac = min(1.0, node.inflight / (node.caps.vcpus or 1))
    return (
        W_WARM * warm
        + W_FREE * free_frac
        + W_LOCAL * local
        + W_REGION * region
        - W_INFLIGHT * inflight_frac
    )


def _hrw_score(key: str, node_id: str) -> int:
    """Rendezvous-hash weight of *node_id* for *key*; the highest weight wins."""
    digest = hashlib.blake2b(f"{key}\x00{node_id}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big")


class Mesh:
    """Owns local mesh state, membership persistence, gossip, and placement."""

    def __init__(
        self,
        engine: Any,
        *,
        advertise: str,
        token: str,
        transport: MeshTransport,
        region: str = "",
        caps: NodeCaps | None = None,
        backend: str | None = None,
        arch: str | None = None,
        state_path: Path | None = None,
    ) -> None:
        self.engine = engine
        self._default_advertise = advertise
        self.advertise = advertise
        self.token = token
        self.transport = transport
        self.region = region
        self.caps = caps or probe_caps()
        probed_backend, probed_arch = probe_compat()
        self.backend = backend or probed_backend
        self.arch = arch or probed_arch
        self.cpu_baseline = probe_cpu_baseline()
        self.state_path = state_path or state_dir() / "mesh.json"
        self.node_id = new_node_id()
        self.enabled = False
        self.interval = float(os.environ.get("VMON_MESH_HEARTBEAT_SEC", "3.0"))
        self.reap_sec = float(os.environ.get("VMON_MESH_REAP_SEC", "300.0"))
        self._lock = threading.RLock()
        self._peers: dict[str, NodeState] = {}
        self._expected_members: int = 1
        self._owners: dict[str, str] = {}
        self._explicit_owners: dict[str, tuple[str, float]] = {}
        self._owner_epoch: dict[str, tuple[int, str]] = {}
        self._local_epoch: dict[str, int] = {}
        self._inflight = 0
        self._orphans: list[tuple[str, str]] = []
        self._stats: dict[str, int] = {"replication": 0, "restore": 0, "fence": 0}
        self.idem_ttl = float(os.environ.get("VMON_MESH_IDEM_TTL_SEC", "900.0"))
        self.create_timeout = float(os.environ.get("VMON_MESH_CREATE_TIMEOUT_SEC", "120.0"))
        # key -> (workload owner node id, pin timestamp): a coordinator's transient
        # record fixing one owner per idempotency key so retries converge.
        self._idem_pins: dict[str, tuple[str, float]] = {}
        # idempotency keys whose create is in flight on THIS node (worker serialization).
        self._idem_busy: set[str] = set()

    def self_state(self) -> NodeState:
        """Build a fresh placement snapshot for this node."""
        from .sandbox import pool_inventory

        committed_vcpus, committed_mem_mib = self.engine.committed()
        template_index_fn = getattr(self.engine, "template_index", None)
        template_index = template_index_fn() if callable(template_index_fn) else {}
        now = time.time()
        with self._lock:
            node_id = self.node_id or new_node_id()
            if not self.node_id:
                self.node_id = node_id
            owned_ids = self.engine.owned_ids()
            return NodeState(
                node_id=node_id,
                advertise=self.advertise,
                region=self.region,
                backend=self.backend,
                arch=self.arch,
                cpu_baseline=self.cpu_baseline,
                caps=self.caps,
                committed_vcpus=committed_vcpus,
                committed_mem_mib=committed_mem_mib,
                inflight=self._inflight,
                templates=self.engine.templates_present(),
                pools=pool_inventory(),
                template_index=template_index,
                owned=owned_ids,
                owned_epochs={sid: self._local_epoch.get(sid, 0) for sid in owned_ids},
                ts=now,
                last_seen=now,
            )

    def peers(self) -> list[NodeState]:
        """Return the currently known peer snapshots."""
        with self._lock:
            return list(self._peers.values())

    def _bump_expected(self) -> bool:
        """Raise expected cluster size to the current known membership.

        Callers must hold ``self._lock``. This is a high-water mark: reap/partition
        paths do not shrink it, so quorum stays sized against the expected cluster.
        Quorum restore needs at least 3 nodes to tolerate 1 failure; a 2-node
        cluster cannot form a majority after losing one and correctly defers.
        """
        before = self._expected_members
        self._expected_members = max(self._expected_members, 1 + len(self._peers))
        return self._expected_members != before

    def is_peer_healthy(self, node_id: str) -> bool:
        """Return whether *node_id* is currently reachable from this node."""
        now = time.time()
        with self._lock:
            if node_id == self.node_id:
                return True
            peer = self._peers.get(node_id)
            return bool(peer is not None and peer.healthy(now, self.interval))

    def live_member_ids(self) -> list[str]:
        """Return this node plus peers that are healthy right now."""
        now = time.time()
        with self._lock:
            members = [self.node_id]
            members.extend(
                peer.node_id for peer in self._peers.values() if peer.healthy(now, self.interval)
            )
            return members

    def quorum_needed(self) -> int:
        """Return the strict majority required for the expected cluster size."""
        with self._lock:
            return self._expected_members // 2 + 1

    def restore_quorum_met(self, confirmations: int) -> bool:
        """Return whether unreachable confirmations meet restore quorum."""
        return confirmations >= self.quorum_needed()

    def note_event(self, kind: str) -> None:
        """Increment an HA event counter (replication/restore/fence) for status()."""
        with self._lock:
            self._stats[kind] = self._stats.get(kind, 0) + 1

    def status(self) -> dict[str, Any]:
        """Return a JSON-ready mesh status snapshot for operators."""
        now = time.time()
        with self._lock:
            self_state = self.self_state().to_wire()
            self_state["healthy"] = True
            self_state["stats"] = dict(self._stats)
            peers = []
            for peer in self._peers.values():
                wire = peer.to_wire()
                wire["healthy"] = peer.healthy(now, self.interval)
                peers.append(wire)
            return {
                "node_id": self.node_id,
                "region": self.region,
                "enabled": self.enabled,
                "self": self_state,
                "peers": peers,
            }

    def setup(
        self, advertise: str | None = None, *, region: str = "", caps: NodeCaps | None = None
    ) -> str:
        """Enable this node as a seed and return a blob other nodes can join with."""
        with self._lock:
            self.advertise = advertise or self._default_advertise
            self.region = region
            if caps is not None:
                self.caps = caps
            if not self.node_id:
                self.node_id = new_node_id()
            self.enabled = True
            self.save()
            return encode_blob(self.advertise, self.token)

    def join(self, blob: str, *, advertise: str | None = None, region: str = "") -> dict[str, Any]:
        """Join this node to an existing mesh through a seed join blob."""
        seed_url, token = decode_blob(blob)
        if token != self.token:
            raise MeshError("unauthorized")
        with self._lock:
            self.advertise = advertise or self._default_advertise
            self.region = region
            if not self.node_id:
                self.node_id = new_node_id()
        response = self.transport.post(seed_url, "/v1/mesh/members", self.self_state().to_wire())
        now = time.time()
        with self._lock:
            for item in response.get("members") or []:
                node = NodeState.from_wire(item)
                if node.node_id and node.node_id != self.node_id:
                    node.last_seen = now
                    self._peers[node.node_id] = node
            self.enabled = True
            self._rebuild_owners_locked()
            self.save()
        return self.status()

    def leave(self) -> None:
        """Leave the mesh without migrating any running sandboxes."""
        with self._lock:
            peers = list(self._peers.values())
            node_id = self.node_id
        for peer in peers:
            with contextlib.suppress(Exception):
                self.transport.post(peer.advertise, "/v1/mesh/depart", {"node_id": node_id})
        with self._lock:
            self._peers.clear()
            self._owners.clear()
            self._explicit_owners.clear()
            self.enabled = False
            self._expected_members = 1
            self.save()

    def register(self, node: NodeState) -> dict[str, Any]:
        """Register a joining peer and return the full known membership list."""
        now = time.time()
        with self._lock:
            if node.node_id and node.node_id != self.node_id:
                node.last_seen = now
                self._peers[node.node_id] = node
                self.enabled = True
            self._rebuild_owners_locked()
            self._bump_expected()
            self.save()
            members = [self.self_state(), *self._peers.values()]
            return {"members": [member.to_wire() for member in members]}

    def heartbeat(self, incoming: NodeState, known: list[dict[str, Any]]) -> dict[str, Any]:
        """Merge a peer heartbeat and return this node's state plus known members."""
        now = time.time()
        with self._lock:
            if incoming.node_id and incoming.node_id != self.node_id:
                incoming.last_seen = now
                self._peers[incoming.node_id] = incoming
            for item in known:
                node = NodeState.from_wire(item)
                if not node.node_id or node.node_id == self.node_id or node.node_id in self._peers:
                    continue
                node.last_seen = now if node.last_seen <= 0.0 else node.last_seen
                self._peers[node.node_id] = node
            self._rebuild_owners_locked()
            return {
                "state": self.self_state().to_wire(),
                "known": [node.to_wire() for node in [self.self_state(), *self._peers.values()]],
            }

    def depart(self, node_id: str) -> None:
        """Forget a peer that announced it is leaving."""
        with self._lock:
            self._peers.pop(node_id, None)
            self._expected_members = max(1, self._expected_members - 1)
            self._explicit_owners = {
                sid: entry for sid, entry in self._explicit_owners.items() if entry[0] != node_id
            }
            self._rebuild_owners_locked()
            self.save()

    def mark_unhealthy(self, node_id: str) -> None:
        """Mark a peer stale so the next placement avoids it."""
        with self._lock:
            peer = self._peers.get(node_id)
            if peer is not None:
                peer.last_seen = 0.0

    def pinned_local(self, req: dict[str, Any]) -> bool:
        """Return whether a request references ingress-host-local input."""
        return bool(
            req.get("dockerfile")
            or req.get("fs_dir")
            or req.get("volumes")
            or req.get("context") not in (None, ".")
        )

    def candidates(self, req: dict[str, Any]) -> list[NodeState]:
        """Return healthy placement candidates for a create/restore/fork request."""
        now = time.time()
        with self._lock:
            nodes = [self.self_state()]
            nodes.extend(peer for peer in self._peers.values() if peer.healthy(now, self.interval))
        ref = str(req.get("template") or req.get("snapshot") or "")
        # Compat is derived from THIS ingress node, never caller-supplied request
        # JSON (unvalidated public input): a client must not steer placement onto
        # an incompatible node. The guest kernel and injected agent are arch-
        # specific, so EVERY placement (even a fresh boot) must match this arch.
        nodes = [n for n in nodes if n.arch == self.arch]
        # A named snapshot/template restore or fork reuses a backend-specific
        # memory image, so it must land on a same-backend node that already holds
        # the template. A zero-config image request (warm-pooled or not) is not
        # backend-bound: each node builds its own backend-appropriate template, so
        # warm preference stays a scoring bias (see score_node), not a hard filter.
        if ref:
            nodes = [
                n
                for n in nodes
                if n.backend == self.backend
                and ref in n.templates
                and cpu_baseline_covers(n.cpu_baseline, self.cpu_baseline)
            ]
        return nodes

    def place(self, req: dict[str, Any]) -> str:
        """Choose the owner node id for a placement request."""
        if self.pinned_local(req):
            return self.node_id
        candidates = self.candidates(req)
        if not candidates:
            return self.node_id
        req_cpus = int(req.get("cpus") or 1)
        req_mem = int(req.get("memory") or req.get("mem") or 512)
        fit = [
            node
            for node in candidates
            if node.free_vcpus >= req_cpus and node.free_mem_mib >= req_mem
        ]
        pool = fit or candidates
        return max(
            pool,
            key=lambda node: (
                score_node(node, req, ingress_id=self.node_id, ingress_region=self.region),
                node.free_vcpus,
                -node.inflight,
                node.node_id,
            ),
        ).node_id

    def owner_of(self, sid: str) -> str | None:
        """Return the node currently believed to own a sandbox id."""
        with self._lock:
            self._rebuild_owners_locked()
            return self._owners.get(sid)

    def _consider_owner(self, sid: str, epoch: int, node_id: str) -> bool:
        """Update the authoritative (epoch, owner) for sid; return True iff this claim won."""
        cur = self._owner_epoch.get(sid)
        if cur is None or (epoch, node_id) > cur:
            self._owner_epoch[sid] = (epoch, node_id)
            self._owners[sid] = node_id
            return True
        return False

    def record_owner(self, sid: str, node_id: str, epoch: int) -> None:
        """Record an owner immediately after a proxied creator returns."""
        with self._lock:
            won = self._consider_owner(sid, epoch, node_id)
            if won:
                self._explicit_owners[sid] = (node_id, time.time())
                if node_id == self.node_id:
                    self._local_epoch[sid] = epoch
                    self._persist_local_epoch(sid, epoch)

    def forget_owner(self, sid: str) -> None:
        """Forget an owner mapping after termination or deletion."""
        with self._lock:
            self._explicit_owners.pop(sid, None)
            self._owners.pop(sid, None)
            self._owner_epoch.pop(sid, None)
            self._local_epoch.pop(sid, None)

    def forget_local_owner(self, sid: str) -> None:
        """Drop only this node's local epoch, preserving remote authority after fencing."""
        with self._lock:
            self._local_epoch.pop(sid, None)

    def _persist_local_epoch(self, sid: str, epoch: int) -> None:
        """Best-effort persist of this node's ownership epoch for restart durability."""
        setter = getattr(self.engine, "set_owner_epoch", None)
        if callable(setter):
            with contextlib.suppress(Exception):
                setter(sid, epoch)

    def _loaded_owned_epoch(self, sid: str) -> int:
        """Read the persisted ownership epoch for a locally-owned sid (0 if unknown)."""
        getter = getattr(self.engine, "owned_epoch", None)
        if not callable(getter):
            return 0
        try:
            return int(getter(sid))
        except Exception:
            return 0

    def migration_target(self, node_id: str) -> NodeState:
        """Validate and return a healthy, restore-compatible peer for a migration.

        The target restores the source's snapshot, so it must be a *different* node
        of the same backend + arch whose CPU covers this node's snapshot baseline
        (snapshots bake the originating CPUID/XSAVE surface).
        """
        with self._lock:
            if not node_id or node_id == self.node_id:
                raise MeshError("a sandbox cannot migrate to its current node", code="invalid")
            peer = self._peers.get(node_id)
            if peer is None or not peer.healthy(time.time(), self.interval):
                raise MeshError(
                    f"target node {node_id!r} is not a healthy mesh peer", code="unreachable"
                )
            if peer.arch != self.arch or peer.backend != self.backend:
                raise MeshError(
                    "target node has an incompatible hypervisor or architecture", code="invalid"
                )
            if not cpu_baseline_covers(peer.cpu_baseline, self.cpu_baseline):
                raise MeshError(
                    "target node CPU cannot restore this node's snapshots", code="invalid"
                )
            return peer

    def broadcast_owner(self, sid: str, node_id: str, epoch: int) -> None:
        """Record a sandbox's new owner locally and push it to every peer.

        Speeds cluster-wide convergence after a migration so requests route to the
        new owner without waiting for the next gossip round; ``_scatter_locate`` is
        the backstop for any peer that misses the push.
        """
        self.record_owner(sid, node_id, epoch)
        with self._lock:
            peers = list(self._peers.values())
        payload = {"sid": sid, "node_id": node_id, "epoch": epoch}
        for peer in peers:
            with contextlib.suppress(Exception):
                self.transport.post(peer.advertise, "/v1/mesh/owner", payload)

    def next_epoch(self, sid: str) -> int:
        """Return the next epoch that can supersede the current authoritative owner."""
        with self._lock:
            return self._owner_epoch.get(sid, (0, ""))[0] + 1

    def authoritative_owner(self, sid: str) -> tuple[str | None, int]:
        """Return the authoritative ``(owner, epoch)`` for *sid*, or ``(None, -1)``."""
        with self._lock:
            cur = self._owner_epoch.get(sid)
            if cur is None:
                return None, -1
            epoch, node = cur
            return node, epoch

    def local_epoch(self, sid: str) -> int:
        """Return the epoch at which THIS node owns *sid* (0 if not locally owned)."""
        with self._lock:
            return self._local_epoch.get(sid, 0)

    def fenced_local_ids(self, local_ids: list[str]) -> list[str]:
        """Return local sandbox ids whose authoritative owner is another node."""
        with self._lock:
            fenced: list[str] = []
            for sid in local_ids:
                cur = self._owner_epoch.get(sid)
                if cur is not None and cur[1] != self.node_id:
                    fenced.append(sid)
            return fenced

    def peer_url(self, node_id: str) -> str | None:
        """Return the advertised URL for a node id."""
        with self._lock:
            if node_id == self.node_id:
                return self.advertise
            peer = self._peers.get(node_id)
            return peer.advertise if peer is not None else None

    def find_template_provider(self, key: str) -> tuple[str, str] | None:
        """Return ``(advertise_url, digest)`` for a compatible peer advertising *key*.

        A pulled snapshot is restored locally, so the provider must be the same
        backend + arch and this node's CPU must cover the provider's baseline
        (snapshots bake the originating CPUID/XSAVE) — the same compat class
        ``candidates()`` enforces for ref-bound restores, in the pull direction.
        """
        now = time.time()
        with self._lock:
            for peer in self._peers.values():
                if peer.node_id == self.node_id or not peer.healthy(now, self.interval):
                    continue
                if peer.arch != self.arch or peer.backend != self.backend:
                    continue
                if not cpu_baseline_covers(self.cpu_baseline, peer.cpu_baseline):
                    continue
                digest = peer.template_index.get(key)
                if digest:
                    return peer.advertise, digest
        return None

    def note_inflight(self, delta: int) -> None:
        """Adjust the local placement in-flight counter."""
        with self._lock:
            self._inflight = max(0, self._inflight + delta)

    def coordinator_for(self, key: str) -> str:
        """Return the node id that owns idempotency decisions for *key*.

        Rendezvous (highest-random-weight) hashing over the live membership picks
        one coordinator per key with no shared state, so independent ingress
        nodes converge a retried key onto the same coordinator as long as their
        member views agree. Membership churn can move the choice; see the
        idempotency durability note on :meth:`idem_pin`.
        """
        now = time.time()
        with self._lock:
            members = [self.node_id]
            members.extend(
                peer.node_id for peer in self._peers.values() if peer.healthy(now, self.interval)
            )
        return max(members, key=lambda nid: _hrw_score(key, nid))

    def replica_targets(self, sid: str, k: int) -> list[str]:
        """Up to *k* live peer node ids for *sid*, highest rendezvous weight first."""
        if k <= 0:
            return []
        with self._lock:
            now = time.time()
            targets = [
                peer.node_id
                for peer in self._peers.values()
                if peer.node_id != self.node_id and peer.healthy(now, self.interval)
            ]
            targets.sort(key=lambda node_id: _hrw_score(sid, node_id), reverse=True)
            return targets[:k]

    def restore_owner(self, sid: str, *, exclude: set[str]) -> str | None:
        """Return the deterministically-elected live restorer for *sid*, excluding dead nodes."""
        with self._lock:
            now = time.time()
            members = [self.node_id]
            members.extend(
                peer.node_id for peer in self._peers.values() if peer.healthy(now, self.interval)
            )
            members = [node_id for node_id in members if node_id not in exclude]
            if not members:
                return None
            return max(members, key=lambda node_id: _hrw_score(sid, node_id))

    def drain_orphans(self) -> list[tuple[str, str]]:
        """Return and clear ``(sid, dead_node)`` pairs orphaned by reaped peers."""
        with self._lock:
            out = list(self._orphans)
            self._orphans.clear()
            return out

    def idem_pin(self, key: str, owner: str) -> str:
        """Pin *key* to a workload *owner*, returning the effective (possibly prior) owner.

        First-writer-wins under the lock: concurrent creates of one key all
        observe the same owner, so they cannot scatter onto different nodes. The
        pin is in-memory and transient — it survives owner restart (the committed
        record is rebuilt from VM metadata) but not coordinator restart, which
        degrades that key to at-least-once until its next commit.
        """
        with self._lock:
            self._prune_idem_locked()
            existing = self._idem_pins.get(key)
            if existing is not None:
                return existing[0]
            self._idem_pins[key] = (owner, time.time())
            return owner

    def idem_owner(self, key: str) -> str | None:
        """Return the owner currently pinned for *key*, if any."""
        with self._lock:
            self._prune_idem_locked()
            pin = self._idem_pins.get(key)
            return pin[0] if pin is not None else None

    def idem_repin(self, key: str, owner: str) -> None:
        """Move *key*'s pin to *owner* after the prior owner failed before committing."""
        with self._lock:
            self._idem_pins[key] = (owner, time.time())

    def idem_unpin(self, key: str) -> None:
        """Drop *key*'s pin so a fresh create may re-place it."""
        with self._lock:
            self._idem_pins.pop(key, None)

    def worker_begin(self, key: str) -> bool:
        """Claim local creation of *key*; ``False`` if a create is already in flight here."""
        with self._lock:
            if key in self._idem_busy:
                return False
            self._idem_busy.add(key)
            return True

    def worker_end(self, key: str) -> None:
        """Release the local creation claim for *key*."""
        with self._lock:
            self._idem_busy.discard(key)

    def _prune_idem_locked(self) -> None:
        now = time.time()
        stale = [k for k, (_, ts) in self._idem_pins.items() if now - ts > self.idem_ttl]
        for key in stale:
            del self._idem_pins[key]

    def heartbeat_once(self) -> None:
        """Run one gossip round against every known peer."""
        with self._lock:
            peers = list(self._peers.values())
        for peer in peers:
            payload = {
                "state": self.self_state().to_wire(),
                "known": [node.to_wire() for node in [self.self_state(), *self.peers()]],
            }
            try:
                response = self.transport.post(peer.advertise, "/v1/mesh/heartbeat", payload)
            except Exception:
                continue
            self._merge_heartbeat_response(response)
        self._reap_stale_peers()

    def run_heartbeat(self, stop: threading.Event) -> None:
        """Run gossip until the supplied stop event is set."""
        while not stop.is_set():
            self.heartbeat_once()
            stop.wait(self.interval)

    def load(self) -> None:
        """Load persisted mesh membership without reading any token from disk."""
        if not self.state_path.exists():
            return
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
        except OSError, json.JSONDecodeError:
            return
        node_id = data.get("node_id")
        if not isinstance(node_id, str) or not node_id or data.get("enabled") is not True:
            return
        with self._lock:
            self.node_id = node_id
            self.advertise = str(data.get("advertise") or self.advertise)
            self.region = str(data.get("region") or "")
            self.caps = NodeCaps(
                int(data.get("vcpus") or self.caps.vcpus),
                int(data.get("mem_mib") or self.caps.mem_mib),
            )
            self._peers = {}
            now = time.time()
            for item in data.get("peers") or []:
                node = NodeState.from_wire(item)
                if node.node_id and node.node_id != self.node_id:
                    node.last_seen = node.last_seen or now
                    self._peers[node.node_id] = node
            self.enabled = True
            self._expected_members = max(1, int(data.get("expected_members") or 1))
            self._rebuild_owners_locked()

    def save(self) -> None:
        """Persist membership state atomically without writing the cluster token."""
        if not self.enabled:
            with contextlib.suppress(OSError):
                self.state_path.unlink()
            return
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "enabled": True,
            "node_id": self.node_id,
            "advertise": self.advertise,
            "region": self.region,
            "vcpus": self.caps.vcpus,
            "mem_mib": self.caps.mem_mib,
            "peers": [peer.to_wire() for peer in self._peers.values()],
            "expected_members": self._expected_members,
        }
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, sort_keys=True), encoding="utf-8")
        tmp.replace(self.state_path)

    def _merge_heartbeat_response(self, response: dict[str, Any]) -> None:
        now = time.time()
        with self._lock:
            state_raw = response.get("state")
            if isinstance(state_raw, dict):
                node = NodeState.from_wire(state_raw)
                if node.node_id and node.node_id != self.node_id:
                    node.last_seen = now
                    self._peers[node.node_id] = node
                    for sid, ep in node.owned_epochs.items():
                        self._consider_owner(sid, ep, node.node_id)
            for item in response.get("known") or []:
                node = NodeState.from_wire(item)
                if not node.node_id or node.node_id == self.node_id:
                    continue
                if node.node_id not in self._peers:
                    node.last_seen = now if node.last_seen <= 0.0 else node.last_seen
                    self._peers[node.node_id] = node
                for sid, ep in node.owned_epochs.items():
                    self._consider_owner(sid, ep, node.node_id)
            self._rebuild_owners_locked()
            self._bump_expected()

    def _reap_stale_peers(self) -> None:
        now = time.time()
        with self._lock:
            stale = [
                node_id
                for node_id, peer in self._peers.items()
                if peer.last_seen > 0.0 and now - peer.last_seen > self.reap_sec
            ]
            if stale:
                queued = set(self._orphans)
                for node_id in stale:
                    for sid, owner in self._owners.items():
                        orphan = (sid, node_id)
                        if owner == node_id and orphan not in queued:
                            self._orphans.append(orphan)
                            queued.add(orphan)
            for node_id in stale:
                self._peers.pop(node_id, None)
            if stale:
                self._explicit_owners = {
                    sid: entry
                    for sid, entry in self._explicit_owners.items()
                    if entry[0] not in stale
                }
            self._rebuild_owners_locked()

    def _rebuild_owners_locked(self) -> None:
        for sid in self.engine.owned_ids():
            if sid not in self._local_epoch:
                self._local_epoch[sid] = self._loaded_owned_epoch(sid)
            self._consider_owner(sid, self._local_epoch[sid], self.node_id)
        for peer in self._peers.values():
            if not peer.healthy(time.time(), self.interval):
                continue
            for sid, ep in peer.owned_epochs.items():
                self._consider_owner(sid, ep, peer.node_id)
        owners: dict[str, str] = {}
        for sid in self.engine.owned_ids():
            owners[sid] = self.node_id
        for peer in self._peers.values():
            for sid in peer.owned:
                owners[sid] = peer.node_id
        now = time.time()
        keep_explicit: dict[str, tuple[str, float]] = {}
        for sid, (node_id, recorded_at) in self._explicit_owners.items():
            if owners.get(sid) == node_id or now - recorded_at < 2.0 * self.interval:
                owners[sid] = node_id
                keep_explicit[sid] = (node_id, recorded_at)
        self._explicit_owners = keep_explicit
        self._owners = owners
        for sid, (_ep, node) in self._owner_epoch.items():
            self._owners[sid] = node
        if self._bump_expected():
            self.save()
