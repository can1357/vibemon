"""Cluster membership, gossip, and placement policy for meshed gateways."""

from __future__ import annotations

import base64
import contextlib
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
        known = {"invalid", "unauthorized", "unreachable", "conflict"}
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
            ts=float(data.get("ts") or 0.0),
            last_seen=float(data.get("last_seen") or 0.0),
        )


class MeshTransport(Protocol):
    """Unary inter-node HTTP transport used by mesh membership and placement."""

    def post(self, base_url: str, path: str, payload: dict[str, Any]) -> dict[str, Any]: ...

    def get(self, base_url: str, path: str) -> dict[str, Any]: ...


class HttpxTransport:
    """Synchronous httpx transport for server-side node-to-node calls."""

    def __init__(self, token: str, timeout: float = 5.0) -> None:
        import httpx

        self._client = httpx.Client(timeout=timeout)
        self._token = token

    def post(self, base_url: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        """POST a JSON payload to a peer and return its JSON object response."""
        return self._request("POST", base_url, path, payload=payload)

    def get(self, base_url: str, path: str) -> dict[str, Any]:
        """GET a JSON object response from a peer."""
        return self._request("GET", base_url, path)

    def _request(
        self, method: str, base_url: str, path: str, *, payload: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        import httpx

        url = base_url.rstrip("/") + path
        headers = {
            "Authorization": f"Bearer {self._token}",
            "X-Vmon-Mesh-Hop": "1",
        }
        try:
            response = self._client.request(method, url, json=payload, headers=headers)
        except httpx.HTTPError as exc:
            raise MeshError("peer unreachable", code="unreachable") from exc
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
        return "unreachable"
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
) -> str:
    """Return the request-derivable identity for a bootable warm template."""
    from .image import _normalize_reference

    normalized = _normalize_reference(ref)
    if normalized is None:
        raise ValueError("template key requires an image reference")
    return (
        f"{normalized}|d{int(disk_mb)}|m{int(memory)}|c{int(cpus)}"
        f"|s{int(fs_slots)}|h{int(host_slot)}|n{int(nic_slot)}"
    )


def _volume_count(value: Any) -> int:
    if isinstance(value, dict):
        return len(value)
    if isinstance(value, list | tuple):
        return len(value)
    return 0


def request_template_key(req: dict[str, Any]) -> str | None:
    """Return the cross-node warm-template key for a request, if pull-warm applies.

    Pull-warm currently supports the plain block-network template only: after a
    pull the create rewrites to ``template=<pulled dir>``, which warm-restores
    without the image. Requests with ``fs_dir``/volumes or networked (NIC)
    flavors still need a local image to rebind/enumerate devices on restore, so
    they are not pull-eligible (they fall back to a local build).
    """
    ref = req.get("image")
    if (
        not ref
        or req.get("template")
        or req.get("dockerfile")
        or req.get("context") not in (None, ".")
        or not req.get("block_network")
        or req.get("fs_dir") is not None
        or _volume_count(req.get("volumes"))
    ):
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
        )
    except (TypeError, ValueError):
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
        self._owners: dict[str, str] = {}
        self._explicit_owners: dict[str, tuple[str, float]] = {}
        self._inflight = 0

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
                owned=self.engine.owned_ids(),
                ts=now,
                last_seen=now,
            )

    def peers(self) -> list[NodeState]:
        """Return the currently known peer snapshots."""
        with self._lock:
            return list(self._peers.values())

    def status(self) -> dict[str, Any]:
        """Return a JSON-ready mesh status snapshot for operators."""
        now = time.time()
        with self._lock:
            self_state = self.self_state().to_wire()
            self_state["healthy"] = True
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

    def record_owner(self, sid: str, node_id: str) -> None:
        """Record an owner immediately after a proxied creator returns."""
        with self._lock:
            self._explicit_owners[sid] = (node_id, time.time())
            self._owners[sid] = node_id

    def forget_owner(self, sid: str) -> None:
        """Forget an owner mapping after termination or deletion."""
        with self._lock:
            self._explicit_owners.pop(sid, None)
            self._owners.pop(sid, None)

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
            for item in response.get("known") or []:
                node = NodeState.from_wire(item)
                if not node.node_id or node.node_id == self.node_id:
                    continue
                if node.node_id not in self._peers:
                    node.last_seen = now if node.last_seen <= 0.0 else node.last_seen
                    self._peers[node.node_id] = node
            self._rebuild_owners_locked()

    def _reap_stale_peers(self) -> None:
        now = time.time()
        with self._lock:
            stale = [
                node_id
                for node_id, peer in self._peers.items()
                if peer.last_seen > 0.0 and now - peer.last_seen > self.reap_sec
            ]
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
