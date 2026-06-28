import asyncio
import json
import time
from pathlib import Path
from urllib.parse import urlsplit

import pytest

pytest.importorskip("fastapi")
from starlette.responses import Response
from starlette.testclient import TestClient

from vmon.mesh import (
    Mesh,
    MeshError,
    NodeCaps,
    NodeState,
    cpu_baseline_covers,
    decode_blob,
    encode_blob,
    pool_eligible,
    template_key,
)


class FakeEngine:
    def __init__(self, *, templates=None, owned=None, committed=(0, 0)):
        self._templates = list(templates or [])
        self._owned = list(owned or [])
        self._committed = committed

    def committed(self):
        return self._committed

    def owned_ids(self):
        return list(self._owned)

    def templates_present(self):
        return list(self._templates)


class FakeTransport:
    def __init__(self):
        self.clients = {}
        self.token = "secret"

    def post(self, base_url, path, payload, *, timeout=None):
        response = self.clients[base_url].post(
            path,
            json=payload,
            headers={"Authorization": f"Bearer {self.token}", "X-Vmon-Mesh-Hop": "1"},
        )
        if response.status_code >= 400:
            raise MeshError(response.text)
        return response.json() if response.content else {}

    def get(self, base_url, path, *, timeout=None):
        response = self.clients[base_url].get(
            path,
            headers={"Authorization": f"Bearer {self.token}", "X-Vmon-Mesh-Hop": "1"},
        )
        if response.status_code >= 400:
            raise MeshError(response.text)
        return response.json() if response.content else {}


class FakeSandbox:
    def __init__(self, name, tags=None):
        self.name = name
        self.tags = dict(tags or {})
        self.returncode = None

    @classmethod
    def create(cls, **kwargs):
        return cls(kwargs.get("name") or "sb", kwargs.get("tags") or {})

    def snapshot_filesystem(self, name=None):
        return name or "snap"

    def terminate(self):
        return None


def _two_node_apps(monkeypatch, tmp_path, server_mod):
    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.setattr("vmon.core.Engine._sandbox_class", staticmethod(lambda: FakeSandbox))
    app_a = server_mod.create_app(token="secret")
    app_b = server_mod.create_app(token="secret")
    transport = FakeTransport()
    app_a.state.mesh.transport = transport
    app_b.state.mesh.transport = transport
    app_a.state.mesh.node_id = "A"
    app_b.state.mesh.node_id = "B"
    app_a.state.mesh.state_path = tmp_path / "a-mesh.json"
    app_b.state.mesh.state_path = tmp_path / "b-mesh.json"
    app_a.state.mesh.setup("http://a")
    app_b.state.mesh.setup("http://b")
    return app_a, app_b, transport


def _write_indexed_template(
    template_dir,
    *,
    image: str = "ubuntu:latest",
    disk_mb: int = 1024,
    memory: int = 512,
    cpus: int = 1,
    fs_slots: int = 0,
    host_slot: bool = False,
    nic_slot: bool = False,
    rootfs: bytes = b"rootfs",
) -> tuple[str, str]:
    from vmon import cas

    template_dir.mkdir(parents=True, exist_ok=True)
    (template_dir / "rootfs.img").write_bytes(rootfs)
    (template_dir / "state.bin").write_bytes(b"state")
    (template_dir / "current-generation").write_text("1", encoding="utf-8")
    (template_dir / "vmstate.1.bin").write_bytes(b"state")
    (template_dir / "memory.1.bin").write_bytes(b"page".ljust(4096, b"\0"))
    marker = {
        "image": image,
        "digest": "image-digest",
        "disk_mb": disk_mb,
        "boot_version": 5,
        "kernel_sha": "kernel",
        "fs_slots": fs_slots,
        "memory": memory,
        "cpus": cpus,
        "host_slot": host_slot,
        "nic_slot": nic_slot,
    }
    marker_path = template_dir / "agent-ready.json"
    marker_path.write_text(json.dumps(marker, sort_keys=True), encoding="utf-8")
    digest = cas.index_template(template_dir)
    marker["content_digest"] = digest
    marker_path.write_text(json.dumps(marker, sort_keys=True), encoding="utf-8")
    cas.index_template(template_dir, digest)
    key = template_key(
        image,
        disk_mb=disk_mb,
        memory=memory,
        cpus=cpus,
        fs_slots=fs_slots,
        host_slot=host_slot,
        nic_slot=nic_slot,
    )
    return digest, key


class MeshHTTPResponse:
    def __init__(self, response):
        self._response = response
        self.status_code = response.status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    async def aiter_bytes(self):
        yield self._response.content


class MeshHTTPStream:
    def __init__(self, response):
        self._response = MeshHTTPResponse(response)

    async def __aenter__(self):
        return self._response

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeMeshHTTP:
    def __init__(self, clients):
        self.clients = clients

    def stream(self, method, url, headers=None):
        parsed = urlsplit(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        path = parsed.path + (f"?{parsed.query}" if parsed.query else "")
        response = self.clients[base].request(method, path, headers=headers)
        return MeshHTTPStream(response)

    async def aclose(self):
        return None


def test_template_key_canonicalizes_shape_and_reference():
    assert (
        template_key(
            " ubuntu:latest ",
            disk_mb=1024,
            memory=2048,
            cpus=2,
            fs_slots=3,
            host_slot=True,
            nic_slot=False,
        )
        == "ubuntu:latest|d1024|m2048|c2|s3|h1|n0|t0"
    )
    # The Linux TAP NIC flavor keys distinctly from the macOS user-net flavor.
    tap = template_key(
        "ubuntu:latest",
        disk_mb=1024,
        memory=512,
        cpus=1,
        fs_slots=0,
        host_slot=False,
        nic_slot=False,
        tap_slot=True,
    )
    assert tap.endswith("|n0|t1")


def test_request_template_key_distinguishes_networked_and_block_network(monkeypatch):
    import vmon.mesh as mesh_mod
    from vmon.mesh import pool_eligible, request_template_key

    # block-network is cross-node pull-warm on any platform (plain template).
    block = request_template_key({"image": "img", "block_network": True})
    assert block is not None and block.endswith("|n0|t0")
    # A networked request is pull-warm only via the Linux TAP flavor...
    monkeypatch.setattr(mesh_mod.platform, "system", lambda: "Linux")
    networked = request_template_key({"image": "img", "block_network": False})
    assert networked is not None and networked.endswith("|n0|t1")
    # ...while macOS user-net networked warm stays single-node (not pull-warm).
    monkeypatch.setattr(mesh_mod.platform, "system", lambda: "Darwin")
    assert request_template_key({"image": "img", "block_network": False}) is None
    # Networked requests are not pool-claim eligible (no per-clone TAP yet), and
    # volume / fs_dir requests still need a local image and are not pull-warm.
    assert pool_eligible({"image": "img", "block_network": False}) is False
    assert request_template_key({"image": "img", "fs_dir": "/x"}) is None
    assert request_template_key({"image": "img", "volumes": {"/a": "v"}}) is None


def test_find_template_provider_skips_incompatible_unhealthy_and_missing(tmp_path):
    key = template_key(
        "ubuntu:latest",
        disk_mb=1024,
        memory=512,
        cpus=1,
        fs_slots=0,
        host_slot=False,
        nic_slot=False,
    )
    digest = "a" * 64
    mesh = Mesh(
        FakeEngine(),
        advertise="http://self",
        token="t",
        transport=FakeTransport(),
        caps=NodeCaps(2, 1024),
        backend="kvm",
        arch="x86_64",
        state_path=tmp_path / "mesh.json",
    )
    mesh.node_id = "self"
    mesh.enabled = True
    mesh.cpu_baseline = "arch:x86_64"
    now = time.time()

    def peer(
        node_id, *, backend="kvm", arch="x86_64", cpu_baseline="arch:x86_64", index=None, ok=True
    ):
        mesh.register(
            NodeState(
                node_id,
                f"http://{node_id}",
                backend=backend,
                arch=arch,
                cpu_baseline=cpu_baseline,
                template_index=index or {},
                last_seen=now,
            )
        )
        if not ok:
            mesh.mark_unhealthy(node_id)

    peer("missing", index={})  # advertises no templates
    peer("stale", index={key: "b" * 64}, ok=False)  # has it but unhealthy
    peer("wrongbackend", backend="hvf", index={key: "c" * 64})  # snapshot backend differs
    peer("wrongarch", arch="aarch64", index={key: "d" * 64})  # kernel/agent arch differs
    peer("provider", index={key: digest})  # compatible

    # Only the healthy, same-backend/arch, CPU-covered peer is a valid provider.
    assert mesh.find_template_provider(key) == ("http://provider", digest)
    assert mesh.find_template_provider("missing-key") is None


def test_score_warm_capacity_pinned_templates_and_overcommit(tmp_path):
    mesh = Mesh(
        FakeEngine(committed=(1, 0)),
        advertise="http://self",
        token="t",
        transport=FakeTransport(),
        caps=NodeCaps(2, 1024),
        backend="kvm",
        arch="x86_64",
        state_path=tmp_path / "mesh.json",
    )
    mesh.node_id = "self"
    mesh.enabled = True
    mesh.cpu_baseline = "arch:x86_64"
    now = time.time()
    warm = NodeState(
        "warm",
        "http://warm",
        caps=NodeCaps(2, 1024),
        backend="kvm",
        arch="x86_64",
        committed_vcpus=1,
        committed_mem_mib=512,
        pools={"img:x": 2},
        last_seen=now,
    )
    free = NodeState(
        "free",
        "http://free",
        backend="kvm",
        arch="x86_64",
        caps=NodeCaps(8, 4096),
        pools={},
        last_seen=now,
    )
    mesh.register(warm)
    mesh.register(free)

    assert mesh.place({"image": "img:x", "pool_size": 1, "block_network": True}) == "warm"
    assert mesh.place({"image": "img:x"}) == "free"
    assert mesh.pinned_local({"dockerfile": "Dockerfile"}) is True
    assert mesh.place({"dockerfile": "Dockerfile"}) == "self"

    templated = NodeState(
        "templated",
        "http://templated",
        backend="kvm",
        arch="x86_64",
        templates=["snap1"],
        caps=NodeCaps(1, 512),
        last_seen=now,
    )
    mesh.register(templated)
    assert mesh.place({"snapshot": "snap1", "cpus": 8, "memory": 8192}) == "templated"

    mesh.mark_unhealthy("free")
    assert mesh.place({"image": "plain"}) != "free"


def test_zero_config_warm_named_claims_and_backend_filter(tmp_path):
    mesh = Mesh(
        FakeEngine(committed=(0, 0)),
        advertise="http://self",
        token="t",
        transport=FakeTransport(),
        caps=NodeCaps(8, 8192),
        backend="kvm",
        arch="x86_64",
        state_path=tmp_path / "mesh.json",
    )
    mesh.node_id = "self"
    mesh.enabled = True
    mesh.cpu_baseline = "arch:x86_64"
    now = time.time()
    warm = NodeState(
        "warm",
        "http://warm",
        caps=NodeCaps(2, 1024),
        backend="kvm",
        arch="x86_64",
        pools={"img:x": 2},
        templates=["snap1"],
        last_seen=now,
    )
    mesh.register(warm)

    # Zero-config: a block_network image request with no pool_size still prefers a
    # node holding a ready warm pool, and a caller-named request claims one too.
    assert mesh.place({"image": "img:x", "block_network": True}) == "warm"
    assert mesh.place({"image": "img:x", "block_network": True, "name": "foo"}) == "warm"

    # Eligibility: block_network is required; volumes/fs_dir need restore-rebind a
    # pooled clone cannot serve, so they stay ineligible.
    assert pool_eligible({"image": "img:x", "block_network": True}) is True
    assert pool_eligible({"image": "img:x", "block_network": True, "name": "foo"}) is True
    assert pool_eligible({"image": "img:x"}) is False
    assert pool_eligible({"image": "img:x", "block_network": True, "volumes": {"/d": "v"}}) is False
    assert pool_eligible({"image": "img:x", "block_network": True, "fs_dir": "/tmp"}) is False

    # Backend filter is for snapshot-class requests only: a zero-config image can
    # land on an opposite-backend node (it builds its own template), but a named
    # snapshot restore must match the backend that baked the memory image.
    hvf = NodeState(
        "hvf",
        "http://hvf",
        caps=NodeCaps(16, 16384),
        backend="hvf",
        arch="x86_64",
        templates=["snap1"],
        last_seen=now,
    )
    mesh.register(hvf)
    cold = {n.node_id for n in mesh.candidates({"image": "plain", "block_network": True})}
    assert "hvf" in cold
    snap = {n.node_id for n in mesh.candidates({"snapshot": "snap1"})}
    assert snap == {"warm"}


def test_join_blob_round_trip_and_invalid():
    assert decode_blob(encode_blob("http://node", "tok")) == ("http://node", "tok")
    with pytest.raises(MeshError):
        decode_blob("not-json")


def test_owner_index_record_and_reap(tmp_path):
    mesh = Mesh(
        FakeEngine(owned=["local"]),
        advertise="http://self",
        token="t",
        transport=FakeTransport(),
        state_path=tmp_path / "mesh.json",
    )
    mesh.node_id = "self"
    mesh.enabled = True
    peer = NodeState("peer", "http://peer", owned=["remote"], last_seen=time.time())
    mesh.register(peer)
    mesh.record_owner("new", "peer")

    assert mesh.owner_of("local") == "self"
    assert mesh.owner_of("remote") == "peer"
    assert mesh.owner_of("new") == "peer"

    mesh.reap_sec = 0.01
    mesh._peers["peer"].last_seen = time.time() - 1
    mesh.heartbeat_once()
    assert mesh.owner_of("remote") is None


def test_two_node_create_proxy_list_and_per_sandbox_proxy(monkeypatch, tmp_path):
    import vmon.server as server_mod

    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.setattr("vmon.core.Engine._sandbox_class", staticmethod(lambda: FakeSandbox))
    app_a = server_mod.create_app(token="secret")
    app_b = server_mod.create_app(token="secret")
    transport = FakeTransport()
    app_a.state.mesh.transport = transport
    app_b.state.mesh.transport = transport
    app_a.state.mesh.node_id = "A"
    app_b.state.mesh.node_id = "B"
    app_a.state.mesh.state_path = tmp_path / "a-mesh.json"
    app_b.state.mesh.state_path = tmp_path / "b-mesh.json"
    app_a.state.mesh.setup("http://a")
    app_b.state.mesh.setup("http://b")

    async def fake_proxy(request, peer_url, token):
        body = await request.body()
        path = request.url.path + (f"?{request.url.query}" if request.url.query else "")
        response = transport.clients[peer_url].request(
            request.method,
            path,
            content=body,
            headers={
                "Authorization": f"Bearer {token}",
                "X-Vmon-Mesh-Hop": "1",
                "Content-Type": request.headers.get("content-type", "application/json"),
            },
        )
        return Response(response.content, status_code=response.status_code)

    monkeypatch.setattr(server_mod, "_proxy_to_peer", fake_proxy)
    with TestClient(app_a) as client_a, TestClient(app_b) as client_b:
        transport.clients = {"http://a": client_a, "http://b": client_b}
        state_b = app_b.state.mesh.self_state()
        state_b.pools = {"img:x": 2}
        state_b.last_seen = time.time()
        app_a.state.mesh.register(state_b)

        created = client_a.post(
            "/v1/sandboxes",
            json={"image": "img:x", "block_network": True, "pool_size": 1, "name": None},
            headers={"Authorization": "Bearer secret"},
        )
        assert created.status_code == 201
        sid = created.json()["id"]
        assert app_a.state.mesh.owner_of(sid) == "B"
        assert sid not in app_a.state.supervisor._engine._records
        assert sid in app_b.state.supervisor._engine._records

        got = client_a.get(f"/v1/sandboxes/{sid}", headers={"Authorization": "Bearer secret"})
        assert got.status_code == 200
        snap = client_a.post(
            f"/v1/sandboxes/{sid}/snapshot",
            json={"name": "snap"},
            headers={"Authorization": "Bearer secret"},
        )
        assert snap.json() == {"image": "snap"}
        listed = client_a.get("/v1/sandboxes", headers={"Authorization": "Bearer secret"})
        rows = listed.json()["sandboxes"]
        assert any(row["id"] == sid and row["node"] == "B" for row in rows)


def test_idempotent_sandbox_create_replays_same_key(monkeypatch, tmp_path):
    import vmon.server as server_mod

    app_a, app_b, transport = _two_node_apps(monkeypatch, tmp_path, server_mod)
    with TestClient(app_a) as client_a, TestClient(app_b) as client_b:
        transport.clients = {"http://a": client_a, "http://b": client_b}
        state_b = app_b.state.mesh.self_state()
        state_b.pools = {"img:x": 2}
        state_b.last_seen = time.time()
        app_a.state.mesh.register(state_b)

        payload = {
            "image": "img:x",
            "block_network": True,
            "pool_size": 1,
            "idempotency_key": "create-key",
        }
        first = client_a.post(
            "/v1/sandboxes", json=payload, headers={"Authorization": "Bearer secret"}
        )
        second = client_a.post(
            "/v1/sandboxes", json=payload, headers={"Authorization": "Bearer secret"}
        )

        assert first.status_code == 201
        assert second.status_code == 201
        assert second.json()["id"] == first.json()["id"]
        assert (
            len(app_a.state.supervisor._engine._records)
            + len(app_b.state.supervisor._engine._records)
            == 1
        )


def test_idempotent_sandbox_create_does_not_reroute_ambiguous_commit(monkeypatch, tmp_path):
    import vmon.server as server_mod

    class AmbiguousAfterCommitTransport(FakeTransport):
        def __init__(self):
            super().__init__()
            self.raised = False

        def post(self, base_url, path, payload, *, timeout=None):
            if (
                base_url == "http://b"
                and path == "/v1/mesh/idem/sandboxes/work"
                and not self.raised
            ):
                response = self.clients[base_url].post(
                    path,
                    json=payload,
                    headers={
                        "Authorization": f"Bearer {self.token}",
                        "X-Vmon-Mesh-Hop": "1",
                    },
                )
                assert response.status_code == 200
                self.raised = True
                raise MeshError("peer response ambiguous", code="ambiguous")
            return super().post(base_url, path, payload, timeout=timeout)

    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.setattr("vmon.core.Engine._sandbox_class", staticmethod(lambda: FakeSandbox))
    app_a = server_mod.create_app(token="secret")
    app_b = server_mod.create_app(token="secret")
    transport = AmbiguousAfterCommitTransport()
    app_a.state.mesh.transport = transport
    app_b.state.mesh.transport = transport
    app_a.state.mesh.node_id = "A"
    app_b.state.mesh.node_id = "B"
    app_a.state.mesh.state_path = tmp_path / "a-mesh.json"
    app_b.state.mesh.state_path = tmp_path / "b-mesh.json"
    app_a.state.mesh.setup("http://a")
    app_b.state.mesh.setup("http://b")

    with TestClient(app_a) as client_a, TestClient(app_b) as client_b:
        transport.clients = {"http://a": client_a, "http://b": client_b}
        state_b = app_b.state.mesh.self_state()
        state_b.pools = {"img:x": 2}
        state_b.last_seen = time.time()
        app_a.state.mesh.register(state_b)
        key = next(
            f"ambiguous-key-{idx}"
            for idx in range(64)
            if app_a.state.mesh.coordinator_for(f"ambiguous-key-{idx}") == "A"
        )

        payload = {
            "image": "img:x",
            "block_network": True,
            "pool_size": 1,
            "idempotency_key": key,
        }
        first = client_a.post(
            "/v1/sandboxes", json=payload, headers={"Authorization": "Bearer secret"}
        )
        assert first.status_code == 504
        assert len(app_b.state.supervisor._engine._records) == 1
        assert len(app_a.state.supervisor._engine._records) == 0

        second = client_a.post(
            "/v1/sandboxes", json=payload, headers={"Authorization": "Bearer secret"}
        )
        assert second.status_code == 201
        assert len(app_b.state.supervisor._engine._records) == 1


def test_idempotent_sandbox_create_rejects_same_name_different_key(monkeypatch, tmp_path):
    import vmon.server as server_mod

    app_a, app_b, transport = _two_node_apps(monkeypatch, tmp_path, server_mod)
    with TestClient(app_a) as client_a, TestClient(app_b) as client_b:
        transport.clients = {"http://a": client_a, "http://b": client_b}
        created = client_a.post(
            "/v1/sandboxes",
            json={"image": "img:x", "name": "fixed", "idempotency_key": "one"},
            headers={"Authorization": "Bearer secret"},
        )
        conflict = client_a.post(
            "/v1/sandboxes",
            json={"image": "img:x", "name": "fixed", "idempotency_key": "two"},
            headers={"Authorization": "Bearer secret"},
        )

        assert created.status_code == 201
        assert conflict.status_code == 409


def test_engine_rehydrates_idempotency_index(monkeypatch, tmp_path):
    import vmon.vmm as vmm_mod
    from vmon.core import Engine

    monkeypatch.setattr(vmm_mod, "STATE", tmp_path)
    vm_dir = tmp_path / "vms" / "replayed"
    vm_dir.mkdir(parents=True)
    (vm_dir / "meta.json").write_text(
        json.dumps(
            {
                "idempotency_key": "rehydrate-key",
                "image": "img:x",
                "status": "stopped",
                "timeout_secs": 300,
            }
        ),
        encoding="utf-8",
    )

    engine = Engine()

    record = engine.find_by_idempotency_key("rehydrate-key")
    assert record is not None
    assert record.id == "replayed"
    assert "idempotency_key" not in record.view()


def test_two_node_create_pulls_peer_template_by_digest(monkeypatch, tmp_path):
    import vmon.server as server_mod

    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    captured: dict[str, object] = {}

    class RecordingSandbox(FakeSandbox):
        @classmethod
        def create(cls, **kwargs):
            captured.update(kwargs)
            return super().create(**kwargs)

    monkeypatch.setattr("vmon.core.Engine._sandbox_class", staticmethod(lambda: RecordingSandbox))
    digest, key = _write_indexed_template(tmp_path / "peer-template", image="ubuntu:latest")
    app_a = server_mod.create_app(token="secret")
    app_b = server_mod.create_app(token="secret")
    transport = FakeTransport()
    app_a.state.mesh.transport = transport
    app_b.state.mesh.transport = transport
    app_a.state.mesh.node_id = "A"
    app_b.state.mesh.node_id = "B"
    app_a.state.mesh.cpu_baseline = "arch:x86_64"
    app_a.state.mesh.state_path = tmp_path / "a-mesh.json"
    app_b.state.mesh.state_path = tmp_path / "b-mesh.json"
    app_a.state.mesh.setup("http://a")
    app_b.state.mesh.setup("http://b")

    with TestClient(app_a) as client_a, TestClient(app_b) as client_b:
        transport.clients = {"http://a": client_a, "http://b": client_b}
        app_a.state.mesh_http = FakeMeshHTTP({"http://b": client_b})
        state_b = app_b.state.mesh.self_state()
        state_b.template_index = {key: digest}
        state_b.cpu_baseline = "arch:x86_64"
        state_b.last_seen = time.time()
        app_a.state.mesh.register(state_b)

        # The hop header makes A the local owner (as if placement already chose it),
        # so A's create must install a metadata-only lazy template stub and
        # warm-restore it with a remote page source instead of building locally.
        created = client_a.post(
            "/v1/sandboxes",
            json={"image": "ubuntu:latest", "block_network": True},
            headers={"Authorization": "Bearer secret", "X-Vmon-Mesh-Hop": "1"},
        )
        assert created.status_code == 201
        installed = Path(str(captured.get("template")))
        assert installed.parent == tmp_path / "templates"
        assert installed.name.startswith("remote-")
        assert (installed / "remote-page.json").is_file()
        assert not (installed / "memory.1.bin").exists()
        assert key not in app_a.state.supervisor._engine.template_index()
        assert captured.get("remote_page_url") == f"http://b/v1/templates/{digest}/pages"
        assert captured.get("remote_page_token") == "secret"
        assert captured.get("remote_page_digest") == digest


def test_pull_template_rejects_digest_mismatch(monkeypatch, tmp_path):
    import vmon.server as server_mod
    from vmon import cas

    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    _digest, _key = _write_indexed_template(tmp_path / "peer-template", image="ubuntu:latest")
    archive = server_mod._template_archive(tmp_path / "peer-template")
    body = archive.read_bytes()
    archive.unlink()
    requested = "0" * 64
    if requested == cas.template_digest(tmp_path / "peer-template"):
        requested = "1" * 64

    class StaticMeshHTTP:
        def stream(self, method, url, headers=None):
            response = type("Response", (), {"status_code": 200, "content": body})()
            return MeshHTTPStream(response)

    with pytest.raises(ValueError):
        asyncio.run(server_mod.pull_template(StaticMeshHTTP(), "http://peer", requested, "secret"))

    assert cas.resolve_digest(requested) is None
    templates = tmp_path / "templates"
    assert not templates.exists() or not list(templates.iterdir())


def test_cli_client_selection(monkeypatch, tmp_path):
    from vmon import cli
    from vmon.client import DaemonClient, GatewayClient

    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.delenv("VMON_REMOTE", raising=False)
    monkeypatch.delenv("VMON_SERVER", raising=False)
    monkeypatch.setenv("VMON_API_TOKEN", "secret")
    assert isinstance(cli._client(), DaemonClient)

    monkeypatch.setenv("VMON_SERVER", "http://gw")
    assert isinstance(cli._client(), GatewayClient)
    monkeypatch.delenv("VMON_SERVER", raising=False)
    (tmp_path / "mesh.json").write_text(
        json.dumps({"enabled": True, "advertise": "http://mesh"}), encoding="utf-8"
    )
    assert isinstance(cli._client(), GatewayClient)


class RecordingGatewayClient:
    def __init__(self):
        self.calls = []

    _q = staticmethod(lambda value: str(value))

    @staticmethod
    def _urlencode(values):
        import urllib.parse

        return urllib.parse.urlencode(values)

    def _json(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        if path == "/v1/sandboxes":
            return {"sandboxes": [{"id": "sb", "status": "running", "pid": 1, "source": "img"}]}
        if path == "/v1/fork":
            return {"clones": []}
        return {"snapshot": "snap", "dir": "/tmp/snap"}


def test_gateway_call_mapping():
    from vmon.client import GatewayClient

    client = RecordingGatewayClient()
    assert GatewayClient.call(client, "ps")["vms"][0]["name"] == "sb"
    GatewayClient.call(client, "rm", name="sb")
    GatewayClient.call(client, "snapshot", name="sb", snapshot="snap", stop=True)
    assert ("DELETE", "/v1/sandboxes/sb/remove", None) in client.calls
    assert (
        "POST",
        "/v1/sandboxes/sb/snapshot_template",
        {"snapshot": "snap", "stop": True},
    ) in client.calls
    GatewayClient.call(client, "fs_list", name="sb", path="/etc")
    GatewayClient.call(client, "fs_stat", name="sb", path="/etc/hosts")
    assert ("GET", "/v1/sandboxes/sb/fs/list?path=%2Fetc", None) in client.calls
    assert ("GET", "/v1/sandboxes/sb/fs/stat?path=%2Fetc%2Fhosts", None) in client.calls


def test_node_state_wire_tolerates_missing_backend_arch_cpu_baseline():
    # Old peers / persisted blobs predate backend+arch+cpu_baseline; they must
    # still load.
    old = NodeState.from_wire({"node_id": "old", "advertise": "http://old"})
    assert (old.backend, old.arch, old.cpu_baseline) == ("", "", "")
    rt = NodeState.from_wire(
        NodeState("n", "u", backend="kvm", arch="x86_64", cpu_baseline="base").to_wire()
    )
    assert (rt.backend, rt.arch, rt.cpu_baseline) == ("kvm", "x86_64", "base")


def test_cpu_baseline_covers_feature_surfaces_and_arch_tokens():
    need = json.dumps({"1.0.ecx": 0b0011, "D.0.eax": 0b0101, "v": 1}, sort_keys=True)
    have = json.dumps({"1.0.ecx": 0b1011, "D.0.eax": 0b0111, "v": 1}, sort_keys=True)
    missing_bit = json.dumps({"1.0.ecx": 0b0001, "D.0.eax": 0b0111, "v": 1}, sort_keys=True)
    missing_key = json.dumps({"1.0.ecx": 0b1011, "v": 1}, sort_keys=True)

    assert cpu_baseline_covers(have, need)
    assert not cpu_baseline_covers(missing_bit, need)
    assert not cpu_baseline_covers(missing_key, need)
    assert cpu_baseline_covers("arch:aarch64", "arch:aarch64")
    assert not cpu_baseline_covers("arch:x86_64", "arch:aarch64")
    # A missing or arch-token baseline cannot prove a real x86 surface, so a
    # stale/pre-upgrade peer never bypasses the ref-bound x86 restore gate.
    assert not cpu_baseline_covers("", need)
    assert not cpu_baseline_covers("arch:x86_64", need)
    # An empty or arch-token need imposes no fine-grained constraint.
    assert cpu_baseline_covers(have, "")
    assert cpu_baseline_covers("arch:x86_64", "arch:x86_64")
    # A failed x86 probe yields an explicit unknown token: it never proves a peer
    # surface, and as an ingress need it strands ref-bound restores to local.
    assert not cpu_baseline_covers("unknown:x86_64", need)
    assert not cpu_baseline_covers(have, "unknown:x86_64")


def test_cpu_baseline_filters_ref_candidates_only(tmp_path):
    need = json.dumps({"1.0.ecx": 0b0011, "xcr0": 0b0101, "v": 1}, sort_keys=True)
    have = json.dumps({"1.0.ecx": 0b1011, "xcr0": 0b0111, "v": 1}, sort_keys=True)
    lacks_bit = json.dumps({"1.0.ecx": 0b0001, "xcr0": 0b0111, "v": 1}, sort_keys=True)
    mesh = Mesh(
        FakeEngine(),
        advertise="http://self",
        token="t",
        transport=FakeTransport(),
        backend="kvm",
        arch="x86_64",
        caps=NodeCaps(2, 1024),
        state_path=tmp_path / "mesh.json",
    )
    mesh.node_id = "self"
    mesh.enabled = True
    mesh.cpu_baseline = need
    now = time.time()
    compatible = NodeState(
        "compatible",
        "http://c",
        backend="kvm",
        arch="x86_64",
        cpu_baseline=have,
        caps=NodeCaps(8, 4096),
        templates=["snap1"],
        last_seen=now,
    )
    incompatible = NodeState(
        "incompatible",
        "http://i",
        backend="kvm",
        arch="x86_64",
        cpu_baseline=lacks_bit,
        caps=NodeCaps(8, 4096),
        templates=["snap1"],
        last_seen=now,
    )
    for peer in (compatible, incompatible):
        mesh.register(peer)

    assert {n.node_id for n in mesh.candidates({"snapshot": "snap1"})} == {"compatible"}
    cold = {n.node_id for n in mesh.candidates({"image": "plain", "block_network": True})}
    assert {"compatible", "incompatible"} <= cold


def test_unknown_cpu_baseline_strands_ref_restores_local(tmp_path):
    mesh = Mesh(
        FakeEngine(templates=["snap1"]),
        advertise="http://self",
        token="t",
        transport=FakeTransport(),
        backend="kvm",
        arch="x86_64",
        caps=NodeCaps(2, 1024),
        state_path=tmp_path / "mesh.json",
    )
    mesh.node_id = "self"
    mesh.enabled = True
    mesh.cpu_baseline = "unknown:x86_64"  # local CPU probe failed
    now = time.time()
    peer = NodeState(
        "peer",
        "http://p",
        backend="kvm",
        arch="x86_64",
        cpu_baseline=json.dumps({"1.0.ecx": 0b1111, "v": 1}, sort_keys=True),
        caps=NodeCaps(8, 4096),
        templates=["snap1"],
        last_seen=now,
    )
    mesh.register(peer)
    # An ingress that cannot prove its own CPU surface never gambles a ref-bound
    # restore onto a peer; placement strands to the local node instead.
    assert mesh.candidates({"snapshot": "snap1"}) == []
    assert mesh.place({"snapshot": "snap1"}) == "self"
    # Cold image placement is unaffected by the unknown baseline.
    assert "peer" in {n.node_id for n in mesh.candidates({"image": "x", "block_network": True})}


def test_placement_is_backend_and_arch_compatible(tmp_path):
    mesh = Mesh(
        FakeEngine(),
        advertise="http://self",
        token="t",
        transport=FakeTransport(),
        backend="kvm",
        arch="x86_64",
        caps=NodeCaps(2, 1024),
        state_path=tmp_path / "mesh.json",
    )
    mesh.node_id = "self"
    mesh.enabled = True
    mesh.cpu_baseline = "arch:x86_64"
    now = time.time()
    wrong_backend = NodeState(
        "hvfnode",
        "http://h",
        backend="hvf",
        arch="x86_64",
        caps=NodeCaps(8, 4096),
        pools={"img:x": 4},
        templates=["snap1"],
        last_seen=now,
    )
    wrong_arch = NodeState(
        "armnode",
        "http://a",
        backend="kvm",
        arch="aarch64",
        caps=NodeCaps(8, 4096),
        pools={"img:x": 4},
        templates=["snap1"],
        last_seen=now,
    )
    compatible = NodeState(
        "kvmnode",
        "http://k",
        backend="kvm",
        arch="x86_64",
        caps=NodeCaps(8, 4096),
        pools={"img:x": 4},
        templates=["snap1"],
        last_seen=now,
    )
    for peer in (wrong_backend, wrong_arch, compatible):
        mesh.register(peer)

    # Warm-pool image request: arch must match, but the backend need not — each
    # node serves the image from its own backend-appropriate pool, so an opposite-
    # backend node with ready clones is a valid (and warm) candidate. Only the
    # wrong-arch node (incompatible kernel + agent) drops out.
    warm_req = {"image": "img:x", "pool_size": 1, "block_network": True}
    assert {n.node_id for n in mesh.candidates(warm_req)} == {"self", "kvmnode", "hvfnode"}
    assert mesh.place(warm_req) in {"kvmnode", "hvfnode"}

    # Snapshot request: only a compatible node carrying the template qualifies.
    assert {n.node_id for n in mesh.candidates({"snapshot": "snap1"})} == {"kvmnode"}

    # Plain image boot: arch must match (kernel + injected agent are arch-specific),
    # but the backend need not — a fresh boot runs under either hypervisor. So the
    # wrong-arch node drops out while the wrong-backend (same-arch) node stays in.
    plain = {n.node_id for n in mesh.candidates({"image": "plain"})}
    assert plain == {"self", "hvfnode", "kvmnode"}
    assert "armnode" not in plain
