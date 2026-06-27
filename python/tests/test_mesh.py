import json
import time

import pytest

pytest.importorskip("fastapi")
from starlette.responses import Response
from starlette.testclient import TestClient

from vmon.mesh import Mesh, MeshError, NodeCaps, NodeState, decode_blob, encode_blob


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

    def post(self, base_url, path, payload):
        response = self.clients[base_url].post(
            path,
            json=payload,
            headers={"Authorization": f"Bearer {self.token}", "X-Vmon-Mesh-Hop": "1"},
        )
        if response.status_code >= 400:
            raise MeshError(response.text)
        return response.json() if response.content else {}

    def get(self, base_url, path):
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


def test_node_state_wire_tolerates_missing_backend_arch():
    # Old peers / persisted blobs predate backend+arch: they must still load.
    old = NodeState.from_wire({"node_id": "old", "advertise": "http://old"})
    assert (old.backend, old.arch) == ("", "")
    rt = NodeState.from_wire(NodeState("n", "u", backend="kvm", arch="x86_64").to_wire())
    assert (rt.backend, rt.arch) == ("kvm", "x86_64")


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

    # Warm-pool request: incompatible backend/arch peers are filtered out.
    warm_req = {"image": "img:x", "pool_size": 1, "block_network": True}
    assert {n.node_id for n in mesh.candidates(warm_req)} == {"self", "kvmnode"}
    assert mesh.place(warm_req) == "kvmnode"

    # Snapshot request: only a compatible node carrying the template qualifies.
    assert {n.node_id for n in mesh.candidates({"snapshot": "snap1"})} == {"kvmnode"}

    # Plain image boot: arch must match (kernel + injected agent are arch-specific),
    # but the backend need not — a fresh boot runs under either hypervisor. So the
    # wrong-arch node drops out while the wrong-backend (same-arch) node stays in.
    plain = {n.node_id for n in mesh.candidates({"image": "plain"})}
    assert plain == {"self", "hvfnode", "kvmnode"}
    assert "armnode" not in plain
