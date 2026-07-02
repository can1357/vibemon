import asyncio
import json
import time

import pytest

pytest.importorskip("fastapi")
from starlette.testclient import TestClient


class FakeSandbox:
    calls: list[dict[str, object]] = []

    def __init__(self, name, tags=None):
        self.name = name
        self.tags = dict(tags or {})
        self.returncode = None
        self.connect_token = None
        self.terminated = False

    @classmethod
    def create(cls, **kwargs):
        cls.calls.append(dict(kwargs))
        return cls(kwargs.get("name") or "sb", kwargs.get("tags") or {})

    def snapshot_filesystem(self, name=None):
        return "img1"

    def set_network_policy(self, block_network=None, cidr_allow=None, domain_allow=None):
        return {
            "block_network": block_network,
            "egress_allow": cidr_allow,
            "egress_allow_domains": domain_allow,
        }

    def create_connect_token(self):
        self.connect_token = "tok"
        return self.connect_token

    def tunnels(self):
        return {8080: ("127.0.0.1", 12345)}

    def extend(self, secs):
        return {"deadline_unix": 1}

    def metrics(self):
        return {"vcpu_exits": 7}

    def terminate(self):
        self.terminated = True


@pytest.fixture
def app(monkeypatch):
    import vmon.server as server_mod

    FakeSandbox.calls = []
    monkeypatch.delenv("VMON_API_TOKEN", raising=False)
    monkeypatch.setattr("vmon.core.Engine._sandbox_class", staticmethod(lambda: FakeSandbox))
    return server_mod.create_app()


@pytest.fixture
def client(app):
    with TestClient(app) as test_client:
        yield test_client


def test_post_sandbox_forwards_tags_volumes_and_secrets(client):
    response = client.post(
        "/v1/sandboxes",
        json={
            "template": "tpl",
            "name": "sb1",
            "tags": {"k": "v"},
            "volumes": {"/data": "vol1"},
            "secrets": [{"FOO": "bar"}],
        },
    )

    assert response.status_code == 201
    body = response.json()
    assert body["id"] == "sb1"
    assert body["tags"] == {"k": "v"}
    call = FakeSandbox.calls[-1]
    assert call["tags"] == {"k": "v"}
    assert list(call["volumes"]) == ["/data"]
    assert call["secrets"][0].names() == ["FOO"]


def test_post_sandbox_volume_read_only_preserved(client):
    response = client.post(
        "/v1/sandboxes",
        json={
            "template": "tpl",
            "name": "sb-ro",
            "volumes": {"/data": {"name": "vol1", "read_only": True}},
        },
    )

    assert response.status_code == 201
    vol, read_only = FakeSandbox.calls[-1]["volumes"]["/data"]
    assert vol.name == "vol1"
    assert read_only is True


def test_list_sandboxes_filters_by_tag(client):
    client.post("/v1/sandboxes", json={"template": "tpl", "name": "match", "tags": {"k": "v"}})
    client.post("/v1/sandboxes", json={"template": "tpl", "name": "miss", "tags": {"k": "other"}})

    response = client.get("/v1/sandboxes", params={"tag": "k:v"})

    assert response.status_code == 200
    sandboxes = response.json()["sandboxes"]
    assert [sandbox["id"] for sandbox in sandboxes] == ["match"]


def test_snapshot_extend_network_and_tunnels_endpoints(client):
    client.post("/v1/sandboxes", json={"template": "tpl", "name": "sb1"})

    assert client.post("/v1/sandboxes/sb1/snapshot", json={"name": "img"}).json() == {
        "image": "img1"
    }
    assert client.post("/v1/sandboxes/sb1/extend", json={"secs": 60}).json() == {"deadline_unix": 1}

    network = client.put(
        "/v1/sandboxes/sb1/network",
        json={"block_network": True, "cidr_allow": ["10.0.0.0/8"], "domain_allow": ["example.com"]},
    )
    assert network.status_code == 200
    assert network.json()["block_network"] is True

    tunnels = client.get("/v1/sandboxes/sb1/tunnels")
    assert tunnels.status_code == 200
    assert tunnels.json() == {"tunnels": {"8080": ["127.0.0.1", 12345]}, "connect_token": "tok"}


def test_metrics_endpoint_returns_runtime_counters(client):
    client.post("/v1/sandboxes", json={"template": "tpl", "name": "sb1"})
    response = client.get("/v1/sandboxes/sb1/metrics")
    assert response.status_code == 200
    assert response.json() == {"vcpu_exits": 7}


def test_mesh_status_reports_sandbox_checkpoint_ages(app, client):
    app.state.mesh.enabled = True
    created = client.post(
        "/v1/sandboxes",
        json={"template": "tpl", "name": "ha-sb", "ha": "async"},
    )
    off = client.post(
        "/v1/sandboxes",
        json={"template": "tpl", "name": "off-sb", "ha": "off"},
    )
    assert created.status_code == 201
    assert off.status_code == 201

    first = client.get("/v1/mesh/status")

    assert first.status_code == 200
    rows = {row["id"]: row for row in first.json()["self"]["sandboxes"]}
    assert rows["ha-sb"]["ha"] == "async"
    assert rows["ha-sb"]["restart_policy"] == "none"
    assert rows["ha-sb"]["checkpoint_age_sec"] is None
    assert rows["ha-sb"]["replicas"] == []
    assert rows["off-sb"]["ha"] == "off"
    assert rows["off-sb"]["checkpoint_age_sec"] is None
    assert rows["off-sb"]["replicas"] == []

    app.state.checkpoint_times["ha-sb"] = time.time() - 12.0
    second = client.get("/v1/mesh/status")

    assert second.status_code == 200
    rows = {row["id"]: row for row in second.json()["self"]["sandboxes"]}
    age = rows["ha-sb"]["checkpoint_age_sec"]
    assert isinstance(age, float)
    assert 0.0 <= age < 60.0
    assert rows["off-sb"]["checkpoint_age_sec"] is None


def test_replicate_forever_records_checkpoint_timestamps(app, client, monkeypatch, tmp_path):
    from fastapi.security import HTTPBearer

    import vmon.server.reconciler as reconciler
    from vmon.server.runtime import ServerRuntime

    created = client.post(
        "/v1/sandboxes",
        json={"template": "tpl", "name": "ha-sb", "ha": "async"},
    )
    assert created.status_code == 201
    app.state.mesh.enabled = True
    app.state.checkpoint_times["gone"] = 1.0
    posts: list[tuple[str, str, dict[str, object]]] = []
    cleanups: list[tuple[str, str]] = []

    def fake_prepare(sid):
        assert sid == "ha-sb"
        return {
            "digest": "digest-a",
            "snapshot_dir": str(tmp_path / "checkpoint"),
            "params": {"name": sid},
        }

    def fake_cleanup(digest, snapshot_dir):
        cleanups.append((digest, snapshot_dir))

    def fake_post(url, path, payload, **_kwargs):
        posts.append((url, path, payload))
        return {"ok": True}

    monkeypatch.setattr(app.state.supervisor._engine, "replicate_prepare", fake_prepare)
    monkeypatch.setattr(app.state.supervisor._engine, "replicate_cleanup", fake_cleanup)
    monkeypatch.setattr(app.state.mesh, "replica_targets", lambda _sid, _k: ["peer-a"])
    monkeypatch.setattr(app.state.mesh, "peer_url", lambda _peer_id: "http://peer")
    monkeypatch.setattr(app.state.mesh.transport, "post", fake_post)

    class StopReplicate(Exception):
        pass

    first_sleep = True

    async def fake_sleep(_delay):
        nonlocal first_sleep
        if first_sleep:
            first_sleep = False
            return
        raise StopReplicate

    ctx = ServerRuntime(
        supervisor=app.state.supervisor,
        mesh=app.state.mesh,
        replica_store=app.state.replica_store,
        record_store=app.state.record_store,
        lease_manager=app.state.lease_manager,
        expected_token=None,
        outbound_token=None,
        client_token=None,
        bearer=HTTPBearer(auto_error=False),
        config=app.state.config,
        checkpoint_times=app.state.checkpoint_times,
    )
    monkeypatch.setattr(reconciler.asyncio, "sleep", fake_sleep)

    async def run_once():
        try:
            await reconciler.replicate_forever(ctx)
        except StopReplicate:
            return
        raise AssertionError("replicator did not stop after one pass")

    asyncio.run(run_once())

    assert posts == [
        (
            "http://peer",
            "/v1/mesh/replica/receive",
            {
                "digest": "digest-a",
                "source_url": app.state.mesh.advertise,
                "source_node": app.state.mesh.node_id,
                "params": {"name": "ha-sb"},
            },
        )
    ]
    assert cleanups == [("digest-a", str(tmp_path / "checkpoint"))]
    assert isinstance(app.state.checkpoint_times["ha-sb"], float)
    assert "gone" not in app.state.checkpoint_times


def test_mesh_routes_accept_request_scoped_arch_without_leaking_to_local_create(
    app, client, monkeypatch
):
    placements: list[dict[str, object]] = []
    app.state.mesh.enabled = True

    def place_local(params):
        placements.append(dict(params))
        return app.state.mesh.node_id

    def run_success(params, **_kwargs):
        return {"detached": bool(params.get("detach"))}

    def restore_success(params, **_kwargs):
        return {"detached": bool(params.get("detach"))}

    def fork_success(params):
        return [{"name": "clone-a"}]

    monkeypatch.setattr(app.state.mesh, "place", place_local)
    monkeypatch.setattr(app.state.supervisor._engine, "run", run_success)
    monkeypatch.setattr(app.state.supervisor._engine, "restore", restore_success)
    monkeypatch.setattr(app.state.supervisor._engine, "fork", fork_success)

    create = client.post(
        "/v1/sandboxes",
        json={"template": "tpl", "name": "placed-create", "arch": "aarch64"},
    )
    run = client.post(
        "/v1/run",
        json={"image": "alpine:latest", "detach": True, "arch": "aarch64"},
    )
    restore = client.post(
        "/v1/restore",
        json={"snapshot": "snap-a", "detach": True, "arch": "aarch64"},
    )
    fork = client.post(
        "/v1/fork",
        json={"snapshot": "snap-a", "count": 1, "arch": "aarch64"},
    )

    assert create.status_code == 201
    assert run.status_code == 200
    assert restore.status_code == 200
    assert fork.status_code == 200
    assert [params["arch"] for params in placements] == ["aarch64"] * 4
    assert [params["template"] for params in placements[:1]] == ["tpl"]
    assert [params["image"] for params in placements[1:2]] == ["alpine:latest"]
    assert [params["snapshot"] for params in placements[2:]] == ["snap-a", "snap-a"]
    assert "arch" not in FakeSandbox.calls[-1]


def test_bearer_auth_required_when_token_is_configured(monkeypatch):
    import vmon.server as server_mod

    FakeSandbox.calls = []
    monkeypatch.setenv("VMON_API_TOKEN", "secret")
    monkeypatch.setattr("vmon.core.Engine._sandbox_class", staticmethod(lambda: FakeSandbox))
    with TestClient(server_mod.create_app()) as client:
        assert client.post("/v1/sandboxes", json={"template": "tpl"}).status_code == 401
        assert (
            client.post(
                "/v1/sandboxes",
                json={"template": "tpl"},
                headers={"Authorization": "Bearer wrong"},
            ).status_code
            == 401
        )
        assert (
            client.post(
                "/v1/sandboxes",
                json={"template": "tpl", "name": "ok"},
                headers={"Authorization": "Bearer secret"},
            ).status_code
            == 201
        )


def test_server_rejects_bad_request_inputs(client):
    client.post("/v1/sandboxes", json={"template": "tpl", "name": "sb1"})

    invalid_exec = client.post("/v1/sandboxes/sb1/exec", json={})
    assert invalid_exec.status_code == 400
    assert invalid_exec.json() == {"detail": "exec cmd must not be empty"}

    invalid_extend = client.post("/v1/sandboxes/sb1/extend", json={"secs": 0})
    assert invalid_extend.status_code == 400
    assert invalid_extend.json() == {"detail": "secs must be positive"}

    invalid_network = client.put("/v1/sandboxes/sb1/network", json={"cidr_allow": ["not-a-cidr"]})
    assert invalid_network.status_code == 400
    assert invalid_network.json() == {"detail": "cidr_allow entries must be valid CIDR networks"}

    invalid_create = client.post(
        "/v1/sandboxes",
        json={"template": "tpl", "name": "bad", "ports": [70000]},
    )
    assert invalid_create.status_code == 400
    assert invalid_create.json() == {"detail": "ports must be TCP port numbers from 1 to 65535"}


def test_expired_sandbox_is_reaped_by_supervisor(app, client):
    client.post("/v1/sandboxes", json={"template": "tpl", "name": "idle", "timeout": 0.01})
    record = app.state.supervisor.get("idle")
    record.last_active -= 1.0

    asyncio.run(app.state.supervisor.reap_expired())

    assert record.status == "terminated"
    assert record.detail["terminated_reason"] == "idle_timeout"
    assert record.sandbox is None


def test_events_stream_includes_created_event(app):
    async def read_one_event():
        body_seen = asyncio.get_running_loop().create_future()
        disconnect = asyncio.Event()
        request_sent = False

        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/v1/events",
            "raw_path": b"/v1/events",
            "query_string": b"",
            "headers": [],
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
            "root_path": "",
        }

        async def receive():
            nonlocal request_sent
            if not request_sent:
                request_sent = True
                return {"type": "http.request", "body": b"", "more_body": False}
            await disconnect.wait()
            return {"type": "http.disconnect"}

        async def send(message):
            if message["type"] == "http.response.body" and message.get("body", b"").startswith(
                b"data: "
            ):
                if not body_seen.done():
                    body_seen.set_result(message["body"])

        task = asyncio.create_task(app(scope, receive, send))
        try:
            for _ in range(100):
                if app.state.supervisor._event_subscribers:
                    break
                await asyncio.sleep(0.01)
            app.state.supervisor.emit("created", id="evt", name="evt", tags={})
            body = await asyncio.wait_for(body_seen, timeout=3.0)
        finally:
            disconnect.set()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        return json.loads(body.decode().removeprefix("data: ").strip())

    event = asyncio.run(read_one_event())
    assert event["event"] == "created"
    assert event["id"] == "evt"


def test_parse_warm_images_requires_explicit_count(monkeypatch):
    from vmon.server.models import _parse_warm_images

    monkeypatch.delenv("VMON_WARM_POOL_SIZE", raising=False)
    # Bare refs keep their image tag and take the default count (1); a ':' is
    # image-tag syntax, never a count delimiter.
    assert _parse_warm_images("python:3.14") == [("python:3.14", 1)]
    assert _parse_warm_images("node:20") == [("node:20", 1)]
    assert _parse_warm_images("alpine:latest, postgres:16") == [
        ("alpine:latest", 1),
        ("postgres:16", 1),
    ]
    # Counts are set only with an explicit '='.
    assert _parse_warm_images("alpine:latest=3") == [("alpine:latest", 3)]
    assert _parse_warm_images("redis:7=2, python:3=4") == [("redis:7", 2), ("python:3", 4)]
    # Empty / missing input yields no pools.
    assert _parse_warm_images("") == []
    assert _parse_warm_images(None) == []
    # The default count is explicit; env overrides are resolved in vmon.config.
    assert _parse_warm_images("busybox", default_count=5) == [("busybox", 5)]
    # Negative or non-integer counts are rejected.
    with pytest.raises(ValueError):
        _parse_warm_images("alpine=-1")
    with pytest.raises(ValueError):
        _parse_warm_images("alpine=x")


def test_server_prewarms_images_and_drains_pools_on_exit(monkeypatch, tmp_path):
    import vmon.sandbox as sandbox_mod
    import vmon.server as server_mod

    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.setenv("VMON_WARM_IMAGES", "alpine:latest=2, busybox")
    monkeypatch.delenv("VMON_WARM_POOL_SIZE", raising=False)

    prewarmed: list[tuple[str, int]] = []
    drained: list[bool] = []
    monkeypatch.setattr(
        sandbox_mod, "prewarm", lambda ref, *, count: prewarmed.append((ref, count))
    )
    monkeypatch.setattr(sandbox_mod, "shutdown_all_pools", lambda: drained.append(True))

    app = server_mod.create_app(token="secret")
    with TestClient(app):
        assert ("alpine:latest", 2) in prewarmed
        assert ("busybox", 1) in prewarmed
    assert drained == [True]
