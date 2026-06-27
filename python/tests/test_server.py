import asyncio
import json

import pytest

pytest.importorskip("fastapi")
from starlette.testclient import TestClient


class FakeSandbox:
    calls = []

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
    from vmon.server import _parse_warm_images

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
    # The default count honors VMON_WARM_POOL_SIZE.
    monkeypatch.setenv("VMON_WARM_POOL_SIZE", "5")
    assert _parse_warm_images("busybox") == [("busybox", 5)]
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
