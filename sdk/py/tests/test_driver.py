from __future__ import annotations

from urllib.parse import urlsplit

import pytest
from v1_stub import V1StubServer

from vmon import APIError, TransportError, connect, parse_dsn
from vmon.context import Context, ContextStore, contexts_path


def _host(server: V1StubServer) -> str:
    parsed = urlsplit(server.url)
    return parsed.netloc


def _multi_dsn(*servers: V1StubServer, discover: str = "on") -> str:
    return f"vmon://{','.join(_host(server) for server in servers)}?discover={discover}"


@pytest.mark.parametrize(
    ("dsn", "endpoints", "token", "discover", "timeout"),
    [
        (
            "vmon://alpha,beta:9000/api?token=secret&discover=off&timeout=2.5",
            ("http://alpha:8000/api", "http://beta:9000/api"),
            "secret",
            False,
            2.5,
        ),
        ("vmons://secure", ("https://secure:8000",), None, True, 60.0),
        (
            "vmon+unix:///tmp/vmond.sock",
            ("vmon+unix:///tmp/vmond.sock",),
            None,
            True,
            60.0,
        ),
        (
            "http://localhost:8080/prefix",
            ("http://localhost:8080/prefix",),
            None,
            True,
            60.0,
        ),
        ("https://localhost", ("https://localhost",), None, True, 60.0),
    ],
)
def test_parse_dsn_table(dsn, endpoints, token, discover, timeout) -> None:
    config = parse_dsn(dsn)
    assert config.endpoints == endpoints
    assert config.token == token
    assert config.discover is discover
    assert config.timeout == timeout


@pytest.mark.parametrize(
    "dsn",
    [
        "vmon://user:password@host",
        "vmon://host?unknown=value",
        "vmon://host?discover=maybe",
        "vmon://host?timeout=0",
        "vmon://host?timeout=nan",
        "http://alpha,beta",
        "vmon+unix://host/tmp/vmond.sock",
        "ftp://host",
    ],
)
def test_parse_dsn_rejects_invalid_input(dsn) -> None:
    with pytest.raises(ValueError):
        parse_dsn(dsn)


def test_empty_dsn_environment_precedence(monkeypatch, mvm_home) -> None:
    monkeypatch.setenv("VMON_DSN", "vmon://dsn-host")
    monkeypatch.setenv("VMON_CONTEXT", "ignored")
    assert parse_dsn().endpoints == ("http://dsn-host:8000",)

    monkeypatch.delenv("VMON_DSN")
    store = ContextStore(contexts_path(mvm_home))
    store.load()
    store.put(Context("prod", ["http://context-host:9000"]))
    store.save_token("prod", "saved-token")
    monkeypatch.setenv("VMON_CONTEXT", "prod")
    config = parse_dsn()
    assert config.endpoints == ("http://context-host:9000",)
    assert config.token == "saved-token"

    monkeypatch.delenv("VMON_CONTEXT")
    local = parse_dsn()
    assert local.endpoints == (f"vmon+unix://{mvm_home}/vmond.sock",)


def test_context_dsn_token_precedence(monkeypatch, mvm_home) -> None:
    store = ContextStore(contexts_path(mvm_home))
    store.load()
    store.put(Context("prod", ["http://one:8000", "https://two:8443"]))
    store.save_token("prod", "saved")

    assert parse_dsn("vmon+context://prod").token == "saved"
    monkeypatch.setenv("VMON_API_TOKEN", "environment")
    assert parse_dsn("vmon+context://prod").token == "environment"
    assert parse_dsn("vmon+context://prod?token=dsn").token == "dsn"

    with pytest.raises(ValueError, match="missing"):
        parse_dsn("vmon+context://missing")


def test_explicit_token_overrides_dsn_and_environment(monkeypatch) -> None:
    with V1StubServer() as server:
        server.required_token = "explicit"
        monkeypatch.setenv("VMON_API_TOKEN", "environment")
        with connect(
            f"{server.url}?token=dsn",
            token="explicit",
            discover=False,
        ) as client:
            assert client.health() == {"ok": True}
        assert server.last("GET", "/healthz").headers["authorization"] == "Bearer explicit"


def test_failover_and_discovery() -> None:
    server_b = V1StubServer(node_id="node-b").start()
    server_a = V1StubServer(node_id="node-a", peers=[server_b]).start()
    try:
        with connect(server_b.url, discover=False) as client_b:
            client_b.sandboxes.create(name="on-b", image="alpine")

        with connect(server_a.url) as client:
            assert client.health() == {"ok": True}
            assert [entry.url for entry in client.driver.endpoints()] == [
                server_a.url,
                server_b.url,
            ]

            server_a.drop_requests = True
            server_a.stop()
            sandboxes = client.sandboxes.list()
            assert [sandbox.id for sandbox in sandboxes] == ["on-b"]
            assert sandboxes[0].endpoint == server_b.url
    finally:
        server_a.stop()
        server_b.stop()


def test_sandbox_relocate() -> None:
    server_b = V1StubServer(node_id="node-b").start()
    server_a = V1StubServer(node_id="node-a", peers=[server_b]).start()
    try:
        with connect(server_b.url, discover=False) as client_b:
            client_b.sandboxes.create(name="moved", image="alpine")

        with connect(_multi_dsn(server_a, server_b), discover=False) as client:
            sandbox = client.sandboxes.ref("moved")
            assert sandbox.refresh().status == "running"
            assert sandbox.endpoint == server_b.url
            assert sandbox.node == "node-b"

            missing = client.sandboxes.ref("missing")
            with pytest.raises(APIError) as exc_info:
                missing.refresh()
            assert exc_info.value.code == "not_found"
            assert exc_info.value.status == 404
    finally:
        server_a.stop()
        server_b.stop()


def test_sticky_preferred_endpoint_and_cooldown() -> None:
    server_a = V1StubServer(node_id="node-a").start()
    server_b = V1StubServer(node_id="node-b").start()
    try:
        server_a.drop_requests = True
        with connect(_multi_dsn(server_a, server_b, discover="off")) as client:
            assert client.health() == {"ok": True}
            server_a.drop_requests = False
            assert client.health() == {"ok": True}

        assert len(server_a.recorded("GET", "/healthz")) == 1
        assert len(server_b.recorded("GET", "/healthz")) == 2
    finally:
        server_a.stop()
        server_b.stop()


def test_discovery_off_keeps_seed_roster() -> None:
    server_b = V1StubServer(node_id="node-b").start()
    server_a = V1StubServer(node_id="node-a", peers=[server_b]).start()
    try:
        with connect(server_a.url, discover=False) as client:
            client.health()
            assert [entry.url for entry in client.driver.endpoints()] == [server_a.url]
            client.driver.refresh(force=True)
            assert [entry.url for entry in client.driver.endpoints()] == [server_a.url]
    finally:
        server_a.stop()
        server_b.stop()


def test_all_transport_failures_raise_last_error() -> None:
    server_a = V1StubServer(node_id="node-a").start()
    server_b = V1StubServer(node_id="node-b").start()
    try:
        server_a.drop_requests = True
        server_b.drop_requests = True
        with connect(_multi_dsn(server_a, server_b, discover="off")) as client:
            with pytest.raises(TransportError):
                client.health()
    finally:
        server_a.stop()
        server_b.stop()


def test_sandbox_list_propagates_api_errors_from_any_node() -> None:
    server_a = V1StubServer(node_id="node-a").start()
    server_b = V1StubServer(node_id="node-b").start()
    try:
        server_a.required_token = "required"
        with connect(_multi_dsn(server_a, server_b, discover="off")) as client:
            with pytest.raises(APIError) as exc_info:
                client.sandboxes.list()
            assert exc_info.value.status == 401
    finally:
        server_a.stop()
        server_b.stop()


def test_sandbox_list_empty_success_wins_over_transport_failure() -> None:
    server_a = V1StubServer(node_id="node-a").start()
    server_b = V1StubServer(node_id="node-b").start()
    try:
        server_a.drop_requests = True
        with connect(_multi_dsn(server_a, server_b, discover="off")) as client:
            assert client.sandboxes.list() == []
    finally:
        server_a.stop()
        server_b.stop()
