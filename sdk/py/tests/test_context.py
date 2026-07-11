from __future__ import annotations

import json

import pytest
from v1_stub import V1StubServer

from vmon import connect, parse_dsn
from vmon.context import Context, ContextStore, context_token_path, contexts_path


def test_context_store_round_trip_omits_tokens_from_contexts_json(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("VMON_CONTEXT", raising=False)
    store = ContextStore(contexts_path(tmp_path))

    store.put(Context("prod", ["https://a.example", "https://b.example"], token="in-memory"))
    store.use("prod")
    store.save_token("prod", "persisted-token")

    raw = json.loads(contexts_path(tmp_path).read_text(encoding="utf-8"))
    assert raw["current"] == "prod"
    assert raw["contexts"]["prod"]["endpoints"] == [
        "https://a.example",
        "https://b.example",
    ]
    contexts_text = contexts_path(tmp_path).read_text(encoding="utf-8")
    assert "persisted-token" not in contexts_text
    assert "in-memory" not in contexts_text
    assert context_token_path("prod", tmp_path).read_text(encoding="utf-8").strip() == (
        "persisted-token"
    )

    reloaded = ContextStore(contexts_path(tmp_path))
    reloaded.load()
    assert reloaded.current_name() == "prod"
    assert reloaded.get("prod") == Context("prod", ["https://a.example", "https://b.example"])
    assert reloaded.resolve_token("prod") == "persisted-token"


def test_context_store_rejects_path_traversal_for_credentials(tmp_path) -> None:
    store = ContextStore(contexts_path(tmp_path))

    with pytest.raises(ValueError):
        store.put(Context("../evil", ["http://127.0.0.1"]))
    with pytest.raises(ValueError):
        store.save_token("../evil", "token")
    assert store.load_token("../evil") is None


def test_context_dsn_resolves_all_endpoints_and_saved_token(monkeypatch, mvm_home) -> None:
    with V1StubServer(node_id="prod-a") as first, V1StubServer(node_id="prod-b") as second:
        monkeypatch.setenv("VMON_HOME", str(mvm_home))
        monkeypatch.delenv("VMON_API_TOKEN", raising=False)
        store = ContextStore(contexts_path(mvm_home))
        store.put(Context("prod", [first.url, second.url]))
        store.save_token("prod", "saved-token")

        config = parse_dsn("vmon+context://prod")
        assert config.endpoints == (first.url, second.url)
        assert config.token == "saved-token"


def test_selected_context_uses_environment_then_saved_token(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        server.required_token = "env-token"
        monkeypatch.setenv("VMON_HOME", str(mvm_home))
        monkeypatch.setenv("VMON_CONTEXT", "prod")
        monkeypatch.setenv("VMON_API_TOKEN", "env-token")
        store = ContextStore(contexts_path(mvm_home))
        store.put(Context("prod", [server.url]))
        store.save_token("prod", "saved-token")

        with connect() as client:
            assert client.sandboxes.list() == []
        assert server.last_rpc("SandboxService/List").headers["authorization"] == (
            "Bearer env-token"
        )

        server.required_token = "saved-token"
        monkeypatch.delenv("VMON_API_TOKEN")
        with connect() as client:
            assert client.sandboxes.list() == []
        assert server.last_rpc("SandboxService/List").headers["authorization"] == (
            "Bearer saved-token"
        )


def test_create_context_is_build_input_not_transport_selection(monkeypatch, mvm_home) -> None:
    with V1StubServer(node_id="prod") as prod, V1StubServer(node_id="dev") as dev:
        monkeypatch.setenv("VMON_HOME", str(mvm_home))
        monkeypatch.setenv("VMON_CONTEXT", "prod")
        monkeypatch.delenv("VMON_API_TOKEN", raising=False)
        store = ContextStore(contexts_path(mvm_home))
        store.put(Context("prod", [prod.url]))
        store.put(Context("dev", [dev.url]))

        with connect() as client:
            sandbox = client.sandboxes.create(
                image="python:3.14-slim",
                context="dev",
                name="routed",
            )
            assert sandbox.id == "routed"
        assert prod.last_rpc("SandboxService/Create").json["context"] == "dev"
        assert dev.rpcs("SandboxService/Create") == []

        with connect("vmon+context://dev") as client:
            client.sandboxes.create(name="explicit-dev")
        assert dev.last_rpc("SandboxService/Create").json["name"] == "explicit-dev"
