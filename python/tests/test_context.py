import pytest
from click.testing import CliRunner

from vmon import cli
from vmon.client import DaemonError, LocalTransport, MeshTransport
from vmon.context import (
    LOCAL,
    Context,
    ContextStore,
    contexts_path,
    roster_from_status,
)


class FakeGateway:
    # Class-level registry keyed by base_url controls each endpoint's behavior.
    behavior: dict = {}
    healthz: dict = {}
    seen: list = []

    def __init__(self, base_url, token=None):
        self.base_url = base_url
        self.token = token

    def call(self, method, **params):
        FakeGateway.seen.append((self.base_url, "call", method, dict(params)))
        beh = FakeGateway.behavior.get(self.base_url)
        if isinstance(beh, Exception):
            raise beh
        return beh(method, params) if callable(beh) else {"ok": True, "ep": self.base_url}

    def stream(self, method, on_event, stdin=None, **params):
        FakeGateway.seen.append((self.base_url, "stream", method, dict(params)))
        beh = FakeGateway.behavior.get(self.base_url)
        if isinstance(beh, Exception):
            raise beh
        return {"ok": True, "ep": self.base_url}

    def interactive(self, method, on_event, *, tty=True, on_ready=None, **params):
        FakeGateway.seen.append((self.base_url, "interactive", method, dict(params)))
        beh = FakeGateway.behavior.get(self.base_url)
        if isinstance(beh, Exception):
            raise beh
        return {"ok": True, "ep": self.base_url}

    def ensure_running(self):
        FakeGateway.seen.append((self.base_url, "healthz", None, {}))
        hz = FakeGateway.healthz.get(self.base_url)
        if isinstance(hz, Exception):
            raise hz
        return {}


@pytest.fixture
def fake_gateway(monkeypatch):
    FakeGateway.behavior = {}
    FakeGateway.healthz = {}
    FakeGateway.seen = []
    monkeypatch.setattr("vmon.client._Gateway", FakeGateway)
    return FakeGateway


def _configure_cli_env(monkeypatch, tmp_path):
    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.setenv("VMON_API_TOKEN", "token")
    monkeypatch.delenv("VMON_CONTEXT", raising=False)


def _stub_status(monkeypatch):
    monkeypatch.setattr(
        "vmon.cli._fetch_mesh_status",
        lambda url, token: {
            "self": {"advertise": "http://a", "arch": "x86_64"},
            "peers": [{"advertise": "http://b", "arch": "x86_64"}],
        },
    )


def test_store_round_trip(tmp_path, monkeypatch):
    monkeypatch.delenv("VMON_CONTEXT", raising=False)
    path = contexts_path(tmp_path)
    store = ContextStore(path)
    store.put(Context("prod", ["http://a", "http://b"], region="r", updated=1.0))
    store.use("prod")

    reloaded = ContextStore(path)
    reloaded.load()
    ctx = reloaded.get("prod")

    assert ctx is not None
    assert ctx.endpoints == ["http://a", "http://b"]
    assert ctx.region == "r"
    assert ctx.updated == 1.0
    assert reloaded.current_name() == "prod"
    assert "endpoint_arches" not in path.read_text(encoding="utf-8")


def test_store_rejects_path_traversal_names(tmp_path):
    store = ContextStore(contexts_path(tmp_path))
    for name in ("../evil", "..", "a/b", ".hidden", ""):
        with pytest.raises(ValueError):
            store.put(Context(name, ["http://a"]))
        with pytest.raises(ValueError):
            store.save_token(name, "t")
    # Tolerated on the removal paths so a legacy bad record can still be dropped.
    store.remove_token("../evil")
    store.remove("../evil")
    assert store.load_token("../evil") is None


def test_load_missing_and_corrupt(tmp_path):
    path = contexts_path(tmp_path)
    store = ContextStore(path)
    store.load()
    assert store.list() == []

    path.write_text("{not json", encoding="utf-8")
    reloaded = ContextStore(path)
    reloaded.load()
    assert reloaded.list() == []


def test_current_env_override(tmp_path, monkeypatch):
    store = ContextStore(contexts_path(tmp_path))
    store.put(Context("a", ["http://a"]))
    store.put(Context("b", ["http://b"]))
    store.use("a")

    monkeypatch.setenv("VMON_CONTEXT", "b")
    assert store.current_name() == "b"

    monkeypatch.delenv("VMON_CONTEXT")
    assert store.current_name() == "a"


def test_current_local_is_none(tmp_path, monkeypatch):
    monkeypatch.delenv("VMON_CONTEXT", raising=False)
    store = ContextStore(contexts_path(tmp_path))
    store.use(LOCAL)

    assert store.current() is None
    assert store.current_name() == LOCAL


def test_token_never_persisted(tmp_path):
    path = contexts_path(tmp_path)
    store = ContextStore(path)
    store.put(Context("c", ["http://a"], token="SUPERSECRET"))

    assert "SUPERSECRET" not in path.read_text(encoding="utf-8")

    reloaded = ContextStore(path)
    reloaded.load()
    ctx = reloaded.get("c")
    assert ctx is not None
    assert ctx.token is None


def test_legacy_endpoint_arches_are_ignored(tmp_path):
    path = contexts_path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """
{
  "current": "prod",
  "contexts": {
    "prod": {
      "name": "prod",
      "endpoints": ["http://a", "http://b"],
      "endpoint_arches": {"http://a": "x86_64", "http://b": "aarch64"}
    }
  }
}
""".strip(),
        encoding="utf-8",
    )

    store = ContextStore(path)
    store.load()
    ctx = store.get("prod")

    assert ctx is not None
    assert ctx.endpoints == ["http://a", "http://b"]
    store.save()
    assert "endpoint_arches" not in path.read_text(encoding="utf-8")


def test_roster_from_status():
    status = {
        "self": {"advertise": "http://a", "arch": "x86_64"},
        "peers": [
            {"advertise": "http://b", "arch": "aarch64"},
            {"advertise": "http://a", "arch": "x86_64"},
        ],
    }

    assert roster_from_status(status) == ["http://a", "http://b"]
    assert roster_from_status({}, fallback="http://x") == ["http://x"]


def test_empty_endpoints_rejected():
    with pytest.raises(DaemonError) as excinfo:
        MeshTransport([])

    assert excinfo.value.code == "invalid"


def test_failover_rotates_on_unreachable(fake_gateway):
    fake_gateway.behavior["http://dead"] = DaemonError("x", code="unreachable")
    fake_gateway.behavior["http://live"] = lambda method, params: {
        "ok": True,
        "ep": "http://live",
        "method": method,
    }
    captured = []
    client = MeshTransport(
        ["http://dead", "http://live"], on_roster=lambda eps: captured.append(eps)
    )

    assert client.call("ps") == {"ok": True, "ep": "http://live", "method": "ps"}
    assert captured[-1] == ["http://live", "http://dead"]


def test_no_rotation_on_5xx(fake_gateway):
    fake_gateway.behavior["http://dead"] = DaemonError("x", code="503")

    with pytest.raises(DaemonError) as excinfo:
        MeshTransport(["http://dead", "http://live"]).call("ps")

    assert excinfo.value.code == "503"
    assert not any(
        base_url == "http://live" and kind == "call"
        for base_url, kind, _method, _params in fake_gateway.seen
    )


def test_stable_idempotency_key_across_endpoints(fake_gateway):
    fake_gateway.behavior["http://dead"] = DaemonError("x", code="unreachable")

    MeshTransport(["http://dead", "http://live"]).call("run", image="x")

    run_calls = [
        record for record in fake_gateway.seen if record[1] == "call" and record[2] == "run"
    ]
    keys = [record[3].get("idempotency_key") for record in run_calls]
    assert [record[0] for record in run_calls] == ["http://dead", "http://live"]
    assert len(keys) == 2
    assert keys[0] == keys[1]
    assert keys[0]


def test_mutating_call_uses_healthz_then_single(fake_gateway):
    fake_gateway.healthz["http://dead"] = DaemonError("x", code="unreachable")
    fake_gateway.healthz["http://live"] = None

    MeshTransport(["http://dead", "http://live"]).call("extend", secs=10)

    assert ("http://dead", "healthz", None, {}) in fake_gateway.seen
    assert ("http://live", "healthz", None, {}) in fake_gateway.seen
    extend_calls = [
        record for record in fake_gateway.seen if record[1] == "call" and record[2] == "extend"
    ]
    assert [record[0] for record in extend_calls] == ["http://live"]


def test_stop_daemon_unsupported():
    with pytest.raises(DaemonError) as excinfo:
        MeshTransport(["http://a"]).stop_daemon()

    assert excinfo.value.code == "unsupported"


def test_client_selection_context(tmp_path, monkeypatch):
    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.setenv("VMON_API_TOKEN", "t")
    monkeypatch.delenv("VMON_CONTEXT", raising=False)

    store = ContextStore(contexts_path(tmp_path))
    store.put(Context("prod", ["http://a"]))
    store.use("prod")
    assert isinstance(cli._client(), MeshTransport)



def test_client_selection_missing_context_errors(tmp_path, monkeypatch):
    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.setenv("VMON_API_TOKEN", "t")
    monkeypatch.setenv("VMON_CONTEXT", "ghost")
    with pytest.raises(DaemonError):
        cli._client()


def test_client_selection_endpointless_context_errors(tmp_path, monkeypatch):
    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.setenv("VMON_API_TOKEN", "t")
    monkeypatch.delenv("VMON_CONTEXT", raising=False)
    store = ContextStore(contexts_path(tmp_path))
    store.put(Context("empty", []))
    store.use("empty")
    with pytest.raises(DaemonError):
        cli._client()


def test_client_selection_local_uses_daemon(tmp_path, monkeypatch):
    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.setenv("VMON_API_TOKEN", "t")
    monkeypatch.delenv("VMON_CONTEXT", raising=False)
    # A mesh.json on disk is ignored by the CLI; contexts are the only non-local
    # transport, and explicit 'local' always selects the daemon.
    (tmp_path / "mesh.json").write_text(
        '{"enabled": true, "advertise": "http://mesh"}', encoding="utf-8"
    )
    store = ContextStore(contexts_path(tmp_path))
    store.use(LOCAL)
    assert isinstance(cli._client(), LocalTransport)


def test_cli_create_and_ls(tmp_path, monkeypatch):
    _configure_cli_env(monkeypatch, tmp_path)
    _stub_status(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(cli.cli, ["context", "create", "prod", "--server", "http://a"])
    assert result.exit_code == 0

    result = runner.invoke(cli.cli, ["context", "ls"])
    assert result.exit_code == 0
    assert "prod" in result.output

    store = ContextStore(contexts_path(tmp_path))
    store.load()
    assert store.get("prod").endpoints == ["http://a", "http://b"]
    assert store.current_name() == "prod"


def test_cli_create_does_not_persist_token(tmp_path, monkeypatch):
    _configure_cli_env(monkeypatch, tmp_path)
    _stub_status(monkeypatch)

    result = CliRunner().invoke(
        cli.cli,
        ["context", "create", "prod", "--server", "http://a", "--token", "SECRET123"],
    )

    assert result.exit_code == 0
    assert "SECRET123" not in contexts_path(tmp_path).read_text(encoding="utf-8")


def test_cli_create_save_token_private_file_not_contexts(tmp_path, monkeypatch):
    _configure_cli_env(monkeypatch, tmp_path)
    _stub_status(monkeypatch)

    result = CliRunner().invoke(
        cli.cli,
        [
            "context",
            "create",
            "prod",
            "--server",
            "http://a",
            "--token",
            "SECRET123",
            "--save-token",
        ],
    )

    assert result.exit_code == 0
    store = ContextStore(contexts_path(tmp_path))
    token_path = store.token_path("prod")
    assert token_path.read_text(encoding="utf-8").strip() == "SECRET123"
    assert token_path.stat().st_mode & 0o777 == 0o600
    assert token_path.parent.stat().st_mode & 0o777 == 0o700
    assert "SECRET123" not in contexts_path(tmp_path).read_text(encoding="utf-8")




def test_cli_client_token_resolution_env_beats_saved_file(tmp_path, monkeypatch):
    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.delenv("VMON_API_TOKEN", raising=False)
    monkeypatch.delenv("VMON_CONTEXT", raising=False)
    store = ContextStore(contexts_path(tmp_path))
    store.put(Context("prod", ["http://a"]))
    store.save_token("prod", "FILE_TOKEN")
    store.use("prod")
    seen: list[str | None] = []

    class RecordingMesh:
        def __init__(self, endpoints, token=None, *, on_roster=None):
            seen.append(token)

    monkeypatch.setattr(cli, "MeshTransport", RecordingMesh)

    cli._client()
    monkeypatch.setenv("VMON_API_TOKEN", "ENV_TOKEN")
    cli._client()

    assert seen == ["FILE_TOKEN", "ENV_TOKEN"]


def test_cli_refresh_uses_saved_token_and_keeps_file(tmp_path, monkeypatch):
    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.delenv("VMON_API_TOKEN", raising=False)
    monkeypatch.delenv("VMON_CONTEXT", raising=False)
    store = ContextStore(contexts_path(tmp_path))
    store.put(Context("prod", ["http://a"]))
    store.save_token("prod", "SAVED_TOKEN")
    seen: list[str] = []

    def fake_status(url, token):
        seen.append(token)
        return {"self": {"advertise": url, "arch": "x86_64"}, "peers": []}

    monkeypatch.setattr(cli, "_fetch_mesh_status", fake_status)

    result = CliRunner().invoke(cli.cli, ["context", "refresh", "prod"])

    assert result.exit_code == 0
    assert seen == ["SAVED_TOKEN"]
    assert store.load_token("prod") == "SAVED_TOKEN"


def test_cli_use_unknown_fails(tmp_path, monkeypatch):
    _configure_cli_env(monkeypatch, tmp_path)

    result = CliRunner().invoke(cli.cli, ["context", "use", "nope"])

    assert result.exit_code != 0


def test_cli_rm(tmp_path, monkeypatch):
    _configure_cli_env(monkeypatch, tmp_path)
    _stub_status(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(cli.cli, ["context", "create", "prod", "--server", "http://a"])
    assert result.exit_code == 0
    store = ContextStore(contexts_path(tmp_path))
    store.save_token("prod", "SAVED_TOKEN")
    result = runner.invoke(cli.cli, ["context", "rm", "prod"])
    assert result.exit_code == 0

    store = ContextStore(contexts_path(tmp_path))
    store.load()
    assert "prod" not in [ctx.name for ctx in store.list()]
    assert not store.token_path("prod").exists()


def test_cli_inspect_omits_token(tmp_path, monkeypatch):
    _configure_cli_env(monkeypatch, tmp_path)
    _stub_status(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli.cli,
        ["context", "create", "prod", "--server", "http://a", "--token", "SECRET123"],
    )
    assert result.exit_code == 0

    result = runner.invoke(cli.cli, ["context", "inspect", "prod"])
    assert result.exit_code == 0
    assert "http://a" in result.output
    assert '"token_saved": false' in result.output
    assert "SECRET123" not in result.output
