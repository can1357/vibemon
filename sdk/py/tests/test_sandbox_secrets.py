from __future__ import annotations

import json

from v1_stub import V1StubServer, configure_context

from vmon.sandbox import Sandbox
from vmon.secret import Secret


def _stream_bytes(stream: object) -> bytes:
    reader = stream.read
    return reader()


def test_create_uses_v1_body_redacts_returned_secret_values_and_exec_layers_env(
    monkeypatch, mvm_home
) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        sandbox = Sandbox.create(
            image="python:3.14-slim",
            context="prod",
            name="secretbox",
            env={"PLAIN": "ok"},
            secrets=[Secret.from_dict({"TOKEN": "secret-value"}, name="runtime")],
            tags={"suite": "secrets"},
        )

        create = server.last("POST", "/v1/sandboxes")
        assert create.json["image"] == "python:3.14-slim"
        assert create.json["name"] == "secretbox"
        assert create.json["env"] == {"PLAIN": "ok"}
        assert create.json["secrets"] == [{"name": "runtime", "values": {"TOKEN": "secret-value"}}]

        # The daemon's view is the observable persisted state returned to callers;
        # secret values must not be reflected there after create or refresh.
        assert "secret-value" not in json.dumps(sandbox.view, sort_keys=True)
        assert "secret-value" not in json.dumps(sandbox.refresh(), sort_keys=True)

        proc = sandbox.exec("env", env={"TOKEN": "call-wins", "EXTRA": "1"})
        assert proc.wait(timeout=5) == 0
        seen_env = json.loads(_stream_bytes(proc.stdout).decode("utf-8"))

        assert seen_env["PLAIN"] == "ok"
        assert seen_env["TOKEN"] == "call-wins"
        assert seen_env["EXTRA"] == "1"

        exec_request = server.last("WS", "/v1/sandboxes/secretbox/exec")
        assert exec_request.json["cmd"] == ["env"]
        assert exec_request.json["env"]["PLAIN"] == "ok"
        assert exec_request.json["env"]["TOKEN"] == "call-wins"
