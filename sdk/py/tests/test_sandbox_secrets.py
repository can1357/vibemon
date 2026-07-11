from __future__ import annotations

import json

from v1_stub import V1StubServer, configure_context

from vmon import Secret, connect


def test_create_redacts_secrets_and_exec_layers_environment(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        with connect() as client:
            sandbox = client.sandboxes.create(
                image="python:3.14-slim",
                context=".",
                name="secretbox",
                env={"PLAIN": "ok"},
                secrets=[Secret.from_dict({"TOKEN": "secret-value"}, name="runtime")],
                tags={"suite": "secrets"},
            )

            create_request = server.last_rpc("SandboxService/Create")
            assert create_request.json["image"] == "python:3.14-slim"
            assert create_request.json["context"] == "."
            assert create_request.json["name"] == "secretbox"
            assert create_request.json["env"] == {"PLAIN": "ok"}
            assert create_request.json["secrets"] == [
                {"name": "runtime", "values": {"TOKEN": "secret-value"}}
            ]

            assert "secret-value" not in json.dumps(sandbox.info.raw, sort_keys=True)
            assert "secret-value" not in json.dumps(sandbox.refresh().raw, sort_keys=True)

            process = sandbox.exec("env", env={"TOKEN": "call-wins", "EXTRA": "1"})
            assert process.wait(timeout=5).code == 0
            seen_env = json.loads(process.stdout.read().decode("utf-8"))

            assert seen_env["PLAIN"] == "ok"
            assert seen_env["TOKEN"] == "call-wins"
            assert seen_env["EXTRA"] == "1"

            exec_request = server.last_rpc("SandboxService/Exec", id="secretbox")
            assert exec_request.json["cmd"] == ["env"]
            assert exec_request.json["env"]["PLAIN"] == "ok"
            assert exec_request.json["env"]["TOKEN"] == "call-wins"
