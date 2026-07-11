from __future__ import annotations

import pytest
from v1_stub import V1StubServer, configure_context

from vmon import Volume, connect


def test_volume_name_validation_and_wire_shape() -> None:
    for name in ["a", "abc123", "a-b", "a.b", "a_b", "a" * 64]:
        volume = Volume(name)
        assert volume.name == name
        assert volume.to_wire() == name
        assert volume.to_wire(read_only=True) == {"name": name, "read_only": True}

    for invalid_name in ["", "A", "-a", ".a", "a/b", "a b", "a" * 65, None]:
        with pytest.raises(ValueError):
            Volume(invalid_name)  # type: ignore[arg-type]


def test_volume_namespace_create_list_and_delete(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        with connect() as client:
            volume = client.volumes.create("data")
            assert volume.name == "data"
            assert server.last("PUT", "/v1/volumes/data").json is None
            assert [item.name for item in client.volumes.list()] == ["data"]
            assert server.last("GET", "/v1/volumes").headers["authorization"] == (
                "Bearer test-token"
            )

            client.volumes.create("cache")
            assert [item.name for item in client.volumes.list()] == ["cache", "data"]

            client.volumes.delete(volume)
            assert [item.name for item in client.volumes.list()] == ["cache"]
            client.volumes.delete("cache")
            assert client.volumes.list() == []


def test_sandbox_create_mounts_named_volumes_without_host_paths(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        with connect() as client:
            client.sandboxes.create(
                image="python:3.14-slim",
                context=".",
                name="with-volumes",
                volumes={
                    "/data": Volume("data"),
                    "/readonly": (Volume("snap"), True),
                    "/cache": "cache",
                },
            )

        create_request = server.last("POST", "/v1/sandboxes")
        assert create_request.json["volumes"] == {
            "/data": "data",
            "/readonly": {"name": "snap", "read_only": True},
            "/cache": "cache",
        }
        assert "host_dir" not in str(create_request.json["volumes"])
        assert "host_path" not in str(create_request.json["volumes"])
