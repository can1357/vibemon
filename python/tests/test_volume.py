from __future__ import annotations

import re

import pytest
from v1_stub import V1StubServer, configure_context

from vmon.sandbox import Sandbox
from vmon.volume import Volume


def test_volume_name_validation_and_wire_shape() -> None:
    for name in ["a", "abc123", "a-b", "a.b", "a_b", "a" * 64]:
        volume = Volume(name)
        assert volume.name == name
        assert volume.to_wire() == name
        assert volume.to_wire(read_only=True) == {"name": name, "read_only": True}

    for name in ["", "A", "-a", ".a", "a/b", "a b", "a" * 65, None]:
        with pytest.raises(ValueError):
            Volume(name)  # type: ignore[arg-type]


def test_volume_create_list_delete_use_v1_volume_resource(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        path = Volume("data").ensure()
        assert path.name == "data"
        assert server.last("PUT", "/v1/volumes/data").json is None
        assert Volume.list() == ["data"]
        assert server.last("GET", "/v1/volumes").headers["authorization"] == "Bearer test-token"

        Volume("cache").create()
        assert Volume.list() == ["cache", "data"]

        Volume("data").delete()
        assert Volume.list() == ["cache"]
        Volume("cache").rm()
        assert Volume.list() == []


def test_sandbox_create_mounts_named_volumes_without_host_paths(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        Sandbox.create(
            image="python:3.14-slim",
            context="prod",
            name="with-volumes",
            volumes={
                "/data": Volume("data"),
                "/readonly": (Volume("snap"), True),
                "/cache": "cache",
            },
        )

        create = server.last("POST", "/v1/sandboxes")
        assert create.json["volumes"] == {
            "/data": "data",
            "/readonly": {"name": "snap", "read_only": True},
            "/cache": "cache",
        }
        assert "host_dir" not in str(create.json["volumes"])
        assert "host_path" not in str(create.json["volumes"])


def test_volume_tag_sanitizes_truncates_and_uses_vmm_charset() -> None:
    long_tag = Volume("abc.def-ghi_jkl" * 4).tag

    assert re.fullmatch(r"[a-z0-9_]{1,32}", long_tag)
