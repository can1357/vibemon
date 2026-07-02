"""Behavior tests for the vmon serve configuration surface."""

from dataclasses import fields

import pytest

from vmon import doctor
from vmon.config import (
    CLI_OPTIONS,
    ENV_KEYS,
    SERVE_CONFIG_KEYS,
    ConfigError,
    ServeConfig,
    WarmImage,
    resolve_serve_config,
    validate_knob_inventory,
)


def _clear_serve_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_name in set(ENV_KEYS.values()) | {"VMON_CONFIG"}:
        monkeypatch.delenv(env_name, raising=False)


def _by_name(checks: list[doctor.Check]) -> dict[str, doctor.Check]:
    return {check.name: check for check in checks}


def test_resolve_serve_config_applies_file_env_then_cli(monkeypatch, tmp_path):
    _clear_serve_env(monkeypatch)
    config_path = tmp_path / "serve.toml"
    config_path.write_text(
        """
[serve]
host = "file.example"
port = 1111
token = "file-token"
replicas = 2
warm_pool_size = 2
warm_images = ["alpine:3.20"]
""".lstrip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("VMON_SERVE_PORT", "2222")
    monkeypatch.setenv("VMON_API_TOKEN", "env-token")
    monkeypatch.setenv("VMON_REPLICAS", "3")

    config = resolve_serve_config({"config": config_path, "token": "flag-token", "replicas": 4})

    assert config.host == "file.example"
    assert config.source("host") == "file"
    assert config.port == 2222
    assert config.source("port") == "env"
    assert config.token == "flag-token"
    assert config.source("token") == "flag"
    assert config.replicas == 4
    assert config.source("replicas") == "flag"
    assert config.warm_images == (WarmImage("alpine:3.20", 2),)


def test_unknown_serve_toml_key_is_rejected(monkeypatch, tmp_path):
    _clear_serve_env(monkeypatch)
    config_path = tmp_path / "serve.toml"
    config_path.write_text(
        """
[serve]
token = "operator"
tyop = true
""".lstrip(),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match=r"unknown serve config key\(s\): tyop"):
        resolve_serve_config({"config": config_path})


def test_doctor_serve_reports_tls_failure_and_two_node_quorum_warning(monkeypatch, tmp_path):
    _clear_serve_env(monkeypatch)
    home = tmp_path / "home"
    home.mkdir()
    (home / "mesh.json").write_text('{"expected_members": 2}\n', encoding="utf-8")
    config_path = tmp_path / "serve.toml"
    config_path.write_text(
        f"""
[serve]
home = "{home.as_posix()}"
host = "10.0.0.5"
port = 18080
token = "operator-token"
tls_cert = "/tmp/cert.pem"
replicate_sec = 30
replicas = 1
warm_images = ["alpine:3.20=1"]
""".lstrip(),
        encoding="utf-8",
    )
    rendered: list[list[doctor.Check]] = []
    monkeypatch.setattr(doctor.ui.console, "print", lambda *args, **kwargs: None)
    monkeypatch.setattr(doctor.ui, "doctor_table", lambda checks: rendered.append(list(checks)))

    assert doctor.run_doctor(serve=True, config_path=str(config_path)) == 1

    checks = _by_name(rendered[-1])
    assert checks["serve token"].status == "ok"
    assert checks["serve TLS"].status == "fail"
    assert "configured together" in checks["serve TLS"].detail
    assert checks["replication cadence"].status == "ok"
    assert checks["advertise URL"].status == "ok"
    assert checks["advertise URL"].detail == "http://10.0.0.5:18080"
    assert checks["restore quorum"].status == "warn"
    assert "2 expected members" in checks["restore quorum"].detail
    assert checks["warm images"].status == "ok"


def test_doctor_serve_flags_missing_token_bad_replication_and_warm_image_refs(tmp_path):
    checks = _by_name(
        doctor.collect_serve_config_checks(
            ServeConfig(
                home=tmp_path,
                host="10.0.0.5",
                port=18080,
                token=None,
                replicas=1,
                replicate_sec=0,
                warm_images=(WarmImage("bad ref", 1),),
            )
        )
    )

    assert checks["serve token"].status == "fail"
    assert "missing operator bearer token" in checks["serve token"].detail
    assert checks["replication cadence"].status == "fail"
    assert "replicas > 0" in checks["replication cadence"].detail
    assert checks["warm images"].status == "fail"
    assert "bad ref" in checks["warm images"].detail


def test_doctor_serve_surfaces_advertise_derivation_failures(monkeypatch, tmp_path):
    import vmon.mesh as mesh

    monkeypatch.setattr(mesh, "default_advertise", lambda host, port: "not-a-url")

    checks = _by_name(
        doctor.collect_serve_config_checks(
            ServeConfig(home=tmp_path, host="10.0.0.5", port=18080, token="operator-token")
        )
    )

    assert checks["advertise URL"].status == "fail"
    assert "not-a-url" in checks["advertise URL"].detail


def test_serve_knob_inventory_stays_aligned_with_dataclass_fields():
    public_fields = tuple(
        field.name for field in fields(ServeConfig) if field.metadata.get("internal") is not True
    )

    assert SERVE_CONFIG_KEYS == public_fields
    assert set(ENV_KEYS) == set(public_fields)
    assert set(CLI_OPTIONS) == set(public_fields)
    assert len(set(ENV_KEYS.values())) == len(ENV_KEYS)
    assert len(set(CLI_OPTIONS.values())) == len(CLI_OPTIONS)
    validate_knob_inventory()
