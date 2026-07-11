from __future__ import annotations

import json

import pytest

from vmon.secret import Secret, merge_secrets


def test_secret_from_dict_coerces_values_and_names_are_sorted() -> None:
    secret = Secret.from_dict({"B": 2, 1: True}, name="runtime")

    assert secret.env == {"B": "2", "1": "True"}
    assert secret.names() == ["1", "B"]
    assert secret.to_wire() == {"name": "runtime", "values": {"B": "2", "1": "True"}}


def test_secret_from_env_captures_only_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRESENT_SECRET", "yes")
    monkeypatch.delenv("ABSENT_SECRET", raising=False)

    secret = Secret.from_env("PRESENT_SECRET", "ABSENT_SECRET", name="host")

    assert secret.as_env() == {"PRESENT_SECRET": "yes"}
    assert secret.to_wire() == {"name": "host", "values": {"PRESENT_SECRET": "yes"}}


def test_secret_repr_and_names_do_not_expose_values() -> None:
    secret = Secret.from_dict({"TOKEN": "super-secret-value"}, name="runtime")

    rendered = repr(secret)

    assert "TOKEN" in rendered
    assert "runtime" in rendered
    assert "super-secret-value" not in rendered


def test_as_env_returns_a_copy() -> None:
    secret = Secret.from_dict({"TOKEN": "value"})
    env = secret.as_env()

    env["TOKEN"] = "changed"

    assert secret.env["TOKEN"] == "value"


def test_secret_wire_mapping_round_trips_through_json() -> None:
    secret = Secret.from_dict({"TOKEN": "value", 1: True}, name="runtime")

    payload = json.loads(json.dumps(secret.to_wire()))

    assert payload == {"name": "runtime", "values": {"TOKEN": "value", "1": "True"}}
    assert Secret.from_dict(payload["values"], name=payload["name"]).as_env() == secret.as_env()


def test_merge_secrets_later_wins_and_rejects_bad_items() -> None:
    merged = merge_secrets(
        [
            Secret.from_dict({"A": "one", "B": "old"}),
            {"B": "new", "C": 3},
        ]
    )

    assert merged == {"A": "one", "B": "new", "C": "3"}

    with pytest.raises(TypeError):
        merge_secrets([Secret.from_dict({"A": "one"}), object()])


def test_secret_rejects_invalid_names_and_nul_values() -> None:
    for name in ["", "A=B", "A\x00B"]:
        with pytest.raises(ValueError):
            Secret.from_dict({name: "value"})

    with pytest.raises(ValueError):
        Secret.from_dict({"TOKEN": "bad\x00value"})
    with pytest.raises(ValueError):
        Secret.from_env("BAD=NAME")
    with pytest.raises(ValueError):
        Secret.from_dict({"TOKEN": "value"}, name="BAD=NAME")
    with pytest.raises(ValueError):
        merge_secrets([{"BAD=NAME": "value"}])
    with pytest.raises(ValueError):
        Secret({"BAD=NAME": "value"})

    mutated = Secret.from_dict({"TOKEN": "value"})
    mutated.env["BAD=NAME"] = "value"
    with pytest.raises(ValueError):
        mutated.as_env()
    with pytest.raises(ValueError):
        merge_secrets([mutated])
