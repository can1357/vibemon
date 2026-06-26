import json

import pytest

from vmon.secret import Secret, merge_secrets


def test_secret_from_dict_coerces_values_and_names_are_sorted():
    secret = Secret.from_dict({"B": 2, 1: True})
    assert secret.env == {"B": "2", "1": "True"}
    assert secret.names() == ["1", "B"]


def test_secret_from_env_captures_only_present(monkeypatch):
    monkeypatch.setenv("PRESENT_SECRET", "yes")
    monkeypatch.delenv("ABSENT_SECRET", raising=False)

    secret = Secret.from_env("PRESENT_SECRET", "ABSENT_SECRET")
    assert secret.env == {"PRESENT_SECRET": "yes"}


def test_as_env_returns_a_copy():
    secret = Secret.from_dict({"TOKEN": "value"})
    env = secret.as_env()
    env["TOKEN"] = "changed"
    assert secret.env["TOKEN"] == "value"


def test_secret_mapping_round_trips_through_json():
    secret = Secret.from_dict({"TOKEN": "value", 1: True})
    payload = json.loads(json.dumps(secret.as_env()))
    assert Secret.from_dict(payload).as_env() == secret.as_env()


def test_merge_secrets_later_wins_and_rejects_bad_items():
    merged = merge_secrets(
        [
            Secret.from_dict({"A": "one", "B": "old"}),
            {"B": "new", "C": 3},
        ]
    )
    assert merged == {"A": "one", "B": "new", "C": "3"}

    with pytest.raises(TypeError):
        merge_secrets([Secret.from_dict({"A": "one"}), object()])


def test_secret_rejects_invalid_env_names_and_nul_values():
    for name in ["", "A=B", "A\x00B"]:
        with pytest.raises(ValueError):
            Secret.from_dict({name: "value"})

    with pytest.raises(ValueError):
        Secret.from_dict({"TOKEN": "bad\x00value"})
    with pytest.raises(ValueError):
        Secret.from_env("BAD=NAME")
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
