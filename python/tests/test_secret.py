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


def test_merge_secrets_later_wins_and_rejects_bad_items():
    merged = merge_secrets([
        Secret.from_dict({"A": "one", "B": "old"}),
        {"B": "new", "C": 3},
    ])
    assert merged == {"A": "one", "B": "new", "C": "3"}

    with pytest.raises(TypeError):
        merge_secrets([Secret.from_dict({"A": "one"}), object()])
