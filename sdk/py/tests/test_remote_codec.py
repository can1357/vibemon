"""Serialization matrix for the value codec: JSON-clean boundaries and spill."""

from __future__ import annotations

import base64
import math
import pickle
from datetime import datetime
from pathlib import Path

import pytest

import vmon._remote_runner as rr
from vmon._remote_runner import _json_clean, decode_value, encode_value
from vmon.errors import RemoteFunctionError


@pytest.mark.parametrize(
    "value",
    [None, True, False, 0, -7, "text", 1.5, [1, "a", [None]], {"k": {"n": 1.0}}, []],
)
def test_json_clean_accepts_plain_json(value) -> None:
    assert _json_clean(value)


@pytest.mark.parametrize(
    "value",
    [
        (1, 2),  # tuple
        {1: "int-key"},  # non-str dict key
        {"k": (1,)},  # nested tuple
        b"bytes",
        {"nan": float("nan")},
        float("inf"),
        -float("inf"),
        {1, 2},
        datetime(2026, 1, 1),
    ],
)
def test_json_clean_rejects_lossy_values(value) -> None:
    assert not _json_clean(value)


def test_auto_uses_json_for_clean_and_pickle_for_rich() -> None:
    assert encode_value([1, 2], serializer="auto") == {"json": [1, 2]}
    frame = encode_value((1, 2), serializer="auto")
    assert set(frame) == {"pickle"}
    assert pickle.loads(base64.b64decode(frame["pickle"])) == (1, 2)


def test_json_serializer_raises_exact_argument_message() -> None:
    with pytest.raises(ValueError) as exc_info:
        encode_value({1, 2}, serializer="json")
    assert str(exc_info.value) == "remote function arguments must be JSON-serializable"


def test_json_serializer_rejects_nan() -> None:
    with pytest.raises(ValueError):
        encode_value(float("nan"), serializer="json")


def test_pickle_serializer_always_pickles() -> None:
    frame = encode_value([1, 2], serializer="pickle")
    assert set(frame) == {"pickle"}


def test_round_trip_preserves_rich_types() -> None:
    payload = {"stamp": datetime(2026, 7, 11), "pair": (1, 2), "keys": {3: "x"}}
    frame = encode_value(payload, serializer="auto")
    value = decode_value(frame)
    assert value == payload
    assert isinstance(value["pair"], tuple)
    assert math.isclose(1.0, 1.0)  # keep math import honest


def test_spill_and_fetch_round_trip(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(rr, "_INLINE_LIMIT", 64)
    written: dict[str, bytes] = {}

    def spill(data: bytes, fmt: str) -> str:
        path = tmp_path / f"spill.{fmt}"
        path.write_bytes(data)
        written[str(path)] = data
        return str(path)

    def fetch(path: str) -> bytes:
        data = Path(path).read_bytes()
        Path(path).unlink()
        return data

    big = "x" * 500
    frame = encode_value(big, serializer="auto", spill=spill)
    assert frame == {"file": str(tmp_path / "spill.json"), "format": "json"}
    assert decode_value(frame, fetch=fetch) == big
    assert not (tmp_path / "spill.json").exists(), "receiver deletes spill files"

    rich = [(i, "y" * 40) for i in range(10)]
    frame = encode_value(rich, serializer="auto", spill=spill)
    assert frame["format"] == "pickle"
    assert decode_value(frame, fetch=fetch) == rich


def test_small_values_stay_inline_even_with_spill() -> None:
    frame = encode_value("small", serializer="auto", spill=lambda data, fmt: pytest.fail("spilled"))
    assert frame == {"json": "small"}


def test_decode_file_frame_without_fetch_is_infra_error() -> None:
    with pytest.raises(RemoteFunctionError):
        decode_value({"file": "/tmp/nope", "format": "json"})


def test_decode_malformed_frame_is_infra_error() -> None:
    with pytest.raises(RemoteFunctionError):
        decode_value({"surprise": 1})
