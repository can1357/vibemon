from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone

import pytest

from vmon.values import (
    ABIMismatchError,
    CodecMismatchError,
    EnvelopeIntegrityError,
    ValueCodec,
    decode_value,
    encode_value,
)


def test_auto_is_json_first_and_preserves_json_values():
    value = {
        "bool": True,
        "int": 2**53 + 1,
        "list": [None, "héllo", 1.25],
        "nested": {"key": "value"},
    }
    envelope = encode_value(value, compression_threshold=10_000)
    assert envelope.codec is ValueCodec.JSON
    assert envelope.python_abi is None
    assert decode_value(envelope) == value
    assert encode_value(value, compression_threshold=10_000) == envelope


def test_explicit_cbor_round_trips_portable_non_json_value():
    value = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    envelope = encode_value(value, codec="cbor", compress=True)
    assert envelope.codec is ValueCodec.CBOR
    assert decode_value(envelope) == value
    with pytest.raises(CodecMismatchError, match="expected=json"):
        decode_value(envelope, expected_codec="json")


def test_cloudpickle_is_explicit_and_decode_requires_trust():
    @dataclass
    class CustomValue:
        amount: int

    value = CustomValue(8)
    envelope = encode_value(value, codec="cloudpickle")
    assert envelope.codec is ValueCodec.CLOUDPICKLE
    with pytest.raises(PermissionError, match="trusted=True"):
        decode_value(envelope)
    assert decode_value(envelope, trusted=True) == value
    with pytest.raises(ABIMismatchError, match="ABI mismatch"):
        decode_value(replace(envelope, python_abi="cp999"), trusted=True)
    with pytest.raises(CodecMismatchError, match="codec mismatch"):
        decode_value(replace(envelope, codec_version="0.0"), trusted=True)


def test_compression_checksum_and_artifact_representation():
    envelope = encode_value(
        {"payload": "x" * 10_000},
        compress=True,
        inline_threshold=8,
    )
    assert envelope.inline is None
    assert envelope.artifact is not None
    assert envelope.artifact.data == envelope.payload
    assert decode_value(envelope) == {"payload": "x" * 10_000}

    corrupted = replace(envelope, sha256="0" * 64)
    with pytest.raises(EnvelopeIntegrityError, match="checksum"):
        decode_value(corrupted)
