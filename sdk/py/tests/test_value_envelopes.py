from __future__ import annotations
import gzip
import hashlib

from dataclasses import dataclass, replace
from datetime import datetime, timezone

import pytest

from vmon.values import (
    ABIMismatchError,
    CodecMismatchError,
    EnvelopeIntegrityError,
    ArtifactPayload,
    ValueCodec,
    ValueCompression,
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


def test_gzip_is_deterministic_and_checks_raw_and_stored_payloads_separately():
    value = {"payload": "x" * 10_000}
    first = encode_value(value, compression="gzip", inline_threshold=8)
    second = encode_value(value, compression="gzip", inline_threshold=8)
    assert first == second
    assert first.compression is ValueCompression.GZIP
    assert first.inline is None
    assert first.artifact is not None
    raw = gzip.decompress(first.artifact.data)
    assert first.sha256 == hashlib.sha256(raw).hexdigest()
    assert first.artifact.sha256 == hashlib.sha256(first.artifact.data).hexdigest()
    assert first.sha256 != first.artifact.sha256
    assert decode_value(first) == value

    with pytest.raises(EnvelopeIntegrityError, match="stored-payload checksum"):
        ArtifactPayload(first.artifact.sha256, first.artifact.size, first.artifact.data[:-1] + b"!")
    with pytest.raises(EnvelopeIntegrityError, match="uncompressed value checksum"):
        decode_value(replace(first, sha256="0" * 64))


def test_compressed_inline_tampering_and_zstd_round_trip():
    gzip_envelope = encode_value("x" * 10_000, compression="gzip", inline_threshold=100_000)
    assert gzip_envelope.inline is not None
    tampered = replace(
        gzip_envelope,
        inline=gzip_envelope.inline[:-1] + bytes([gzip_envelope.inline[-1] ^ 1]),
    )
    with pytest.raises(EnvelopeIntegrityError):
        decode_value(tampered)

    zstd_envelope = encode_value({"portable": [1, 2, 3]}, compression="zstd")
    assert zstd_envelope.compression is ValueCompression.ZSTD
    assert decode_value(zstd_envelope) == {"portable": [1, 2, 3]}
