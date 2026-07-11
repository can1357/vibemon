from __future__ import annotations

import hashlib
import json
import sys
import zlib
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

PYTHON_ABI = sys.implementation.cache_tag or f"py{sys.version_info.major}{sys.version_info.minor}"


class ValueErrorBase(ValueError):
    pass


class EnvelopeIntegrityError(ValueErrorBase):
    pass


class CodecMismatchError(ValueErrorBase):
    pass


class ABIMismatchError(ValueErrorBase):
    pass


class ValueCodec(StrEnum):
    JSON = "json"
    CBOR = "cbor"
    CLOUDPICKLE = "cloudpickle"


@dataclass(frozen=True, slots=True)
class ArtifactPayload:
    sha256: str
    size: int
    data: bytes


@dataclass(frozen=True, slots=True)
class ValueEnvelope:
    version: int
    codec: ValueCodec
    codec_version: str
    python_abi: str | None
    compression: Literal["none", "zlib"]
    sha256: str
    uncompressed_size: int
    inline: bytes | None = None
    artifact: ArtifactPayload | None = None

    def __post_init__(self) -> None:
        if self.version != 1:
            raise ValueErrorBase(f"unsupported value envelope version: {self.version}")
        if (self.inline is None) == (self.artifact is None):
            raise ValueErrorBase("exactly one of inline or artifact is required")
        if self.uncompressed_size < 0:
            raise ValueErrorBase("uncompressed_size must be non-negative")
        if self.artifact is not None:
            if self.artifact.size != len(self.artifact.data):
                raise EnvelopeIntegrityError("artifact size does not match its payload")
            if hashlib.sha256(self.artifact.data).hexdigest() != self.artifact.sha256:
                raise EnvelopeIntegrityError("artifact checksum mismatch")

    @property
    def payload(self) -> bytes:
        return self.inline if self.inline is not None else self.artifact.data  # type: ignore[union-attr]


def encode_value(
    value: object,
    *,
    codec: ValueCodec | Literal["auto", "json", "cbor", "cloudpickle"] = "auto",
    compress: bool | None = None,
    compression_threshold: int = 1024,
    inline_threshold: int = 512 * 1024,
) -> ValueEnvelope:
    if compression_threshold < 0:
        raise ValueErrorBase("compression_threshold must be non-negative")
    if inline_threshold < 0:
        raise ValueErrorBase("inline_threshold must be non-negative")
    chosen: ValueCodec
    if codec == "auto":
        try:
            raw = _encode_json(value)
        except (TypeError, ValueError):
            chosen = ValueCodec.CBOR
            raw = _encode_cbor(value)
        else:
            chosen = ValueCodec.JSON
    else:
        try:
            chosen = ValueCodec(codec)
        except ValueError as exc:
            raise ValueErrorBase(f"unsupported value codec: {codec}") from exc
        if chosen is ValueCodec.JSON:
            raw = _encode_json(value)
        elif chosen is ValueCodec.CBOR:
            raw = _encode_cbor(value)
        else:
            raw = _encode_cloudpickle(value)

    codec_version = _codec_version(chosen)
    should_compress = compress is True or (compress is None and len(raw) >= compression_threshold)
    encoded = zlib.compress(raw, level=9) if should_compress else raw
    compression: Literal["none", "zlib"] = "zlib" if should_compress else "none"
    payload_digest = hashlib.sha256(encoded).hexdigest()
    common = dict(
        version=1,
        codec=chosen,
        codec_version=codec_version,
        python_abi=PYTHON_ABI if chosen is ValueCodec.CLOUDPICKLE else None,
        compression=compression,
        sha256=payload_digest,
        uncompressed_size=len(raw),
    )
    if len(encoded) <= inline_threshold:
        return ValueEnvelope(**common, inline=encoded)
    return ValueEnvelope(
        **common,
        artifact=ArtifactPayload(payload_digest, len(encoded), encoded),
    )


def decode_value(
    envelope: ValueEnvelope,
    *,
    trusted: bool = False,
    expected_codec: ValueCodec | Literal["json", "cbor", "cloudpickle"] | None = None,
) -> object:
    if expected_codec is not None and envelope.codec is not ValueCodec(expected_codec):
        raise CodecMismatchError(
            f"value codec mismatch: expected={ValueCodec(expected_codec).value}, "
            f"artifact={envelope.codec.value}"
        )
    payload = envelope.payload
    if hashlib.sha256(payload).hexdigest() != envelope.sha256:
        raise EnvelopeIntegrityError("value payload checksum mismatch")
    if envelope.compression == "zlib":
        try:
            raw = zlib.decompress(payload)
        except zlib.error as exc:
            raise EnvelopeIntegrityError("invalid compressed value payload") from exc
    elif envelope.compression == "none":
        raw = payload
    else:
        raise ValueErrorBase(f"unsupported compression: {envelope.compression}")
    if len(raw) != envelope.uncompressed_size:
        raise EnvelopeIntegrityError("uncompressed value size mismatch")

    runtime_version = _codec_version(envelope.codec)
    if envelope.codec_version != runtime_version:
        raise CodecMismatchError(
            f"{envelope.codec.value} codec mismatch: artifact={envelope.codec_version}, "
            f"runtime={runtime_version}"
        )
    if envelope.codec is ValueCodec.JSON:
        return json.loads(raw)
    if envelope.codec is ValueCodec.CBOR:
        import cbor2

        return cbor2.loads(raw)
    if not trusted:
        raise PermissionError("cloudpickle decoding requires trusted=True")
    if envelope.python_abi != PYTHON_ABI:
        raise ABIMismatchError(
            f"Python ABI mismatch: artifact={envelope.python_abi}, runtime={PYTHON_ABI}"
        )
    import cloudpickle

    return cloudpickle.loads(raw)


def _encode_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _encode_cbor(value: object) -> bytes:
    import cbor2

    return cbor2.dumps(value, canonical=True)


def _encode_cloudpickle(value: object) -> bytes:
    import cloudpickle

    return cloudpickle.dumps(value, protocol=5)


def _codec_version(codec: ValueCodec) -> str:
    if codec is ValueCodec.JSON:
        return "stdlib-json-1"
    if codec is ValueCodec.CBOR:
        import cbor2

        return getattr(cbor2, "__version__", "cbor2")
    import cloudpickle

    return cloudpickle.__version__
