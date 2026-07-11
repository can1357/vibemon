from __future__ import annotations

import gzip
import hashlib
import json
import sys
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


class ValueCompression(StrEnum):
    NONE = "none"
    GZIP = "gzip"
    ZSTD = "zstd"


@dataclass(frozen=True, slots=True)
class ArtifactPayload:
    sha256: str
    size: int
    data: bytes

    def __post_init__(self) -> None:
        if self.size != len(self.data):
            raise EnvelopeIntegrityError("artifact size does not match its stored payload")
        if hashlib.sha256(self.data).hexdigest() != self.sha256:
            raise EnvelopeIntegrityError("artifact stored-payload checksum mismatch")


@dataclass(frozen=True, slots=True)
class ValueEnvelope:
    version: int
    codec: ValueCodec
    codec_version: str
    python_abi: str | None
    compression: ValueCompression
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
        try:
            object.__setattr__(self, "codec", ValueCodec(self.codec))
        except ValueError as exc:
            raise ValueErrorBase(f"unsupported value codec: {self.codec}") from exc
        try:
            object.__setattr__(self, "compression", ValueCompression(self.compression))
        except ValueError as exc:
            raise ValueErrorBase(f"unsupported compression: {self.compression}") from exc

    @property
    def payload(self) -> bytes:
        return self.inline if self.inline is not None else self.artifact.data  # type: ignore[union-attr]


def encode_value(
    value: object,
    *,
    codec: ValueCodec | Literal["auto", "json", "cbor", "cloudpickle"] = "auto",
    compress: bool | None = None,
    compression: ValueCompression | Literal["none", "gzip", "zstd"] | None = None,
    compression_threshold: int = 1024,
    inline_threshold: int = 512 * 1024,
) -> ValueEnvelope:
    """Encode a value into a checksummed, optionally compressed transport envelope."""
    if compression_threshold < 0:
        raise ValueErrorBase("compression_threshold must be non-negative")
    if inline_threshold < 0:
        raise ValueErrorBase("inline_threshold must be non-negative")
    if compress is not None and compression is not None:
        raise ValueErrorBase("compress and compression cannot both be supplied")
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

    if compression is not None:
        selected_compression = ValueCompression(compression)
    elif compress is True:
        selected_compression = ValueCompression.GZIP
    elif compress is False:
        selected_compression = ValueCompression.NONE
    else:
        selected_compression = (
            ValueCompression.GZIP if len(raw) >= compression_threshold else ValueCompression.NONE
        )
    encoded = _compress(raw, selected_compression)
    raw_digest = hashlib.sha256(raw).hexdigest()
    codec_version = _codec_version(chosen)
    python_abi = PYTHON_ABI if chosen is ValueCodec.CLOUDPICKLE else None
    if len(encoded) <= inline_threshold:
        return ValueEnvelope(
            version=1,
            codec=chosen,
            codec_version=codec_version,
            python_abi=python_abi,
            compression=selected_compression,
            sha256=raw_digest,
            uncompressed_size=len(raw),
            inline=encoded,
        )
    return ValueEnvelope(
        version=1,
        codec=chosen,
        codec_version=codec_version,
        python_abi=python_abi,
        compression=selected_compression,
        sha256=raw_digest,
        uncompressed_size=len(raw),
        artifact=ArtifactPayload(
            hashlib.sha256(encoded).hexdigest(),
            len(encoded),
            encoded,
        ),
    )


def decode_value(
    envelope: ValueEnvelope,
    *,
    trusted: bool = False,
    expected_codec: ValueCodec | Literal["json", "cbor", "cloudpickle"] | None = None,
) -> object:
    """Verify and decode a value envelope, requiring explicit trust for cloudpickle."""
    if expected_codec is not None and envelope.codec is not ValueCodec(expected_codec):
        raise CodecMismatchError(
            f"value codec mismatch: expected={ValueCodec(expected_codec).value}, "
            f"artifact={envelope.codec.value}"
        )
    raw = _decompress(envelope.payload, envelope.compression)
    if len(raw) != envelope.uncompressed_size:
        raise EnvelopeIntegrityError("uncompressed value size mismatch")
    if hashlib.sha256(raw).hexdigest() != envelope.sha256:
        raise EnvelopeIntegrityError("uncompressed value checksum mismatch")

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


def _compress(raw: bytes, compression: ValueCompression) -> bytes:
    if compression is ValueCompression.NONE:
        return raw
    if compression is ValueCompression.GZIP:
        return gzip.compress(raw, compresslevel=9, mtime=0)
    import zstandard

    return zstandard.ZstdCompressor(level=19, threads=0, write_checksum=True).compress(raw)


def _decompress(payload: bytes, compression: ValueCompression) -> bytes:
    if compression is ValueCompression.NONE:
        return payload
    if compression is ValueCompression.GZIP:
        try:
            return gzip.decompress(payload)
        except (gzip.BadGzipFile, EOFError, OSError) as exc:
            raise EnvelopeIntegrityError("invalid compressed value payload") from exc
    import zstandard

    try:
        return zstandard.ZstdDecompressor().decompress(payload)
    except zstandard.ZstdError as exc:
        raise EnvelopeIntegrityError("invalid compressed value payload") from exc


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
