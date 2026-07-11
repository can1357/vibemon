from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import os
import re
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Mapping

from .image import Image
from .package import PackageArtifact, SerializedCallable
from .values import ValueCodec, ValueCompression


class OptionsError(ValueError):
    pass


class CpuArchitecture(StrEnum):
    UNSPECIFIED = "unspecified"
    AMD64 = "amd64"
    ARM64 = "arm64"


class HighAvailabilityPolicy(StrEnum):
    UNSPECIFIED = "unspecified"
    NONE = "none"
    HOST = "host"
    ZONE = "zone"


@dataclass(frozen=True, slots=True)
class FunctionVolumeMount:
    volume: str
    mount_path: str
    read_only: bool = False

    def __post_init__(self) -> None:
        _nonempty("volume", self.volume)
        _absolute_path("mount_path", self.mount_path)
        _boolean("read_only", self.read_only)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 1
    initial_backoff: float = 1.0
    max_backoff: float = 60.0
    backoff_multiplier: float = 2.0
    retryable_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _integer("max_attempts", self.max_attempts, minimum=1)
        _positive("initial_backoff", self.initial_backoff)
        _positive("max_backoff", self.max_backoff)
        if self.max_backoff < self.initial_backoff:
            raise OptionsError("max_backoff must be greater than or equal to initial_backoff")
        if not math.isfinite(self.backoff_multiplier) or self.backoff_multiplier < 1:
            raise OptionsError("backoff_multiplier must be finite and at least 1")
        _unique_strings("retryable_codes", self.retryable_codes)


@dataclass(frozen=True, slots=True)
class TimeoutPolicy:
    execution: float = 300.0
    queue: float = 300.0
    startup: float = 60.0
    graceful_shutdown: float = 30.0
    result_ttl: float = 86_400.0

    def __post_init__(self) -> None:
        for name in ("execution", "queue", "startup", "graceful_shutdown", "result_ttl"):
            _positive(name, getattr(self, name))


@dataclass(frozen=True, slots=True)
class WorkerPolicy:
    min_workers: int = 0
    max_workers: int = 1
    idle_timeout: float = 300.0
    max_calls_per_worker: int = 0
    buffer_workers: int = 0
    max_outstanding_inputs: int = 1

    def __post_init__(self) -> None:
        _integer("min_workers", self.min_workers, minimum=0)
        _integer("max_workers", self.max_workers, minimum=1)
        if self.min_workers > self.max_workers:
            raise OptionsError("min_workers cannot exceed max_workers")
        _positive("idle_timeout", self.idle_timeout)
        _integer("max_calls_per_worker", self.max_calls_per_worker, minimum=0)
        _integer("buffer_workers", self.buffer_workers, minimum=0)
        if self.buffer_workers > self.max_workers:
            raise OptionsError("buffer_workers cannot exceed max_workers")
        _integer("max_outstanding_inputs", self.max_outstanding_inputs, minimum=1)


@dataclass(frozen=True, slots=True)
class ConcurrencyPolicy:
    max_concurrent_calls: int = 1
    serialize_actor_calls: bool = False

    def __post_init__(self) -> None:
        _integer("max_concurrent_calls", self.max_concurrent_calls, minimum=1)
        _boolean("serialize_actor_calls", self.serialize_actor_calls)


@dataclass(frozen=True, slots=True)
class BatchingPolicy:
    enabled: bool = False
    max_batch_size: int = 1
    max_wait: float = 0.0

    def __post_init__(self) -> None:
        _boolean("enabled", self.enabled)
        _integer("max_batch_size", self.max_batch_size, minimum=1)
        _nonnegative("max_wait", self.max_wait)
        if self.enabled and self.max_batch_size < 2:
            raise OptionsError("enabled batching requires max_batch_size of at least 2")
        if self.enabled and self.max_wait == 0:
            raise OptionsError("enabled batching requires positive max_wait")
        if not self.enabled and (self.max_batch_size != 1 or self.max_wait != 0):
            raise OptionsError("disabled batching cannot set batch size or wait")


@dataclass(frozen=True, slots=True)
class SerializerPolicy:
    input_serializer: ValueCodec = ValueCodec.JSON
    result_serializer: ValueCodec = ValueCodec.JSON
    compression: ValueCompression = ValueCompression.GZIP
    allow_trusted_python: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "input_serializer", _codec(self.input_serializer, "input_serializer")
        )
        object.__setattr__(
            self, "result_serializer", _codec(self.result_serializer, "result_serializer")
        )
        try:
            object.__setattr__(self, "compression", ValueCompression(self.compression))
        except ValueError as exc:
            raise OptionsError(f"invalid compression: {self.compression!r}") from exc
        _boolean("allow_trusted_python", self.allow_trusted_python)
        if (
            ValueCodec.CLOUDPICKLE in (self.input_serializer, self.result_serializer)
            and not self.allow_trusted_python
        ):
            raise OptionsError("cloudpickle serializers require allow_trusted_python=True")


@dataclass(frozen=True, slots=True)
class NetworkPolicy:
    block_network: bool = False
    egress_cidrs: tuple[str, ...] = ()
    egress_domains: tuple[str, ...] = ()
    inbound_cidrs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _boolean("block_network", self.block_network)
        _cidrs("egress_cidrs", self.egress_cidrs)
        _domains("egress_domains", self.egress_domains)
        _cidrs("inbound_cidrs", self.inbound_cidrs)
        if self.block_network and (self.egress_cidrs or self.egress_domains or self.inbound_cidrs):
            raise OptionsError("block_network cannot be combined with network allowlists")


@dataclass(frozen=True, slots=True)
class ResourcePolicy:
    cpu_millis: int = 1000
    memory_bytes: int = 512 * 1024 * 1024
    ephemeral_disk_bytes: int = 1024 * 1024 * 1024
    architecture: CpuArchitecture = CpuArchitecture.UNSPECIFIED
    high_availability: HighAvailabilityPolicy = HighAvailabilityPolicy.UNSPECIFIED
    volume_mounts: tuple[FunctionVolumeMount, ...] = ()
    network: NetworkPolicy = field(default_factory=NetworkPolicy)

    def __post_init__(self) -> None:
        _integer("cpu_millis", self.cpu_millis, minimum=1)
        _integer("memory_bytes", self.memory_bytes, minimum=1)
        _integer("ephemeral_disk_bytes", self.ephemeral_disk_bytes, minimum=1)
        object.__setattr__(
            self, "architecture", _enum(CpuArchitecture, self.architecture, "architecture")
        )
        object.__setattr__(
            self,
            "high_availability",
            _enum(HighAvailabilityPolicy, self.high_availability, "high_availability"),
        )
        if not isinstance(self.volume_mounts, tuple) or not all(
            isinstance(mount, FunctionVolumeMount) for mount in self.volume_mounts
        ):
            raise OptionsError("volume_mounts must be a tuple of FunctionVolumeMount values")
        mount_paths = [mount.mount_path for mount in self.volume_mounts]
        if len(set(mount_paths)) != len(mount_paths):
            raise OptionsError("volume mount paths must be unique")
        if not isinstance(self.network, NetworkPolicy):
            raise OptionsError("network must be NetworkPolicy")


@dataclass(frozen=True, slots=True)
class FunctionOptions:
    image: Image = field(default_factory=Image.python)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    timeouts: TimeoutPolicy = field(default_factory=TimeoutPolicy)
    workers: WorkerPolicy = field(default_factory=WorkerPolicy)
    concurrency: ConcurrencyPolicy = field(default_factory=ConcurrencyPolicy)
    batching: BatchingPolicy = field(default_factory=BatchingPolicy)
    serializer: SerializerPolicy = field(default_factory=SerializerPolicy)
    resources: ResourcePolicy = field(default_factory=ResourcePolicy)

    def __post_init__(self) -> None:
        expected = (
            ("image", Image),
            ("retry", RetryPolicy),
            ("timeouts", TimeoutPolicy),
            ("workers", WorkerPolicy),
            ("concurrency", ConcurrencyPolicy),
            ("batching", BatchingPolicy),
            ("serializer", SerializerPolicy),
            ("resources", ResourcePolicy),
        )
        for name, kind in expected:
            if not isinstance(getattr(self, name), kind):
                raise OptionsError(f"{name} must be {kind.__name__}")

    def canonical_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["image"] = self.image.canonical_dict()
        serializer = value["serializer"]
        serializer["input_serializer"] = self.serializer.input_serializer.value
        serializer["result_serializer"] = self.serializer.result_serializer.value
        resources = value["resources"]
        resources["architecture"] = self.resources.architecture.value
        resources["high_availability"] = self.resources.high_availability.value
        return value

    def canonical_digest(
        self,
        code: PackageArtifact | SerializedCallable | bytes | str,
        *,
        lockfiles: Mapping[str, bytes | str | os.PathLike[str]] | None = None,
    ) -> str:
        if isinstance(code, (PackageArtifact, SerializedCallable)):
            code_digest = code.sha256
        elif isinstance(code, bytes):
            code_digest = hashlib.sha256(code).hexdigest()
        elif isinstance(code, str):
            code_digest = code if _is_sha256(code) else hashlib.sha256(code.encode()).hexdigest()
        else:
            raise OptionsError("code must be an artifact, bytes, or digest string")
        locks: list[dict[str, str]] = []
        for name, content in sorted((lockfiles or {}).items()):
            if isinstance(content, os.PathLike):
                raw = Path(content).read_bytes()
            elif isinstance(content, bytes):
                raw = content
            elif isinstance(content, str):
                raw = content.encode()
            else:
                raise OptionsError(f"unsupported lockfile content for {name!r}")
            locks.append({"path": name, "sha256": hashlib.sha256(raw).hexdigest()})
        value = {
            "code_sha256": code_digest,
            "lockfiles": locks,
            "options": self.canonical_dict(),
            "version": 1,
        }
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


def _codec(value: ValueCodec | str, name: str) -> ValueCodec:
    try:
        return ValueCodec(value)
    except (TypeError, ValueError) as exc:
        raise OptionsError(f"invalid {name} codec: {value!r}") from exc


def _enum(kind: type[StrEnum], value: object, name: str) -> StrEnum:
    if not isinstance(value, str):
        raise OptionsError(f"invalid {name}: {value!r}")
    try:
        return kind(value)
    except (TypeError, ValueError) as exc:
        raise OptionsError(f"invalid {name}: {value!r}") from exc


def _integer(name: str, value: object, *, minimum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise OptionsError(f"{name} must be an integer greater than or equal to {minimum}")


def _positive(name: str, value: object) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise OptionsError(f"{name} must be a positive finite number")


def _nonnegative(name: str, value: object) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise OptionsError(f"{name} must be a non-negative finite number")


def _boolean(name: str, value: object) -> None:
    if not isinstance(value, bool):
        raise OptionsError(f"{name} must be a bool")


def _nonempty(name: str, value: object) -> None:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise OptionsError(f"{name} must be a non-empty string")


def _absolute_path(name: str, value: object) -> None:
    _nonempty(name, value)
    path = PurePosixPath(value)  # type: ignore[arg-type]
    if not path.is_absolute() or ".." in path.parts:
        raise OptionsError(f"{name} must be an absolute normalized path")


def _unique_strings(name: str, values: object) -> None:
    if not isinstance(values, tuple) or not all(
        isinstance(value, str) and value for value in values
    ):
        raise OptionsError(f"{name} must be a tuple of non-empty strings")
    if len(set(values)) != len(values):
        raise OptionsError(f"{name} must not contain duplicates")


def _cidrs(name: str, values: tuple[str, ...]) -> None:
    _unique_strings(name, values)
    for value in values:
        try:
            network = ipaddress.ip_network(value, strict=False)
        except ValueError as exc:
            raise OptionsError(f"invalid {name} entry: {value!r}") from exc
        if str(network) != value:
            raise OptionsError(f"{name} entries must be canonical CIDRs: {value!r}")


def _domains(name: str, values: tuple[str, ...]) -> None:
    _unique_strings(name, values)
    pattern = re.compile(r"^(?:\*\.)?(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$")
    for value in values:
        if value != value.lower() or not pattern.fullmatch(value):
            raise OptionsError(f"invalid {name} entry: {value!r}")


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)
