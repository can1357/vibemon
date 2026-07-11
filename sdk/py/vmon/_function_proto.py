from __future__ import annotations

import hashlib
import importlib.util
import platform
import sys
from typing import Any

from .options import FunctionOptions
from .package import PackageArtifact, SerializedCallable
from .values import ArtifactPayload, ValueCodec, ValueEnvelope
from .v1 import api_pb2 as pb

_DIGEST_ALGO = pb.DIGEST_ALGORITHM_SHA256
_CODECS = {ValueCodec.JSON: pb.VALUE_SERIALIZER_JSON, ValueCodec.CBOR: pb.VALUE_SERIALIZER_CBOR, ValueCodec.CLOUDPICKLE: pb.VALUE_SERIALIZER_CLOUDPICKLE}
_COMPRESS = {"none": pb.VALUE_COMPRESSION_NONE, "zlib": pb.VALUE_COMPRESSION_GZIP}


def digest(value: str) -> pb.Digest:
    return pb.Digest(algorithm=_DIGEST_ALGO, value=bytes.fromhex(value))


def artifact_ref(sha256: str, size: int = 0) -> pb.ArtifactRef:
    return pb.ArtifactRef(digest=digest(sha256), size_bytes=size)


def value_to_proto(value: ValueEnvelope) -> pb.ValueEnvelope:
    message = pb.ValueEnvelope(version=value.version, serializer=_CODECS[value.codec], codec_version=value.codec_version,
        compression=_COMPRESS[value.compression], checksum=digest(value.sha256), uncompressed_size_bytes=value.uncompressed_size)
    if value.inline is not None:
        message.inline_data = value.inline
    else:
        assert value.artifact is not None
        message.artifact.CopyFrom(artifact_ref(value.artifact.sha256, value.artifact.size))
    if value.python_abi is not None:
        message.python.CopyFrom(pb.PythonCodecMetadata(implementation=platform.python_implementation(), version=platform.python_version(), abi_tag=value.python_abi))
    return message


def value_from_proto(message: pb.ValueEnvelope, artifact_data: bytes | None = None) -> ValueEnvelope:
    codecs = {v: k for k, v in _CODECS.items()}; compress = {v: k for k, v in _COMPRESS.items()}
    storage = message.WhichOneof("storage")
    payload = None
    if storage == "artifact":
        if artifact_data is None: raise ValueError("artifact-backed value requires downloaded data")
        payload = ArtifactPayload(message.artifact.digest.value.hex(), message.artifact.size_bytes, artifact_data)
    return ValueEnvelope(version=message.version, codec=codecs[message.serializer], codec_version=message.codec_version,
        python_abi=message.python.abi_tag if message.HasField("python") else None, compression=compress[message.compression],
        sha256=message.checksum.value.hex(), uncompressed_size=message.uncompressed_size_bytes,
        inline=message.inline_data if storage == "inline_data" else None, artifact=payload)


def package_to_proto(
    package: PackageArtifact | SerializedCallable, source: pb.ArtifactRef
) -> pb.PackageSpec:
    if isinstance(package, PackageArtifact):
        module = package.manifest.module
        qualname = package.manifest.qualname
        mode = pb.PACKAGE_MODE_PACKAGE
        codec_version = None
        abi = package.manifest.python_abi
        included = [entry.path for entry in package.manifest.files]
    else:
        module = ""
        qualname = ""
        mode = pb.PACKAGE_MODE_TRUSTED_SERIALIZED
        codec_version = package.codec_version
        abi = package.python_abi
        included = []
    python = pb.PythonCodeMetadata(
        implementation=platform.python_implementation(),
        version=platform.python_version(),
        abi_tag=abi,
        bytecode_magic=importlib.util.MAGIC_NUMBER,
    )
    if codec_version:
        python.cloudpickle_version = codec_version
    return pb.PackageSpec(
        source=source,
        module=module,
        qualname=qualname,
        content_digest=digest(package.sha256),
        mode=mode,
        python=python,
        included_paths=included,
    )


def options_to_proto(options: FunctionOptions) -> dict[str, Any]:
    r, t, w, c, b, s, resources = options.retry, options.timeouts, options.workers, options.concurrency, options.batching, options.serializer, options.resources
    arch = {"unspecified": pb.CPU_ARCHITECTURE_UNSPECIFIED, "amd64": pb.CPU_ARCHITECTURE_AMD64, "arm64": pb.CPU_ARCHITECTURE_ARM64}[resources.architecture.value]
    ha = {"unspecified": pb.HIGH_AVAILABILITY_POLICY_UNSPECIFIED, "none": pb.HIGH_AVAILABILITY_POLICY_NONE, "host": pb.HIGH_AVAILABILITY_POLICY_HOST, "zone": pb.HIGH_AVAILABILITY_POLICY_ZONE}[resources.high_availability.value]
    image = pb.ImageSpec()
    src = options.image.source
    if src.kind == "python":
        version, _, variant = src.value.partition("-"); image.python.CopyFrom(pb.PythonImageSource(python_version=version, variant=variant))
    elif src.kind == "registry": image.registry.CopyFrom(pb.RegistryImageSource(reference=src.value))
    elif src.kind == "template": image.template.CopyFrom(pb.TemplateImageSource(name=src.value))
    # Local Docker contexts and local-file image steps require separate artifact uploads and are rejected here rather than persisted as paths.
    else: raise ValueError("Dockerfile image contexts must be uploaded before function registration")
    for step in options.image.steps:
        if step.kind == "apt_install":
            for item in step.values:
                name, sep, version = item.partition("="); image.apt_packages.append(pb.AptPackage(name=name, version=version if sep else ""))
        elif step.kind == "uv_install":
            for item in step.values:
                name, sep, version = item.partition("=="); image.uv_packages.append(pb.UvPackage(name=name, version=version if sep else ""))
        elif step.kind == "env": image.environment.update(dict(zip(step.values[::2], step.values[1::2])))
        elif step.kind == "run_commands":
            for command in step.values: image.commands.append(pb.ImageBuildCommand(argv=["/bin/sh", "-c", command]))
        else: raise ValueError(f"image step {step.kind!r} requires artifact build support")
    return dict(image=image,
      resources=pb.ResourceSpec(cpu_millis=resources.cpu_millis, memory_bytes=resources.memory_bytes, ephemeral_disk_bytes=resources.ephemeral_disk_bytes, architecture=arch, high_availability=ha,
        volume_mounts=[pb.FunctionVolumeMount(volume=pb.VolumeRef(id=x.volume), mount_path=x.mount_path, read_only=x.read_only) for x in resources.volume_mounts],
        network=pb.NetworkPolicy(block_network=resources.network.block_network, egress_cidrs=resources.network.egress_cidrs, egress_domains=resources.network.egress_domains, inbound_cidrs=resources.network.inbound_cidrs)),
      retry=pb.RetryPolicy(max_attempts=r.max_attempts, initial_backoff_millis=int(r.initial_backoff*1000), max_backoff_millis=int(r.max_backoff*1000), backoff_multiplier=r.backoff_multiplier, retryable_codes=r.retryable_codes),
      timeouts=pb.TimeoutSpec(execution_millis=int(t.execution*1000), queue_millis=int(t.queue*1000), startup_millis=int(t.startup*1000), graceful_shutdown_millis=int(t.graceful_shutdown*1000), result_ttl_millis=int(t.result_ttl*1000)),
      workers=pb.WorkerSpec(min_workers=w.min_workers,max_workers=w.max_workers,idle_timeout_millis=int(w.idle_timeout*1000),max_calls_per_worker=w.max_calls_per_worker,buffer_workers=w.buffer_workers,max_outstanding_inputs=w.max_outstanding_inputs),
      concurrency=pb.ConcurrencySpec(max_concurrent_calls=c.max_concurrent_calls,serialize_actor_calls=c.serialize_actor_calls), batching=pb.BatchingSpec(enabled=b.enabled,max_batch_size=b.max_batch_size,max_wait_millis=int(b.max_wait*1000)),
      serializer=pb.SerializerSpec(input_serializer=_CODECS[s.input_serializer],result_serializer=_CODECS[s.result_serializer],compression=_COMPRESS[s.compression],allow_trusted_python=s.allow_trusted_python))
