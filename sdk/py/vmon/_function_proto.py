from __future__ import annotations

import importlib.util
import platform
from typing import Any

from .image import command_argv
from .options import FunctionOptions
from .package import PackageArtifact, SerializedCallable
from .v1 import api_pb2 as pb
from .values import ArtifactPayload, ValueCodec, ValueCompression, ValueEnvelope

_DIGEST_ALGO = pb.DIGEST_ALGORITHM_SHA256
_CODECS = {
    ValueCodec.JSON: pb.VALUE_SERIALIZER_JSON,
    ValueCodec.CBOR: pb.VALUE_SERIALIZER_CBOR,
    ValueCodec.CLOUDPICKLE: pb.VALUE_SERIALIZER_CLOUDPICKLE,
}
_COMPRESS = {
    ValueCompression.NONE: pb.VALUE_COMPRESSION_NONE,
    ValueCompression.GZIP: pb.VALUE_COMPRESSION_GZIP,
    ValueCompression.ZSTD: pb.VALUE_COMPRESSION_ZSTD,
}


def digest(value: str) -> pb.Digest:
    return pb.Digest(algorithm=_DIGEST_ALGO, value=bytes.fromhex(value))


def artifact_ref(sha256: str, size: int = 0) -> pb.ArtifactRef:
    del size
    return pb.ArtifactRef(digest=digest(sha256))


def value_to_proto(value: ValueEnvelope) -> pb.ValueEnvelope:
    message = pb.ValueEnvelope(
        schema_version=value.version,
        serializer=_CODECS[value.codec],
        compression=_COMPRESS[value.compression],
        checksum=digest(value.sha256),
        uncompressed_size_bytes=value.uncompressed_size,
    )
    if value.inline is not None:
        message.inline_data = value.inline
    else:
        assert value.artifact is not None
        message.artifact.CopyFrom(artifact_ref(value.artifact.sha256, value.artifact.size))
    if value.python_abi is not None:
        message.python.CopyFrom(
            pb.PythonCodecMetadata(
                implementation=platform.python_implementation(),
                abi_tag=value.python_abi,
                python_version=platform.python_version(),
                cloudpickle_version=value.codec_version,
            )
        )
    return message


def value_from_proto(
    message: pb.ValueEnvelope, artifact_data: bytes | None = None
) -> ValueEnvelope:
    codecs = {value: key for key, value in _CODECS.items()}
    compress = {value: key for key, value in _COMPRESS.items()}
    storage = message.WhichOneof("storage")
    payload = None
    if storage == "artifact":
        if artifact_data is None:
            raise ValueError("artifact-backed value requires downloaded data")
        payload = ArtifactPayload(
            message.artifact.digest.value.hex(), len(artifact_data), artifact_data
        )
    codec = codecs[message.serializer]
    if codec is ValueCodec.JSON:
        codec_version = "stdlib-json-1"
    elif codec is ValueCodec.CBOR:
        import cbor2

        codec_version = getattr(cbor2, "__version__", "cbor2")
    else:
        codec_version = message.python.cloudpickle_version
    return ValueEnvelope(
        version=message.schema_version,
        codec=codec,
        codec_version=codec_version,
        python_abi=message.python.abi_tag if message.HasField("python") else None,
        compression=compress[message.compression],
        sha256=message.checksum.value.hex(),
        uncompressed_size=message.uncompressed_size_bytes,
        inline=message.inline_data if storage == "inline_data" else None,
        artifact=payload,
    )


def package_to_proto(
    package: PackageArtifact | SerializedCallable,
    source: pb.ArtifactRef,
    *,
    trusted_serialization: bool = False,
) -> pb.PackageSpec:
    included: list[str]
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
    elif trusted_serialization:
        import cloudpickle

        python.cloudpickle_version = cloudpickle.__version__
    return pb.PackageSpec(
        source=source,
        module=module,
        qualname=qualname,
        content_digest=digest(package.sha256),
        mode=mode,
        python=python,
        included_paths=included,
    )


def options_to_proto(
    options: FunctionOptions,
    image_refs: dict[str, pb.ArtifactRef] | None = None,
) -> dict[str, Any]:
    r, t, w, c, b, s, resources = (
        options.retry,
        options.timeouts,
        options.workers,
        options.concurrency,
        options.batching,
        options.serializer,
        options.resources,
    )
    arch = {
        "unspecified": pb.CPU_ARCHITECTURE_UNSPECIFIED,
        "amd64": pb.CPU_ARCHITECTURE_AMD64,
        "arm64": pb.CPU_ARCHITECTURE_ARM64,
    }[resources.architecture.value]
    ha = {
        "unspecified": pb.HIGH_AVAILABILITY_POLICY_UNSPECIFIED,
        "none": pb.HIGH_AVAILABILITY_POLICY_NONE,
        "host": pb.HIGH_AVAILABILITY_POLICY_HOST,
        "zone": pb.HIGH_AVAILABILITY_POLICY_ZONE,
    }[resources.high_availability.value]
    image = pb.ImageSpec()
    image_refs = image_refs or {}
    src = options.image.source
    if src.kind == "python":
        version, _, variant = src.value.partition("-")
        image.python.CopyFrom(pb.PythonImageSource(python_version=version, variant=variant))
    elif src.kind == "registry":
        image.registry.CopyFrom(pb.RegistryImageSource(reference=src.value))
    elif src.kind == "template":
        if src.revision is None:
            raise ValueError("template image requires an immutable SHA-256 revision")
        image.template.CopyFrom(pb.TemplateImageSource(name=src.value, revision=src.revision))
    elif src.kind == "dockerfile":
        ref = image_refs.get("dockerfile_context")
        if ref is None:
            raise ValueError(
                "Dockerfile image context must be uploaded before function registration"
            )
        from pathlib import Path

        context = Path(src.context or ".").resolve()
        dockerfile = Path(src.value)
        if not dockerfile.is_absolute():
            dockerfile = (context / dockerfile).resolve()
        image.dockerfile.CopyFrom(
            pb.DockerfileImageSource(
                context=ref,
                dockerfile_path=dockerfile.relative_to(context).as_posix(),
            )
        )
    for step_index, step in enumerate(options.image.steps):
        if step.kind == "apt_install":
            for item in step.values:
                name, sep, version = item.partition("=")
                image.apt_packages.append(pb.AptPackage(name=name, version=version if sep else ""))
        elif step.kind == "uv_install":
            for item in step.values:
                name, sep, version = item.partition("==")
                image.uv_packages.append(pb.UvPackage(name=name, version=version if sep else ""))
        elif step.kind == "env":
            image.environment.update(dict(zip(step.values[::2], step.values[1::2], strict=True)))
        elif step.kind == "run_commands":
            for command in step.values:
                image.commands.append(pb.ImageBuildCommand(argv=command_argv(command)))
        elif step.kind == "uv_sync":
            ref = image_refs.get(f"step:{step_index}")
            if ref is None:
                raise ValueError("uv_sync project must be uploaded before function registration")
            destination = f"/opt/vmon/projects/{ref.digest.value.hex()}"
            image.local_artifact_mounts.append(
                pb.LocalArtifactMount(artifact=ref, path=destination, read_only=True)
            )
            argv = ["python", "-m", "uv", "sync", "--project", destination]
            if step.values[1] == "frozen":
                argv.append("--frozen")
            for group in step.values[2:]:
                argv.extend(("--group", group))
            image.commands.append(pb.ImageBuildCommand(argv=argv))
        elif step.kind in {"add_local_file", "add_local_python_source"}:
            ref = image_refs.get(f"step:{step_index}")
            if ref is None:
                raise ValueError(f"{step.kind} input must be uploaded before function registration")
            image.local_artifact_mounts.append(
                pb.LocalArtifactMount(artifact=ref, path=step.values[1], read_only=True)
            )
        else:
            raise ValueError(f"unsupported image step {step.kind!r}")
    return dict(
        image=image,
        resources=pb.ResourceSpec(
            cpus=max(1, (resources.cpu_millis + 999) // 1000),
            memory_bytes=resources.memory_bytes,
            ephemeral_disk_bytes=resources.ephemeral_disk_bytes,
            architecture=arch,
            high_availability=ha,
            volume_mounts=[
                pb.FunctionVolumeMount(
                    volume=pb.VolumeRef(name=x.volume),
                    mount_path=x.mount_path,
                    read_only=x.read_only,
                )
                for x in resources.volume_mounts
            ],
            network=pb.NetworkPolicy(
                block_network=resources.network.block_network,
                egress_cidrs=resources.network.egress_cidrs,
                egress_domains=resources.network.egress_domains,
                inbound_cidrs=resources.network.inbound_cidrs,
            ),
        ),
        retry=pb.RetryPolicy(
            max_attempts=r.max_attempts,
            initial_backoff_millis=int(r.initial_backoff * 1000),
            max_backoff_millis=int(r.max_backoff * 1000),
            backoff_multiplier=r.backoff_multiplier,
            retryable_codes=r.retryable_codes,
        ),
        timeouts=pb.TimeoutSpec(
            execution_millis=int(t.execution * 1000),
            queue_millis=int(t.queue * 1000),
            startup_millis=int(t.startup * 1000),
            graceful_shutdown_millis=int(t.graceful_shutdown * 1000),
            result_ttl_millis=int(t.result_ttl * 1000),
        ),
        workers=pb.WorkerSpec(
            min_workers=w.min_workers,
            max_workers=w.max_workers,
            idle_timeout_millis=int(w.idle_timeout * 1000),
            max_calls_per_worker=w.max_calls_per_worker,
            buffer_workers=w.buffer_workers,
            max_outstanding_inputs=w.max_outstanding_inputs,
        ),
        concurrency=pb.ConcurrencySpec(
            max_concurrent_calls=c.max_concurrent_calls,
            serialize_actor_calls=c.serialize_actor_calls,
        ),
        batching=pb.BatchingSpec(
            enabled=b.enabled,
            max_batch_size=b.max_batch_size,
            max_wait_millis=int(b.max_wait * 1000),
        ),
        serializer=pb.SerializerSpec(
            input_serializer=_CODECS[s.input_serializer],
            result_serializer=_CODECS[s.result_serializer],
            compression=_COMPRESS[s.compression],
            allow_trusted_python=s.allow_trusted_python,
        ),
    )
