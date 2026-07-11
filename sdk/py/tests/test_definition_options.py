from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest

from vmon._function_proto import artifact_ref, options_to_proto
from vmon.image import Image, ImageError
from vmon.options import (
    BatchingPolicy,
    ConcurrencyPolicy,
    CpuArchitecture,
    FunctionOptions,
    FunctionVolumeMount,
    HighAvailabilityPolicy,
    NetworkPolicy,
    OptionsError,
    ResourcePolicy,
    RetryPolicy,
    SerializerPolicy,
    TimeoutPolicy,
    WorkerPolicy,
)
from vmon.package import package_callable
from vmon.remote import RemoteFunction
from vmon.values import ValueCodec


def _provenance_target(value: int) -> int:
    return value + 1


def _other_provenance_target(value: int) -> int:
    return value + 2


def test_function_specs_record_stable_complete_reproducibility() -> None:
    package = package_callable(_provenance_target)
    function = RemoteFunction(_provenance_target)
    spec = function._make_spec(package, artifact_ref(package.sha256), {})
    repeated = function._make_spec(package, artifact_ref(package.sha256), {})

    assert spec.reproducibility == repeated.reproducibility
    assert spec.reproducibility.build_inputs_digest.value == bytes.fromhex(
        function.options.canonical_digest(package)
    )
    assert spec.reproducibility.builder_id == "vmon.python.function"
    assert spec.reproducibility.builder_version == "1"
    assert spec.reproducibility.source_date_epoch == 315_532_800
    assert spec.reproducibility.environment == {
        "PACKAGE_MODE": "zip",
        "PYTHON_ABI": package.manifest.python_abi,
    }

    changed_options = function.with_options(
        replace(function.options, resources=ResourcePolicy(memory_bytes=2 << 30))
    )
    changed_spec = changed_options._make_spec(package, artifact_ref(package.sha256), {})
    other_package = package_callable(_other_provenance_target)
    other_spec = RemoteFunction(_other_provenance_target)._make_spec(
        other_package,
        artifact_ref(other_package.sha256),
        {},
    )
    assert (
        changed_spec.reproducibility.build_inputs_digest != spec.reproducibility.build_inputs_digest
    )
    assert (
        other_spec.reproducibility.build_inputs_digest != spec.reproducibility.build_inputs_digest
    )


def test_images_are_immutable_declarations_and_side_effect_free(tmp_path):
    missing = tmp_path / "does-not-exist"
    base = Image.python("3.14")
    image = (
        base.uv_sync(project=missing)
        .uv_install("httpx==0.28.1", "cbor2")
        .apt_install("git", "curl")
        .env({"B": "2", "A": "1"})
        .run_commands("python -VV")
        .add_local_file(missing / "config.toml", "/etc/demo/config.toml")
        .add_local_python_source(missing, destination="/opt/src")
    )
    assert base.steps == ()
    assert len(image.steps) == 7
    assert image.steps[3].values == ("A", "1", "B", "2")
    assert image.digest == replace(image).digest
    assert not missing.exists()
    with pytest.raises(FrozenInstanceError):
        image.steps = ()


def test_all_image_sources_and_validation_are_declarative(tmp_path):
    assert Image.from_registry("ghcr.io/acme/python:latest").source.kind == "registry"
    docker = Image.from_dockerfile(tmp_path / "Missingfile", context=tmp_path / "missing")
    assert docker.source.kind == "dockerfile"
    revision = "a" * 64
    assert Image.from_template("base-small", revision=revision).source.kind == "template"
    with pytest.raises(ImageError, match="immutable"):
        Image.from_template("base-small")
    assert not (tmp_path / "missing").exists()
    with pytest.raises(ImageError):
        Image.python("latest")
    with pytest.raises(ImageError):
        Image.python().add_local_file("x", "relative/path")


def test_image_local_inputs_are_content_addressed_and_server_typed(tmp_path):
    context = tmp_path / "context"
    context.mkdir()
    (context / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
    config = tmp_path / "config.toml"
    config.write_text("value = 1\n", encoding="utf-8")
    source = tmp_path / "source"
    source.mkdir()
    (source / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    image = (
        Image.from_dockerfile("Dockerfile", context=context)
        .uv_sync(project=source)
        .run_commands("python -VV")
        .add_local_file(config, "/etc/demo/config.toml")
        .add_local_python_source(source)
    )
    artifacts = image.artifacts()
    refs = {item.key: artifact_ref(item.sha256) for item in artifacts}
    spec = options_to_proto(FunctionOptions(image=image), refs)["image"]

    assert spec.dockerfile.context == refs["dockerfile_context"]
    assert spec.dockerfile.dockerfile_path == "Dockerfile"
    assert [list(command.argv) for command in spec.commands] == [
        [
            "python",
            "-m",
            "uv",
            "sync",
            "--project",
            f"/opt/vmon/projects/{refs['step:0'].digest.value.hex()}",
            "--frozen",
        ],
        ["python", "-VV"],
    ]
    assert [mount.artifact for mount in spec.local_artifact_mounts] == [
        refs["step:0"],
        refs["step:2"],
        refs["step:3"],
    ]
    before = image.digest
    (source / "module.py").write_text("VALUE = 2\n", encoding="utf-8")
    assert image.digest != before


@pytest.mark.parametrize(("slim", "variant"), [(True, "slim"), (False, "bookworm")])
def test_python_image_variants_are_server_valid(slim, variant):
    spec = options_to_proto(FunctionOptions(image=Image.python(slim=slim)))["image"]
    assert spec.python.python_version == "3.14"
    assert spec.python.variant == variant


@pytest.mark.parametrize(
    ("factory", "message"),
    [
        (lambda: RetryPolicy(max_attempts=0), "max_attempts"),
        (lambda: RetryPolicy(initial_backoff=0), "initial_backoff"),
        (lambda: RetryPolicy(initial_backoff=2, max_backoff=1), "max_backoff"),
        (lambda: RetryPolicy(backoff_multiplier=0.5), "backoff_multiplier"),
        (lambda: RetryPolicy(retryable_codes=("busy", "busy")), "duplicates"),
        (lambda: TimeoutPolicy(startup=0), "startup"),
        (lambda: TimeoutPolicy(execution=float("inf")), "execution"),
        (lambda: TimeoutPolicy(queue=-1), "queue"),
        (lambda: TimeoutPolicy(graceful_shutdown=0), "graceful_shutdown"),
        (lambda: TimeoutPolicy(result_ttl=0), "result_ttl"),
        (lambda: WorkerPolicy(min_workers=-1), "min_workers"),
        (lambda: WorkerPolicy(min_workers=2, max_workers=1), "min_workers"),
        (lambda: WorkerPolicy(idle_timeout=0), "idle_timeout"),
        (lambda: WorkerPolicy(max_calls_per_worker=-1), "max_calls_per_worker"),
        (lambda: WorkerPolicy(buffer_workers=-1), "buffer_workers"),
        (lambda: WorkerPolicy(max_workers=1, buffer_workers=2), "buffer_workers"),
        (lambda: WorkerPolicy(max_outstanding_inputs=0), "max_outstanding_inputs"),
        (lambda: ConcurrencyPolicy(max_concurrent_calls=0), "max_concurrent_calls"),
        (lambda: ConcurrencyPolicy(serialize_actor_calls=1), "serialize_actor_calls"),
        (lambda: BatchingPolicy(max_batch_size=0), "max_batch_size"),
        (lambda: BatchingPolicy(enabled=True), "max_batch_size"),
        (lambda: BatchingPolicy(enabled=True, max_batch_size=2), "max_wait"),
        (lambda: BatchingPolicy(max_wait=-1), "max_wait"),
        (lambda: ResourcePolicy(cpu_millis=0), "cpu_millis"),
        (lambda: ResourcePolicy(memory_bytes=0), "memory_bytes"),
        (lambda: ResourcePolicy(ephemeral_disk_bytes=0), "ephemeral_disk_bytes"),
        (lambda: ResourcePolicy(architecture="sparc"), "architecture"),
        (lambda: ResourcePolicy(high_availability="rack"), "high_availability"),
        (lambda: FunctionVolumeMount("data", "relative"), "mount_path"),
        (lambda: FunctionVolumeMount("", "/data"), "volume"),
        (
            lambda: ResourcePolicy(
                volume_mounts=(
                    FunctionVolumeMount("one", "/data"),
                    FunctionVolumeMount("two", "/data"),
                )
            ),
            "unique",
        ),
        (lambda: NetworkPolicy(egress_cidrs=("10.0.0.1/24",)), "canonical"),
        (lambda: NetworkPolicy(inbound_cidrs=("not-a-cidr",)), "invalid"),
        (lambda: NetworkPolicy(egress_domains=("HTTP://EXAMPLE.COM",)), "invalid"),
        (
            lambda: NetworkPolicy(block_network=True, egress_cidrs=("10.0.0.0/24",)),
            "cannot be combined",
        ),
        (
            lambda: SerializerPolicy(
                input_serializer=ValueCodec.CLOUDPICKLE,
                allow_trusted_python=False,
            ),
            "allow_trusted_python",
        ),
        (lambda: SerializerPolicy(compression="snappy"), "compression"),
    ],
)
def test_every_policy_boundary_is_validated(factory, message):
    with pytest.raises(OptionsError, match=message):
        factory()


def test_native_resource_and_network_policies_are_typed_and_immutable():
    network = NetworkPolicy(
        egress_cidrs=("10.0.0.0/24", "2001:db8::/32"),
        egress_domains=("api.example.com", "*.objects.example.com"),
        inbound_cidrs=("192.0.2.0/24",),
    )
    resources = ResourcePolicy(
        architecture=CpuArchitecture.ARM64,
        high_availability=HighAvailabilityPolicy.ZONE,
        volume_mounts=(FunctionVolumeMount("models", "/models", read_only=True),),
        network=network,
    )
    assert resources.network.egress_domains == (
        "api.example.com",
        "*.objects.example.com",
    )
    assert resources.volume_mounts[0].read_only
    with pytest.raises(FrozenInstanceError):
        resources.network = NetworkPolicy(block_network=True)


def test_canonical_definition_digest_covers_code_lock_image_and_every_policy():
    options = FunctionOptions()
    baseline = options.canonical_digest(b"code-v1", lockfiles={"uv.lock": b"lock-v1"})
    variants = [
        options.canonical_digest(b"code-v2", lockfiles={"uv.lock": b"lock-v1"}),
        options.canonical_digest(b"code-v1", lockfiles={"uv.lock": b"lock-v2"}),
        replace(options, image=options.image.apt_install("git")).canonical_digest(b"code-v1"),
        replace(options, retry=RetryPolicy(max_attempts=2)).canonical_digest(b"code-v1"),
        replace(options, timeouts=TimeoutPolicy(graceful_shutdown=31)).canonical_digest(b"code-v1"),
        replace(options, workers=WorkerPolicy(max_workers=2, buffer_workers=1)).canonical_digest(
            b"code-v1"
        ),
        replace(options, concurrency=ConcurrencyPolicy(max_concurrent_calls=2)).canonical_digest(
            b"code-v1"
        ),
        replace(
            options,
            batching=BatchingPolicy(enabled=True, max_batch_size=2, max_wait=0.1),
        ).canonical_digest(b"code-v1"),
        replace(
            options,
            serializer=SerializerPolicy(input_serializer=ValueCodec.CBOR),
        ).canonical_digest(b"code-v1"),
        replace(
            options,
            resources=ResourcePolicy(
                architecture=CpuArchitecture.ARM64,
                network=NetworkPolicy(block_network=True),
            ),
        ).canonical_digest(b"code-v1"),
    ]
    assert baseline == options.canonical_digest(b"code-v1", lockfiles={"uv.lock": b"lock-v1"})
    assert len(set(variants)) == len(variants)
    assert baseline not in variants
