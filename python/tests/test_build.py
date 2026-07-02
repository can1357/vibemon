"""Daemonless Dockerfile build tests."""

from __future__ import annotations

import io
import json
import tarfile
from pathlib import Path

import pytest

import vmon.build as build
import vmon.image as image
from vmon.client import DaemonError


def test_detect_backend_prefers_buildah_over_docker(monkeypatch):
    seen: list[str] = []

    def which(name: str) -> str | None:
        seen.append(name)
        return {"buildah": "/usr/bin/buildah", "docker": "/usr/bin/docker"}.get(name)

    monkeypatch.setattr(build.platform, "system", lambda: "Linux")
    monkeypatch.setattr(build.shutil, "which", which)

    assert build._detect_backend() == ("buildah", "/usr/bin/buildah")
    assert seen == ["buildah"]


def test_build_image_reports_unsupported_when_no_backend(monkeypatch, tmp_path):
    dockerfile = tmp_path / "Dockerfile"
    context = tmp_path / "ctx"
    dockerfile.write_text("FROM scratch\n", encoding="utf-8")
    context.mkdir()

    monkeypatch.setattr(build.platform, "system", lambda: "Linux")
    monkeypatch.setattr(build.shutil, "which", lambda _name: None)
    monkeypatch.setattr(
        build,
        "_run",
        lambda _cmd: (_ for _ in ()).throw(AssertionError("builder must not run")),
    )

    with pytest.raises(DaemonError) as exc:
        build.build_image(dockerfile, context, "test:latest")

    assert exc.value.code == "unsupported"
    message = exc.value.message
    assert "Dockerfile builds require buildah" in message
    assert "docker buildx" in message
    assert "install buildah or Docker with buildx enabled" in message


def test_buildah_build_pushes_daemonless_oci_layout(monkeypatch, tmp_path):
    dockerfile = tmp_path / "Dockerfile"
    context = tmp_path / "ctx"
    dockerfile.write_text("FROM scratch\n", encoding="utf-8")
    context.mkdir()
    home = tmp_path / "home"
    calls: list[list[str]] = []

    def fake_run(cmd):
        cmd = list(cmd)
        calls.append(cmd)
        if cmd[:2] == ["/usr/bin/buildah", "push"]:
            destination = cmd[-1]
            assert destination.startswith("oci:")
            layout_s, pushed_tag = destination.removeprefix("oci:").rsplit(":", 1)
            assert pushed_tag == "vmon-test"
            layout = Path(layout_s)
            (layout / "blobs").mkdir(parents=True)
            (layout / "index.json").write_text('{"schemaVersion":2}\n', encoding="utf-8")
            (layout / "blobs" / "layer").write_bytes(b"layer")

    monkeypatch.setenv("VMON_HOME", str(home))
    monkeypatch.setattr(build.platform, "system", lambda: "Linux")
    monkeypatch.setattr(
        build.shutil,
        "which",
        lambda name: {"buildah": "/usr/bin/buildah", "docker": "/usr/bin/docker"}.get(name),
    )
    monkeypatch.setattr(build, "_run", fake_run)

    ref = build.build_image(dockerfile, context, "vmon-test", arch="x86_64")

    assert calls[0] == [
        "/usr/bin/buildah",
        "build",
        "--format",
        "oci",
        "--platform",
        "linux/amd64",
        "-f",
        str(dockerfile),
        "-t",
        "vmon-test",
        str(context),
    ]
    assert calls[1][0:3] == ["/usr/bin/buildah", "push", "vmon-test"]
    assert calls[1][3].startswith("oci:")
    assert calls[1][3].endswith(":vmon-test")

    layout = Path(ref.removeprefix("oci:").rsplit(":", 1)[0])
    assert ref.startswith(f"oci:{(home / 'builds').resolve()}/")
    assert ref.endswith(":vmon-test")
    assert (layout / "index.json").read_text(encoding="utf-8") == '{"schemaVersion":2}\n'


def test_docker_fallback_buildx_exports_and_unpacks_oci_layout(monkeypatch, tmp_path):
    dockerfile = tmp_path / "Dockerfile"
    context = tmp_path / "ctx"
    dockerfile.write_text("FROM scratch\n", encoding="utf-8")
    context.mkdir()
    home = tmp_path / "home"
    calls: list[list[str]] = []

    def fake_run(cmd):
        cmd = list(cmd)
        calls.append(cmd)
        output = cmd[cmd.index("-o") + 1]
        assert output.startswith("type=oci,dest=")
        archive = Path(output.removeprefix("type=oci,dest="))
        archive.parent.mkdir(parents=True, exist_ok=True)
        data = b'{"schemaVersion":2}\n'
        with tarfile.open(archive, "w") as tar:
            info = tarfile.TarInfo("index.json")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
            layer = b"docker-layer"
            layer_info = tarfile.TarInfo("blobs/layer")
            layer_info.size = len(layer)
            tar.addfile(layer_info, io.BytesIO(layer))

    monkeypatch.setenv("VMON_HOME", str(home))
    monkeypatch.setattr(build.platform, "system", lambda: "Linux")
    monkeypatch.setattr(
        build.shutil,
        "which",
        lambda name: {"docker": "/usr/bin/docker"}.get(name),
    )
    monkeypatch.setattr(build, "_run", fake_run)

    ref = build.build_image(dockerfile, context, "docker-test", arch="aarch64")

    assert len(calls) == 1
    command = calls[0]
    assert command[:3] == ["/usr/bin/docker", "buildx", "build"]
    assert command[command.index("--platform") + 1] == "linux/arm64"
    assert command[command.index("-f") + 1] == str(dockerfile)
    assert command[command.index("-t") + 1] == "docker-test"
    output = command[command.index("-o") + 1]
    assert output.startswith("type=oci,dest=")
    assert output.endswith("layout.tar")
    assert command[-1] == str(context)

    layout = Path(ref.removeprefix("oci:").rsplit(":", 1)[0])
    assert ref.startswith(f"oci:{(home / 'builds').resolve()}/")
    assert ref.endswith(":docker-test")
    assert (layout / "index.json").read_text(encoding="utf-8") == '{"schemaVersion":2}\n'


def test_dockerfile_prepare_passes_built_oci_ref_to_skopeo_inspect(monkeypatch, tmp_path):
    dockerfile = tmp_path / "Dockerfile"
    context = tmp_path / "ctx"
    layout = tmp_path / "layout"
    dockerfile.write_text("FROM scratch\n", encoding="utf-8")
    context.mkdir()
    layout.mkdir()
    oci_ref = f"oci:{layout.resolve()}:built"
    build_calls: list[tuple[Path, Path, str]] = []
    subprocess_calls: list[list[str]] = []

    class Result:
        def __init__(self, stdout: str):
            self.stdout = stdout

    def fake_build_image(dockerfile_arg: Path, context_arg: Path, tag: str) -> str:
        build_calls.append((dockerfile_arg, context_arg, tag))
        return oci_ref

    def fake_run(cmd: list[str], **_kwargs: object) -> Result:
        subprocess_calls.append(cmd)
        if "--config" in cmd:
            return Result(json.dumps({"config": {"Cmd": ["/bin/app"]}}))
        return Result(json.dumps({"Digest": "sha256:abcdef"}))

    monkeypatch.setattr(build, "build_image", fake_build_image)
    monkeypatch.setattr(
        image,
        "detect_image_tools",
        lambda: image.ImageTools("/usr/bin/skopeo", "/usr/bin/umoci"),
    )
    monkeypatch.setattr(image.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(image.subprocess, "run", fake_run)

    prepared = image._prepare_oci_image("built", str(dockerfile), str(context))

    assert build_calls == [(dockerfile, context, "built")]
    assert prepared.reference == oci_ref
    assert prepared.transport_ref == oci_ref
    inspect_commands = [
        cmd for cmd in subprocess_calls if cmd[:2] == ["/usr/bin/skopeo", "inspect"]
    ]
    assert len(inspect_commands) == 2
    for command in inspect_commands:
        assert command[command.index("--override-os") + 1] == "linux"
        assert command[command.index("--override-arch") + 1] == "amd64"
        assert command[-1] == oci_ref


def test_export_oci_image_uses_oci_ref_as_skopeo_source(monkeypatch, tmp_path):
    layout = tmp_path / "layout"
    layout.mkdir()
    oci_ref = f"oci:{layout.resolve()}:built"
    work = tmp_path / "work"
    rootfs = tmp_path / "rootfs"
    work.mkdir()
    rootfs.mkdir()
    calls: list[list[str]] = []

    class Result:
        stdout = ""

    def fake_run(cmd: list[str], **_kwargs: object) -> Result:
        calls.append(cmd)
        if cmd[:2] == ["/usr/bin/skopeo", "copy"]:
            return Result()
        if cmd[:2] == ["/usr/bin/umoci", "unpack"]:
            bundle_rootfs = work / "bundle" / "rootfs"
            bundle_rootfs.mkdir(parents=True)
            (bundle_rootfs / "etc-release").write_text("ID=test\n", encoding="utf-8")
            return Result()
        raise AssertionError(f"unexpected command: {cmd}")

    prepared = image.PreparedImage(
        reference=oci_ref,
        transport_ref=oci_ref,
        spec=image.ImageSpec(reference=oci_ref),
        digest="abcdef",
        tools=image.ImageTools(skopeo="/usr/bin/skopeo", umoci="/usr/bin/umoci"),
        arch="amd64",
    )
    monkeypatch.setattr(image.os, "geteuid", lambda: 501, raising=False)
    monkeypatch.setattr(image.subprocess, "run", fake_run)

    image._export_oci_image(prepared, rootfs, work)

    copy_cmd = next(cmd for cmd in calls if cmd[:2] == ["/usr/bin/skopeo", "copy"])
    unpack_cmd = next(cmd for cmd in calls if cmd[:2] == ["/usr/bin/umoci", "unpack"])
    assert copy_cmd[copy_cmd.index("--override-arch") + 1] == "amd64"
    assert oci_ref in copy_cmd
    assert f"oci:{work / 'oci'}:latest" in copy_cmd
    assert "--rootless" in unpack_cmd
    assert (rootfs / "etc-release").read_text(encoding="utf-8") == "ID=test\n"
