import hashlib
import json
import struct
from pathlib import Path

import pytest


def _write_elf64(path: Path, p_type: int) -> Path:
    ident = b"\x7fELF" + bytes([2, 1, 1]) + b"\x00" * 9
    header = struct.pack(
        "<16sHHIQQQIHHHHHH",
        ident,
        2,  # ET_EXEC
        62,  # EM_X86_64
        1,
        0,
        64,  # e_phoff immediately after ELF header
        0,
        0,
        64,
        56,
        1,
        0,
        0,
        0,
    )
    phdr = struct.pack("<IIQQQQQQ", p_type, 0, 0, 0, 0, 0, 0, 0)
    path.write_bytes(header + phdr)
    return path


def test_is_static_elf_detects_pt_interp(tmp_path):
    from vmon.image import _is_static_elf

    dynamic = _write_elf64(tmp_path / "dynamic-agent", 3)
    static = _write_elf64(tmp_path / "static-agent", 1)
    text = tmp_path / "not-elf"
    text.write_text("hello", encoding="utf-8")

    assert _is_static_elf(dynamic) is False
    assert _is_static_elf(static) is True
    assert _is_static_elf(text) is False


def test_find_agent_binary_validates_mvm_agent_override(monkeypatch, tmp_path):
    from vmon.image import find_agent_binary

    dynamic = _write_elf64(tmp_path / "dynamic-agent", 3)
    dynamic.chmod(0o755)
    static = _write_elf64(tmp_path / "static-agent", 1)
    static.chmod(0o755)

    monkeypatch.setenv("VMON_AGENT", str(dynamic))
    with pytest.raises(RuntimeError, match="must be a static \\(musl\\) binary"):
        find_agent_binary()

    monkeypatch.setenv("VMON_AGENT", str(static))
    assert find_agent_binary() == static


def test_first_static_skips_dynamic_and_non_elf(tmp_path):
    from vmon.image import _first_static

    dynamic = _write_elf64(tmp_path / "dynamic-agent", 3)
    static = _write_elf64(tmp_path / "static-agent", 1)
    text = tmp_path / "not-elf"
    text.write_text("hello", encoding="utf-8")
    for p in (dynamic, static, text):
        p.chmod(0o755)

    # A stray dynamically linked (or non-ELF) build must not mask a usable
    # static one earlier-or-later in the candidate list.
    assert _first_static([text, dynamic, static]) == static
    assert _first_static([text, dynamic]) is None
    assert _first_static([tmp_path / "missing"]) is None


def test_first_static_accepts_non_executable(tmp_path):
    from vmon.image import _first_static

    # Wheel/package-data installs can drop the +x bit on the bundled agent;
    # it must still be selected (the rootfs copy is chmod'd at inject time).
    static = _write_elf64(tmp_path / "bundled-agent", 1)
    static.chmod(0o644)
    assert _first_static([static]) == static


def test_find_agent_binary_expands_user_override(monkeypatch, tmp_path):
    from vmon.image import find_agent_binary

    home = tmp_path / "home"
    agent = home / "bin" / "agent"
    agent.parent.mkdir(parents=True)
    _write_elf64(agent, 1)

    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("VMON_AGENT", "~/bin/agent")

    assert find_agent_binary() == agent


def test_find_agent_binary_reports_missing_arch_and_checked_asset(monkeypatch, tmp_path):
    import vmon.image as image

    fake_pkg = tmp_path / "pkg" / "vmon" / "image.py"
    monkeypatch.setattr(image, "__file__", str(fake_pkg))
    monkeypatch.setattr(image.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(image.shutil, "which", lambda _name: None)
    monkeypatch.delenv("VMON_AGENT", raising=False)
    monkeypatch.delenv("CARGO_TARGET_DIR", raising=False)

    with pytest.raises(RuntimeError, match=r"x86_64.*vmon-agent-x86_64"):
        image.find_agent_binary()


def test_build_or_pull_normalizes_and_rejects_image_refs(monkeypatch):
    import vmon.image as image

    calls: list[tuple[list[str], dict[str, object]]] = []

    class Result:
        returncode = 0

    def fake_run(cmd: list[str], **kwargs: object) -> Result:
        calls.append((cmd, kwargs))
        return Result()

    monkeypatch.setattr(image.subprocess, "run", fake_run)

    assert image.build_or_pull(" alpine:latest ", None, engine="docker") == (
        "docker",
        "alpine:latest",
    )
    assert calls[0][0] == ["docker", "image", "inspect", "alpine:latest"]

    with pytest.raises(ValueError, match="must not contain whitespace"):
        image.build_or_pull("bad ref", None, engine="docker")


def test_template_marker_current_requires_matching_kernel_sha(tmp_path):
    import vmon.image as image

    kernel_sha = "current-kernel-sha"
    marker = tmp_path / "agent-ready.json"
    marker.write_text(
        json.dumps(
            {
                "boot_version": image._TEMPLATE_BOOT_VERSION,
                "kernel_sha": kernel_sha,
            }
        ),
        encoding="utf-8",
    )

    assert image._template_marker_current(marker, kernel_sha) is True

    marker.write_text(
        json.dumps(
            {
                "boot_version": image._TEMPLATE_BOOT_VERSION,
                "kernel_sha": "previous-kernel-sha",
            }
        ),
        encoding="utf-8",
    )
    assert image._template_marker_current(marker, kernel_sha) is False

    marker.write_text(
        json.dumps({"boot_version": image._TEMPLATE_BOOT_VERSION}),
        encoding="utf-8",
    )
    assert image._template_marker_current(marker, kernel_sha) is False

    assert image._template_marker_current(tmp_path / "missing.json", kernel_sha) is False

    marker.write_text("{", encoding="utf-8")
    assert image._template_marker_current(marker, kernel_sha) is False


def test_ensure_kernel_expands_home_and_normalizes_arch(monkeypatch, tmp_path):
    import vmon.assets as assets

    home = tmp_path / "home"
    data = b"kernel"
    digest = hashlib.sha256(data).hexdigest()
    cached = home / "vmon-state" / "assets" / "Image"
    cached.parent.mkdir(parents=True)
    cached.write_bytes(data)

    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("VMON_HOME", "~/vmon-state")
    monkeypatch.setattr(
        assets, "_KERNELS", {"aarch64": ("Image", "https://example.invalid/kernel", digest)}
    )

    assert assets.ensure_kernel("arm64") == cached
