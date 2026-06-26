import struct

import pytest


def _write_elf64(path, p_type):
    ident = b"\x7fELF" + bytes([2, 1, 1]) + b"\x00" * 9
    header = struct.pack(
        "<16sHHIQQQIHHHHHH",
        ident,
        2,      # ET_EXEC
        62,     # EM_X86_64
        1,
        0,
        64,     # e_phoff immediately after ELF header
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
    text.write_text("hello")

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
    text.write_text("hello")
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
