import re

import pytest


def test_volume_name_validation():
    from vmon.volume import Volume

    for name in ["a", "abc123", "a-b", "a.b", "a_b", "a" * 64]:
        assert Volume(name).name == name

    for name in ["", "A", "-a", ".a", "a/b", "a b", "a" * 65]:
        with pytest.raises(ValueError):
            Volume(name)


def test_volume_paths_ensure_lock_context_and_list(mvm_home):
    from vmon.volume import Volume

    vol = Volume("data")
    assert vol.host_dir == mvm_home / "volumes" / "data"
    assert not vol.host_dir.exists()
    assert vol.ensure() == vol.host_dir
    assert vol.host_dir.is_dir()

    vol.acquire()
    try:
        with pytest.raises(RuntimeError):
            Volume("data").acquire()
    finally:
        vol.release()

    second = Volume("data")
    second.acquire()
    second.release()

    with Volume("ctx") as ctx:
        assert ctx.host_dir.is_dir()
        with pytest.raises(RuntimeError):
            Volume("ctx").acquire()
    after_context = Volume("ctx")
    after_context.acquire()
    after_context.release()

    Volume("other").ensure()
    assert Volume.list() == ["ctx", "data", "other"]


def test_volume_tag_sanitizes_truncates_and_uses_vmm_charset():
    from vmon.volume import Volume

    assert Volume("a-b").tag == "a_b"
    assert Volume("MiXeD".lower()).tag == "mixed"
    long_tag = Volume("a" * 64).tag
    assert len(long_tag) <= 32
    assert re.fullmatch(r"[a-z0-9_]{1,32}", long_tag)
