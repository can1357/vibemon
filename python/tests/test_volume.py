import re
import stat

import pytest


def test_volume_name_validation():
    from vmon.volume import Volume

    for name in ["a", "abc123", "a-b", "a.b", "a_b", "a" * 64]:
        assert Volume(name).name == name

    for name in ["", "A", "-a", ".a", "a/b", "a b", "a" * 65, None]:
        with pytest.raises(ValueError):
            Volume(name)


def test_volume_paths_ensure_lock_context_and_list(mvm_home):
    from vmon.volume import Volume

    vol = Volume("data")
    assert vol.host_dir == mvm_home / "volumes" / "data"
    assert not vol.host_dir.exists()
    assert vol.ensure() == vol.host_dir
    assert vol.host_dir.is_dir()
    assert stat.S_IMODE(vol.host_dir.stat().st_mode) == 0o700

    vol.acquire()
    assert stat.S_IMODE((vol.host_dir / ".lock").stat().st_mode) == 0o600
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


def test_volume_rejects_symlink_paths_and_filters_invalid_entries(mvm_home):
    from vmon.volume import VOLUME_DIR, Volume

    VOLUME_DIR.mkdir()
    outside = mvm_home / "outside"
    outside.mkdir()
    (VOLUME_DIR / "safe").mkdir()
    (VOLUME_DIR / "bad name").mkdir()
    (VOLUME_DIR / "link").symlink_to(outside, target_is_directory=True)

    assert Volume.list() == ["safe"]
    with pytest.raises(ValueError):
        Volume("link").ensure()


def test_warm_pool_teardown_does_not_remove_unowned_vm_dir(tmp_path):
    import vmon.pool as pool_mod

    outside = tmp_path / "outside"
    outside.mkdir()

    class FakeVM:
        dir = outside

        def __init__(self):
            self.stopped = False
            self.removed = False

        def stop(self):
            self.stopped = True

        def remove(self):
            self.removed = True

    vm = FakeVM()
    pool_mod.WarmPool._teardown(vm)

    assert vm.stopped
    assert not vm.removed
    assert outside.exists()

    root_vm = FakeVM()
    root_vm.dir = pool_mod._vmm_mod.STATE / "vms"
    root_vm.dir.mkdir(parents=True)
    pool_mod.WarmPool._teardown(root_vm)
    assert root_vm.stopped
    assert not root_vm.removed


def test_volume_tag_sanitizes_truncates_and_uses_vmm_charset():
    from vmon.volume import Volume

    assert Volume("a-b").tag == "a_b"
    assert Volume("MiXeD".lower()).tag == "mixed"
    long_tag = Volume("a" * 64).tag
    assert len(long_tag) <= 32
    assert re.fullmatch(r"[a-z0-9_]{1,32}", long_tag)
