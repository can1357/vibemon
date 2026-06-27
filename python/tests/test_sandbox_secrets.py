import io
import json
from pathlib import Path

import pytest


class FakeSession:
    def __init__(self) -> None:
        self.stdout = io.BytesIO(b"")
        self.stderr = io.BytesIO(b"")
        self.stdin = io.BytesIO()
        self.returncode = 0

    def write_stdin(self, data: bytes | bytearray | memoryview) -> None:
        self.stdin.write(bytes(data))

    def close_stdin(self) -> None:
        pass

    def kill(self) -> None:
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        return self.returncode

    def resize(self, rows: int, cols: int) -> None:
        self.rows = rows
        self.cols = cols


class FakeAgentConn:
    def __init__(self) -> None:
        self.closed = False
        self.exec_envs: list[dict[str, str]] = []
        self.mounts: list[tuple[str, str, bool]] = []
        self.net_configs: list[dict[str, object]] = []
        self.ping_timeouts: list[float | None] = []

    def ping(self, timeout: float | None = None) -> dict[str, bool]:
        self.ping_timeouts.append(timeout)
        return {"ok": True}

    def mount(self, tag: str, path: str, ro: bool = False) -> dict[str, bool]:
        self.mounts.append((tag, path, ro))
        return {"ok": True}

    def net_config(self, *args: object, **kwargs: object) -> dict[str, bool]:
        self.net_configs.append({"args": tuple(args), "kwargs": dict(kwargs)})
        return {"ok": True}

    def exec(
        self,
        argv: object,
        cwd: object = None,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
        tty: bool = False,
    ) -> FakeSession:
        self.exec_envs.append(dict(env or {}))
        return FakeSession()

    def close(self) -> None:
        self.closed = True


class FakeControl:
    def extend(self, secs: int) -> dict[str, int]:
        return {"deadline_unix": secs}


class FakeMicroVM:
    restored: list[tuple[object, str | None, dict[str, object], object]] = []
    booted: list[tuple[object, str | None, dict[str, object], object]] = []

    def __init__(self, name: str, root: Path) -> None:
        self.name = name
        self.dir = root / "vms" / name
        self.dir.mkdir(parents=True, exist_ok=True)
        self.rootfs_img = self.dir / "rootfs.img"
        self.control = FakeControl()
        self._agent = FakeAgentConn()
        self.stopped = False

    @classmethod
    def restore(cls, template: object, name: str | None = None, **kwargs: object):
        import vmon.sandbox as sandbox_mod

        vm = cls(name or "sb", sandbox_mod.STATE)
        cls.restored.append((template, name, dict(kwargs), vm))
        return vm

    @classmethod
    def boot_rootfs(cls, rootfs: object, name: str | None = None, **kwargs: object):
        import vmon.sandbox as sandbox_mod

        vm = cls(name or "sb", sandbox_mod.STATE)
        cls.booted.append((rootfs, name, dict(kwargs), vm))
        return vm

    def agent(self, connect_timeout: float | None = None) -> FakeAgentConn:
        return self._agent

    def _save_meta(self, **kw: object) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        (self.dir / "meta.json").write_text(
            json.dumps(kw, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def stop(self, wait: bool = True) -> None:
        self.stopped = True


def _install_fakes(monkeypatch, mvm_home: Path):
    import vmon.sandbox as sandbox_mod

    FakeMicroVM.restored = []
    FakeMicroVM.booted = []
    # A disk-backed template so the fresh-boot path (networked, or block_network
    # with fs_dir/volumes) finds its copy-on-write overlay base.
    tpl = mvm_home / "templates" / "t"
    tpl.mkdir(parents=True, exist_ok=True)
    (tpl / "rootfs.img").touch()
    monkeypatch.setattr(sandbox_mod, "MicroVM", FakeMicroVM)
    monkeypatch.setattr(
        sandbox_mod.Sandbox,
        "_resolve_template",
        staticmethod(lambda **kwargs: (mvm_home / "templates" / "t", None, "t")),
    )
    return sandbox_mod


def test_create_never_persists_secret_values_and_exec_merges_env(monkeypatch, mvm_home):
    sandbox_mod = _install_fakes(monkeypatch, mvm_home)
    from vmon.secret import Secret

    sb = sandbox_mod.Sandbox.create(
        template="t",
        secrets=[Secret.from_dict({"FOO": "sekret-value"})],
        env={"PLAIN": "ok"},
        pool_size=0,
        block_network=True,
    )
    vm = FakeMicroVM.restored[-1][3]
    assert vm._agent.ping_timeouts == [300]

    for path in vm.dir.rglob("*"):
        if path.is_file():
            assert "sekret-value" not in path.read_text(encoding="utf-8", errors="ignore")

    meta = json.loads((vm.dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["secret_names"] == ["FOO"]
    assert meta["env_names"] == ["PLAIN"]
    assert "env" not in meta
    assert "sekret-value" not in json.dumps(meta)

    sb.exec("true")
    assert vm._agent.exec_envs[-1]["FOO"] == "sekret-value"
    assert vm._agent.exec_envs[-1]["PLAIN"] == "ok"
    sb.terminate()


def test_volume_tag_collisions_get_unique_tags(monkeypatch, mvm_home):
    sandbox_mod = _install_fakes(monkeypatch, mvm_home)
    from vmon.volume import Volume

    # A template-based create cold-boots even with volumes: a pre-built template
    # snapshot has no provisioned slots to rebind, so virtio-fs devices are
    # enumerated by a fresh boot. (Image-built requests warm-restore; see below.)
    sb = sandbox_mod.Sandbox.create(
        template="t",
        volumes={"/one": Volume("a-b"), "/two": Volume("a_b")},
        pool_size=0,
        block_network=True,
    )
    boot_kwargs = FakeMicroVM.booted[-1][2]
    assert FakeMicroVM.booted[-1][3]._agent.ping_timeouts == [300]
    tags = [tag for tag, _host_dir, _read_only in boot_kwargs["volumes"]]

    assert tags == ["a_b", "a_b_2"]
    assert len(tags) == len(set(tags))
    sb.terminate()


def test_image_volume_request_warm_restores_with_slot_tags(monkeypatch, mvm_home):
    sandbox_mod = _install_fakes(monkeypatch, mvm_home)
    from vmon.image import slot_tag
    from vmon.volume import Volume

    resolved_slots: list[int] = []
    real_resolve = sandbox_mod.Sandbox._resolve_template

    def spy_resolve(**kwargs):
        resolved_slots.append(int(kwargs.get("fs_slots", -1)))
        return real_resolve(**kwargs)

    monkeypatch.setattr(sandbox_mod.Sandbox, "_resolve_template", staticmethod(spy_resolve))

    # An image-built block_network request with volumes warm-restores from a
    # slot-provisioned template instead of cold-booting.
    sb = sandbox_mod.Sandbox.create(
        image="img",
        volumes={"/one": Volume("a-b"), "/two": Volume("c_d")},
        pool_size=0,
        block_network=True,
    )
    # The provisioned template is requested with exactly one slot per volume.
    assert resolved_slots == [2]
    # Warm restore was used, never a cold boot.
    assert FakeMicroVM.booted == []
    restore_kwargs = FakeMicroVM.restored[-1][2]
    tags = [tag for tag, _host, _ro in restore_kwargs["volumes"]]
    assert tags == [slot_tag(0), slot_tag(1)]
    # Each slot tag is mounted at its requested mountpoint in the guest.
    mounts = FakeMicroVM.restored[-1][3]._agent.mounts
    assert {(t, p) for t, p, _ro in mounts} == {(slot_tag(0), "/one"), (slot_tag(1), "/two")}
    sb.terminate()


def test_create_rejects_invalid_resource_args_before_template_resolution(monkeypatch, mvm_home):
    sandbox_mod = _install_fakes(monkeypatch, mvm_home)

    def fail_resolve(**kwargs):
        raise AssertionError("_resolve_template should not run for invalid resources")

    monkeypatch.setattr(sandbox_mod.Sandbox, "_resolve_template", staticmethod(fail_resolve))
    with pytest.raises(ValueError):
        sandbox_mod.Sandbox.create(template="t", cpus=0, block_network=True)
    with pytest.raises(ValueError):
        sandbox_mod.Sandbox.create(template="t", memory=0, block_network=True)
    with pytest.raises(ValueError):
        sandbox_mod.Sandbox.create(template="t", disk_mb=0, block_network=True)
    with pytest.raises(ValueError):
        sandbox_mod.Sandbox.create(template="t", timeout_secs=86_401, block_network=True)


def test_sandbox_exec_layers_image_caller_and_secret_env(monkeypatch, mvm_home):
    sandbox_mod = _install_fakes(monkeypatch, mvm_home)
    from vmon.image import ImageSpec

    spec = ImageSpec(reference="img", env=["PATH=/usr/bin:/bin", "HOME=/root"])
    vm = FakeMicroVM("sb", mvm_home)
    sb = sandbox_mod.Sandbox(vm, image_spec=spec, env={"PATH": "/custom/bin"})
    sb._secret_env = {"TOKEN": "s3cret"}

    sb.exec("true")
    seen = vm._agent.exec_envs[-1]
    assert seen["HOME"] == "/root"
    assert seen["PATH"] == "/custom/bin"
    assert seen["TOKEN"] == "s3cret"


def test_macos_networked_create_warm_restores_and_replays_net_config(monkeypatch, mvm_home):
    sandbox_mod = _install_fakes(monkeypatch, mvm_home)
    from vmon import net

    # Force the macOS user-net path regardless of the test host OS.
    monkeypatch.setattr(sandbox_mod.platform, "system", lambda: "Darwin")

    nic_slots: list[bool] = []
    real_resolve = sandbox_mod.Sandbox._resolve_template

    def spy_resolve(**kwargs):
        nic_slots.append(bool(kwargs.get("nic_slot")))
        return real_resolve(**kwargs)

    monkeypatch.setattr(sandbox_mod.Sandbox, "_resolve_template", staticmethod(spy_resolve))

    sb = sandbox_mod.Sandbox.create(image="img", block_network=False)
    try:
        # A default (networked) macOS create resolves the nic-slot template and
        # warm-restores it instead of cold fresh-booting.
        assert nic_slots == [True]
        assert FakeMicroVM.booted == []
        assert FakeMicroVM.restored, "networked create should warm-restore the nic-slot template"
        # net_config is replayed positionally with the fixed user-net layout.
        cfg = FakeMicroVM.restored[-1][3]._agent.net_configs
        assert cfg, "net_config was not replayed on the warm networked sandbox"
        assert cfg[-1]["args"] == (
            net.USER_NET_GUEST_IP,
            net.USER_NET_PREFIX,
            net.USER_NET_GATEWAY,
            list(net.USER_NET_DNS),
        )
    finally:
        sb.terminate()


class _FakePool:
    size = 1

    def __init__(self, clone: FakeMicroVM) -> None:
        self._clone = clone
        self.hits = 0

    def claim(self) -> FakeMicroVM | None:
        self.hits += 1
        return self._clone

    def stats(self) -> dict[str, int]:
        return {"hits": self.hits, "misses": 0, "ready_count": 0}

    def shutdown(self) -> None:
        return None


def test_named_pool_claim_renames_clone(monkeypatch, mvm_home):
    sandbox_mod = _install_fakes(monkeypatch, mvm_home)

    clone = FakeMicroVM("pool-clone-auto", sandbox_mod.STATE)
    renamed: dict[str, str] = {}

    def fake_rename(self, new_name: str) -> None:
        renamed["to"] = new_name
        self.name = new_name

    monkeypatch.setattr(FakeMicroVM, "rename", fake_rename, raising=False)
    key = str(mvm_home / "templates" / "t")
    with sandbox_mod._POOLS_LOCK:
        sandbox_mod._POOLS[key] = _FakePool(clone)
        sandbox_mod._POOL_REFS[key] = "img"
    try:
        sb = sandbox_mod.Sandbox.create(image="img", block_network=True, name="my-name")
        # The pooled clone was claimed (not restored/booted) and renamed in place.
        assert FakeMicroVM.restored == []
        assert FakeMicroVM.booted == []
        assert renamed["to"] == "my-name"
        assert sb.vm.name == "my-name"
        sb.terminate()
    finally:
        with sandbox_mod._POOLS_LOCK:
            sandbox_mod._POOLS.pop(key, None)
            sandbox_mod._POOL_REFS.pop(key, None)


def test_macos_networked_pool_claim_replays_net_config(monkeypatch, mvm_home):
    sandbox_mod = _install_fakes(monkeypatch, mvm_home)
    from vmon import net

    monkeypatch.setattr(sandbox_mod.platform, "system", lambda: "Darwin")
    clone = FakeMicroVM("netpool-clone", sandbox_mod.STATE)
    key = str(mvm_home / "templates" / "t")
    with sandbox_mod._POOLS_LOCK:
        sandbox_mod._POOLS[key] = _FakePool(clone)
        sandbox_mod._POOL_REFS[key] = "img"
    try:
        sb = sandbox_mod.Sandbox.create(image="img", block_network=False)
        # A default networked create claims the nic-slot pool (no restore/boot)
        # and replays user-net config on the claimed clone.
        assert FakeMicroVM.restored == []
        assert FakeMicroVM.booted == []
        assert sb.vm is clone
        cfg = clone._agent.net_configs
        assert cfg and cfg[-1]["args"] == (
            net.USER_NET_GUEST_IP,
            net.USER_NET_PREFIX,
            net.USER_NET_GATEWAY,
            list(net.USER_NET_DNS),
        )
        sb.terminate()
    finally:
        with sandbox_mod._POOLS_LOCK:
            sandbox_mod._POOLS.pop(key, None)
            sandbox_mod._POOL_REFS.pop(key, None)
