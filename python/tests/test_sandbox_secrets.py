import io
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

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


def _sample_remote_sum(x: int, y: int = 0) -> dict[str, int]:
    print("remote ran")
    return {"sum": x + y}


def _sample_remote_non_json() -> set[int]:
    return {1, 2}


class _CompletedFakeProcess:
    def __init__(self, rc: int = 0, stdout: bytes = b"", stderr: bytes = b"") -> None:
        self._rc = rc
        self.stdout = io.BytesIO(stdout)
        self.stderr = io.BytesIO(stderr)

    def wait(self, timeout: float | None = None) -> int:
        return self._rc


class _RunnerFakeProcess:
    def __init__(self, runner: str) -> None:
        self._runner = runner
        self._stdin = bytearray()
        self._rc: int | None = None
        self.stdout = io.BytesIO()
        self.stderr = io.BytesIO()

    def write_stdin(self, data: bytes | bytearray | memoryview | str) -> None:
        raw = data.encode() if isinstance(data, str) else bytes(data)
        self._stdin.extend(raw)

    def close_stdin(self) -> None:
        completed = subprocess.run(
            [sys.executable, "-c", self._runner],
            input=bytes(self._stdin),
            capture_output=True,
            check=False,
        )
        self._rc = completed.returncode
        self.stdout = io.BytesIO(completed.stdout)
        self.stderr = io.BytesIO(completed.stderr)

    def wait(self, timeout: float | None = None) -> int:
        return int(self._rc if self._rc is not None else 0)


class _RemoteFakeFilesystem:
    def __init__(self) -> None:
        self.writes: dict[str, str] = {}

    def write_text(self, path: str, text: str, encoding: str = "utf-8", mode: int = 0o644) -> None:
        self.writes[path] = text


class _RemoteFakeSandbox:
    def __init__(self, sandbox_mod) -> None:
        self._sandbox_mod = sandbox_mod
        self.filesystem = _RemoteFakeFilesystem()
        self.execs: list[tuple[tuple[str, ...], dict[str, object]]] = []
        self.terminated = False

    def poll(self) -> int | None:
        return None

    def exec(self, *cmd: str, **kwargs: object):
        self.execs.append((tuple(cmd), dict(kwargs)))
        if tuple(cmd) == ("python3", "--version"):
            return _CompletedFakeProcess(stdout=b"Python 3.14\n")
        return _RunnerFakeProcess(self._sandbox_mod._REMOTE_FUNCTION_RUNNER)

    def terminate(self) -> None:
        self.terminated = True


def test_remote_function_runner_executes_source_function() -> None:
    import vmon.sandbox as sandbox_mod

    payload = sandbox_mod._remote_function_payload(_sample_remote_sum, (2,), {"y": 5})
    completed = subprocess.run(
        [sys.executable, "-c", sandbox_mod._REMOTE_FUNCTION_RUNNER],
        input=payload,
        capture_output=True,
        check=True,
    )
    assert sandbox_mod._remote_function_result(completed.stdout) == {"sum": 7}


def test_remote_function_runner_reports_non_json_result() -> None:
    import vmon.sandbox as sandbox_mod

    payload = sandbox_mod._remote_function_payload(_sample_remote_non_json, (), {})
    completed = subprocess.run(
        [sys.executable, "-c", sandbox_mod._REMOTE_FUNCTION_RUNNER],
        input=payload,
        capture_output=True,
        check=True,
    )
    with pytest.raises(sandbox_mod.RemoteFunctionError) as exc:
        sandbox_mod._remote_function_result(completed.stdout)
    assert exc.value.remote_type == "TypeError"
    assert "JSON-serializable" in str(exc.value)


def test_remote_function_rejects_closures() -> None:
    import vmon.sandbox as sandbox_mod

    captured = 3

    def inner() -> int:
        return captured

    with pytest.raises(ValueError, match="cannot close over"):
        sandbox_mod._remote_function_payload(inner, (), {})


def test_function_decorator_remote_uses_cached_python_sandbox(monkeypatch) -> None:
    import vmon.sandbox as sandbox_mod

    created: list[dict[str, object]] = []
    sandboxes: list[_RemoteFakeSandbox] = []

    def fake_create(**kwargs: object) -> _RemoteFakeSandbox:
        created.append(dict(kwargs))
        sandbox = _RemoteFakeSandbox(sandbox_mod)
        sandboxes.append(sandbox)
        return sandbox

    monkeypatch.setattr(sandbox_mod.Sandbox, "create", staticmethod(fake_create))

    remote = sandbox_mod.function(block_network=True)(_sample_remote_sum)

    assert remote.remote(4, y=6) == {"sum": 10}
    assert remote.remote(1, y=2) == {"sum": 3}
    assert created == [{"image": "python:3.14-slim", "block_network": True}]
    assert len(sandboxes) == 1
    assert sandboxes[0].execs[0][0] == ("python3", "--version")
    assert sandboxes[0].execs[1][0] == ("python3", "-u", "/tmp/vmon-function-runner.py")
    assert "/tmp/vmon-function-runner.py" in sandboxes[0].filesystem.writes


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


def test_linux_networked_create_warm_restores_and_replays_net_config(monkeypatch, mvm_home):
    sandbox_mod = _install_fakes(monkeypatch, mvm_home)

    # Force the Linux TAP path regardless of the test host OS.
    monkeypatch.setattr(sandbox_mod.platform, "system", lambda: "Linux")

    tap_slots: list[bool] = []
    real_resolve = sandbox_mod.Sandbox._resolve_template

    def spy_resolve(**kwargs):
        tap_slots.append(bool(kwargs.get("tap_slot")))
        return real_resolve(**kwargs)

    monkeypatch.setattr(sandbox_mod.Sandbox, "_resolve_template", staticmethod(spy_resolve))

    guest_config = {
        "tap": "vmon0",
        "guest_ip": "10.42.0.2",
        "host_ip": "10.42.0.1",
        "prefix": 30,
        "dns": ["1.1.1.1"],
    }

    class _FakeNet:
        def __init__(self) -> None:
            self.guest_config = guest_config
            self.torn = False

        def teardown(self) -> None:
            self.torn = True

    fake_net = _FakeNet()
    monkeypatch.setattr(sandbox_mod, "_setup_sandbox_network", lambda name, **kwargs: fake_net)

    sb = sandbox_mod.Sandbox.create(image="img", block_network=False)
    try:
        # A default (networked) Linux create resolves the tap-slot template and
        # warm-restores it onto a per-sandbox TAP instead of cold fresh-booting.
        assert tap_slots == [True]
        assert FakeMicroVM.booted == []
        assert FakeMicroVM.restored, "networked create should warm-restore the tap-slot template"
        assert FakeMicroVM.restored[-1][2].get("tap") == "vmon0"
        # The per-sandbox guest network config is replayed on the warm sandbox.
        cfg = FakeMicroVM.restored[-1][3]._agent.net_configs
        assert cfg, "net_config was not replayed on the warm networked sandbox"
        assert cfg[-1]["args"] == ("10.42.0.2", 30, "10.42.0.1", ["1.1.1.1"])
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


def _install_prewarm_fakes(monkeypatch, mvm_home: Path):
    """Fakes exercising the real ``prewarm`` -> ``create`` pool-claim key path.

    Only ``cached_template`` is faked, and its snapshot dir is keyed by the
    network-slot flavor exactly as the real builder is (block-network, macOS
    user-NAT, and Linux TAP templates are distinct dirs). Both ``prewarm`` and
    ``Sandbox._resolve_template`` route through it, so the key one registers and
    the key the other looks up are compared for real rather than pinned to a
    constant. ``WarmPool`` returns a claimable stub recording one clone per key.
    """
    import vmon.sandbox as sandbox_mod

    FakeMicroVM.restored = []
    FakeMicroVM.booted = []
    monkeypatch.setattr(sandbox_mod, "MicroVM", FakeMicroVM)

    def fake_cached_template(
        image, *, nic_slot=False, tap_slot=False, host_slot=False, fs_slots=0, **_kw
    ):
        if nic_slot:
            flavor = "nic"
        elif tap_slot:
            flavor = "tap"
        elif host_slot:
            flavor = "host"
        elif fs_slots:
            flavor = f"fs{fs_slots}"
        else:
            flavor = "plain"
        safe = str(image).replace("/", "_").replace(":", "_")
        snapshot_dir = mvm_home / "templates" / f"{safe}-{flavor}"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "rootfs.img").touch()
        return SimpleNamespace(
            snapshot_dir=snapshot_dir,
            spec=SimpleNamespace(reference=image, workdir=None, env_dict=lambda: {}),
        )

    monkeypatch.setattr(sandbox_mod, "cached_template", fake_cached_template)

    clones: dict[str, FakeMicroVM] = {}

    def fake_warmpool(template_dir, count):
        key = str(template_dir)
        clone = FakeMicroVM(f"pool-{Path(key).name}", sandbox_mod.STATE)
        pool = _FakePool(clone)
        pool.size = int(count)
        clones[key] = clone
        return pool

    monkeypatch.setattr(sandbox_mod, "WarmPool", fake_warmpool)
    return sandbox_mod, clones


def test_prewarm_block_network_pool_claimed_on_linux(monkeypatch, mvm_home):
    sandbox_mod, clones = _install_prewarm_fakes(monkeypatch, mvm_home)
    monkeypatch.setattr(sandbox_mod.platform, "system", lambda: "Linux")
    try:
        sandbox_mod.prewarm("img", count=1)
        # Linux prewarms the block-network flavor; a block_network=True create
        # resolves the same key and claims the pooled clone (no restore/boot).
        sb = sandbox_mod.Sandbox.create(image="img", block_network=True)
        assert FakeMicroVM.restored == []
        assert FakeMicroVM.booted == []
        assert sb.vm is clones[str(mvm_home / "templates" / "img-plain")]
        sb.terminate()
    finally:
        sandbox_mod.shutdown_all_pools()


def test_prewarm_pool_not_claimed_by_networked_linux_create(monkeypatch, mvm_home):
    sandbox_mod, _clones = _install_prewarm_fakes(monkeypatch, mvm_home)
    monkeypatch.setattr(sandbox_mod.platform, "system", lambda: "Linux")
    fake_net = SimpleNamespace(
        guest_config={
            "tap": "vmon0",
            "guest_ip": "10.42.0.2",
            "host_ip": "10.42.0.1",
            "prefix": 30,
            "dns": ["1.1.1.1"],
        },
        teardown=lambda: None,
    )
    monkeypatch.setattr(sandbox_mod, "_setup_sandbox_network", lambda name, **kw: fake_net)
    try:
        pool = sandbox_mod.prewarm("img", count=1)
        # A networked (default) Linux sandbox needs a per-sandbox TAP the pool
        # cannot bake in, so the create warm-restores onto a fresh TAP instead of
        # claiming the prewarmed block-network clone.
        sb = sandbox_mod.Sandbox.create(image="img", block_network=False)
        assert pool.hits == 0
        assert FakeMicroVM.restored, "networked Linux create should warm-restore"
        assert FakeMicroVM.restored[-1][2].get("tap") == "vmon0"
        sb.terminate()
    finally:
        sandbox_mod.shutdown_all_pools()


def test_prewarm_pool_claimed_by_default_create_on_macos(monkeypatch, mvm_home):
    sandbox_mod, clones = _install_prewarm_fakes(monkeypatch, mvm_home)
    monkeypatch.setattr(sandbox_mod.platform, "system", lambda: "Darwin")
    try:
        sandbox_mod.prewarm("img", count=1)
        # macOS prewarms the user-NAT NIC flavor; the default networked create
        # resolves the same key and claims the pooled clone.
        sb = sandbox_mod.Sandbox.create(image="img")
        assert FakeMicroVM.restored == []
        assert FakeMicroVM.booted == []
        assert sb.vm is clones[str(mvm_home / "templates" / "img-nic")]
        sb.terminate()
    finally:
        sandbox_mod.shutdown_all_pools()
