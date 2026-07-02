import ast
import asyncio
import inspect
import io
import json
import math
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
        self.fs_reads: list[str] = []
        self.fs_writes: list[tuple[str, bytes, int]] = []
        self.fs_storage: dict[str, bytes] = {}
        self.fs_mkdirs: list[tuple[str, bool]] = []
        self.fs_removes: list[tuple[str, bool]] = []

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

    def fs_read(self, path: str) -> bytes:
        self.fs_reads.append(path)
        return self.fs_storage[path]

    def fs_write(self, path: str, data: bytes | bytearray | memoryview, mode: int = 0o644) -> None:
        raw = bytes(data)
        self.fs_writes.append((path, raw, mode))
        self.fs_storage[path] = raw

    def fs_list(self, path: str = ".") -> list[dict[str, object]]:
        return [
            {"path": child, "type": "file"}
            for child in sorted(self.fs_storage)
            if child.startswith(path)
        ]

    def fs_mkdir(self, path: str, parents: bool = True) -> None:
        self.fs_mkdirs.append((path, parents))

    def fs_remove(self, path: str, recursive: bool = False) -> None:
        self.fs_removes.append((path, recursive))
        self.fs_storage.pop(path, None)

    def fs_stat(self, path: str) -> dict[str, object]:
        return {"path": path, "size": len(self.fs_storage[path])}

    def close(self) -> None:
        self.closed = True


class FakeControl:
    def extend(self, secs: int) -> dict[str, int]:
        return {"deadline_unix": secs}

    def metrics(self) -> dict[str, int]:
        return {"vcpu_exits": 42}


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
        self.snapshots: list[tuple[str, bool, Path]] = []

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

    def snapshot(self, name: str, keep_running: bool = True, disk_src: Path | None = None) -> Path:
        disk = disk_src or self.rootfs_img
        self.snapshots.append((name, keep_running, disk))
        snap_dir = self.dir / "snapshots" / name
        snap_dir.mkdir(parents=True, exist_ok=True)
        return snap_dir

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


_REMOTE_FN_CONSTANT = 7


def _identity_decorator(**_kwargs):
    def decorate(fn):
        return fn

    return decorate


@_identity_decorator(
    marker="multi",
)
def _sample_multiline_decorated(x: int) -> int:
    return x + 1


def _remote_fn_helper(value: float) -> int:
    return math.ceil(value) + _REMOTE_FN_CONSTANT


def _sample_remote_uses_module_context(value: float) -> int:
    return _remote_fn_helper(value)


def _rf_param_shadows_import(math: int) -> int:
    # `math` is a parameter, not the module's `import math`.
    return math + 1


def _rf_closure_shadows_const() -> int:
    _REMOTE_FN_CONSTANT = 99  # local shadow of the module constant

    def _inner() -> int:
        return _REMOTE_FN_CONSTANT  # binds the enclosing local, not the module const

    return _inner()


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


def test_remote_function_source_strips_multiline_decorator() -> None:
    import vmon.sandbox as sandbox_mod

    source = sandbox_mod._remote_function_source(_sample_multiline_decorated)
    ast.parse(source)
    payload = sandbox_mod._remote_function_payload(_sample_multiline_decorated, (8,), {})
    completed = subprocess.run(
        [sys.executable, "-c", sandbox_mod._REMOTE_FUNCTION_RUNNER],
        input=payload,
        capture_output=True,
        check=True,
    )
    assert sandbox_mod._remote_function_result(completed.stdout) == 9


def test_remote_function_source_includes_module_context() -> None:
    import vmon.sandbox as sandbox_mod

    source = sandbox_mod._remote_function_source(_sample_remote_uses_module_context)
    assert "import math" in source
    assert "_REMOTE_FN_CONSTANT = 7" in source
    assert "def _remote_fn_helper" in source
    payload = sandbox_mod._remote_function_payload(_sample_remote_uses_module_context, (2.2,), {})
    completed = subprocess.run(
        [sys.executable, "-c", sandbox_mod._REMOTE_FUNCTION_RUNNER],
        input=payload,
        capture_output=True,
        check=True,
    )
    assert sandbox_mod._remote_function_result(completed.stdout) == 10


def test_remote_function_source_excludes_param_shadowed_import() -> None:
    import vmon.sandbox as sandbox_mod

    source = sandbox_mod._remote_function_source(_rf_param_shadows_import)
    # `math` is a parameter; the module's `import math` must not be shipped.
    assert "import math" not in source
    assert "_rf_param_shadows_import" in source


def test_remote_function_source_excludes_closure_shadowed_const() -> None:
    import vmon.sandbox as sandbox_mod

    source = sandbox_mod._remote_function_source(_rf_closure_shadows_const)
    # The inner function captures the enclosing LOCAL constant, so the module
    # constant of the same name must not leak into the shipped source.
    assert "_REMOTE_FN_CONSTANT = 7" not in source
    payload = sandbox_mod._remote_function_payload(_rf_closure_shadows_const, (), {})
    completed = subprocess.run(
        [sys.executable, "-c", sandbox_mod._REMOTE_FUNCTION_RUNNER],
        input=payload,
        capture_output=True,
        check=True,
    )
    assert sandbox_mod._remote_function_result(completed.stdout) == 99


def test_remote_function_result_forwards_captured_stdout(capsys) -> None:
    import vmon.sandbox as sandbox_mod

    payload = sandbox_mod._remote_function_payload(_sample_remote_sum, (2,), {"y": 5})
    completed = subprocess.run(
        [sys.executable, "-c", sandbox_mod._REMOTE_FUNCTION_RUNNER],
        input=payload,
        capture_output=True,
        check=True,
    )
    assert sandbox_mod._remote_function_result(completed.stdout) == {"sum": 7}
    assert capsys.readouterr().out == "remote ran\n"

    raw = json.dumps(
        {
            "ok": False,
            "type": "ValueError",
            "message": "boom",
            "traceback": "",
            "stdout": "before failure\n",
        }
    ).encode()
    with pytest.raises(sandbox_mod.RemoteFunctionError):
        sandbox_mod._remote_function_result(raw)
    assert capsys.readouterr().out == "before failure\n"


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


def test_remote_function_aio_remote_delegates(monkeypatch) -> None:
    import vmon.sandbox as sandbox_mod

    monkeypatch.setattr(
        sandbox_mod.Sandbox,
        "create",
        staticmethod(lambda **kwargs: _RemoteFakeSandbox(sandbox_mod)),
    )
    remote = sandbox_mod.function(block_network=True)(_sample_remote_sum)
    # `fn.aio.{remote,map,starmap}` await the same paths as the sync `fn.*`.
    assert asyncio.run(remote.aio.remote(4, y=6)) == {"sum": 10}
    assert asyncio.run(remote.aio.starmap([(1, 2), (3, 4)])) == [{"sum": 3}, {"sum": 7}]
    ctx = sandbox_mod.function(block_network=True)(_sample_remote_uses_module_context)
    assert asyncio.run(ctx.aio.map([1.1, 2.1])) == [9, 10]


def test_remote_function_map_and_starmap_use_ephemeral_pool(monkeypatch) -> None:
    import vmon.sandbox as sandbox_mod

    created: list[dict[str, object]] = []
    sandboxes: list[_RemoteFakeSandbox] = []

    def fake_create(**kwargs: object) -> _RemoteFakeSandbox:
        created.append(dict(kwargs))
        sandbox = _RemoteFakeSandbox(sandbox_mod)
        sandboxes.append(sandbox)
        return sandbox

    monkeypatch.setattr(sandbox_mod.Sandbox, "create", staticmethod(fake_create))

    remote = sandbox_mod.function(block_network=True)(_sample_remote_uses_module_context)
    assert remote.map([1.1, 2.1, 3.1, 4.1], concurrency=3) == [9, 10, 11, 12]
    assert len(sandboxes) == 3
    assert len(created) == 3
    assert all(kwargs == {"image": "python:3.14-slim", "block_network": True} for kwargs in created)
    assert all(sandbox.terminated for sandbox in sandboxes)

    created.clear()
    sandboxes.clear()

    star_remote = sandbox_mod.function(block_network=True)(_sample_remote_sum)
    assert star_remote.starmap([(1, 2), (3, 4), (5, 6)], concurrency=2) == [
        {"sum": 3},
        {"sum": 7},
        {"sum": 11},
    ]
    assert len(sandboxes) == 2
    assert len(created) == 2
    assert all(kwargs == {"image": "python:3.14-slim", "block_network": True} for kwargs in created)
    assert all(sandbox.terminated for sandbox in sandboxes)


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


def test_async_sandbox_lifecycle_methods_delegate(monkeypatch, mvm_home) -> None:
    sandbox_mod = _install_fakes(monkeypatch, mvm_home)
    vm = FakeMicroVM("sb", mvm_home)
    network = SimpleNamespace(tunnels=lambda: {8000: ("127.0.0.1", 18000)})
    sb = sandbox_mod.Sandbox(vm, network=network)
    monkeypatch.setattr(sb, "_start_timeout_watchdog", lambda secs: None)
    monkeypatch.setattr(sb, "_stop_timeout_watchdog", lambda: None)

    assert asyncio.run(sb.aio.snapshot("snap")) == sb.snapshot("snap")
    assert asyncio.run(sb.aio.extend(90)) == sb.extend(90)
    assert asyncio.run(sb.aio.metrics()) == sb.metrics()
    assert asyncio.run(sb.aio.tunnels()) == sb.tunnels()
    assert asyncio.run(sb.aio.create_connect_token()) == sb.create_connect_token()
    assert vm.snapshots == [
        ("snap", True, vm.rootfs_img),
        ("snap", True, vm.rootfs_img),
    ]


def test_async_sandbox_filesystem_delegates_to_agent(monkeypatch, mvm_home) -> None:
    sandbox_mod = _install_fakes(monkeypatch, mvm_home)
    vm = FakeMicroVM("sb", mvm_home)
    sb = sandbox_mod.Sandbox(vm)
    path = "/tmp/async.txt"

    asyncio.run(sb.aio.filesystem.write_text(path, "hello async", mode=0o600))
    assert asyncio.run(sb.aio.filesystem.read_text(path)) == "hello async"
    assert vm._agent.fs_writes == [(path, b"hello async", 0o600)]
    assert vm._agent.fs_reads == [path]

    assert asyncio.run(sb.aio.filesystem.stat(path)) == {"path": path, "size": 11}
    assert asyncio.run(sb.aio.filesystem.list_files("/tmp")) == [{"path": path, "type": "file"}]
    asyncio.run(sb.aio.filesystem.make_directory("/tmp/nested", parents=False))
    asyncio.run(sb.aio.filesystem.remove(path, recursive=True))
    assert vm._agent.fs_mkdirs == [("/tmp/nested", False)]
    assert vm._agent.fs_removes == [(path, True)]


def test_async_sandbox_parity_guard(monkeypatch, mvm_home) -> None:
    sandbox_mod = _install_fakes(monkeypatch, mvm_home)
    sb = sandbox_mod.Sandbox(FakeMicroVM("sb", mvm_home))
    async_sb = sandbox_mod._AsyncSandbox(sb)
    sandbox_methods = {
        "exec",
        "terminate",
        "wait_until_ready",
        "snapshot",
        "snapshot_filesystem",
        "poll",
        "set_network_policy",
        "extend",
        "metrics",
        "tunnels",
        "create_connect_token",
    }
    filesystem_methods = {
        "read_bytes",
        "read_text",
        "write_bytes",
        "write_text",
        "list_files",
        "make_directory",
        "remove",
        "stat",
    }

    for name in sandbox_methods:
        assert inspect.iscoroutinefunction(getattr(async_sb, name))
    for name in filesystem_methods:
        assert inspect.iscoroutinefunction(getattr(async_sb.filesystem, name))


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


def test_linux_networked_snapshot_template_restore_allocates_fresh_tap_and_replays_net_config(
    monkeypatch, mvm_home
):
    sandbox_mod = _install_fakes(monkeypatch, mvm_home)
    monkeypatch.setattr(sandbox_mod.platform, "system", lambda: "Linux")
    template_dir = mvm_home / "templates" / "t"
    (template_dir / "current-generation").write_text("1\n", encoding="utf-8")
    (template_dir / "vmstate.1.bin").touch()
    guest_config = {
        "tap": "vmon-fresh",
        "guest_ip": "10.42.0.2",
        "host_ip": "10.42.0.1",
        "prefix": 30,
        "dns": ["1.1.1.1"],
    }
    setup_calls: list[tuple[str, dict[str, object]]] = []

    def fake_setup(name, **kwargs):
        setup_calls.append((name, dict(kwargs)))
        return SimpleNamespace(
            guest_config=guest_config,
            tunnels=lambda: {8080: ("127.0.0.1", 18080)},
            teardown=lambda: None,
        )

    monkeypatch.setattr(sandbox_mod, "_setup_sandbox_network", fake_setup)

    sb = sandbox_mod.Sandbox.create(
        template="t",
        name="tap-restored",
        block_network=False,
        ports=[8080],
        egress_allow=["10.0.0.0/8"],
        egress_allow_domains=["example.com"],
        inbound_cidr_allowlist=["203.0.113.0/24"],
        pool_size=0,
    )

    try:
        assert setup_calls == [
            (
                "tap-restored",
                {
                    "ports": [8080],
                    "egress_allow": ["10.0.0.0/8"],
                    "egress_allow_domains": ["example.com"],
                    "inbound_cidr_allowlist": ["203.0.113.0/24"],
                },
            )
        ]
        assert FakeMicroVM.booted == []
        assert FakeMicroVM.restored, "snapshot template should be restored, not cold-booted"
        template, name, restore_kwargs, vm = FakeMicroVM.restored[-1]
        assert template == template_dir
        assert name == "tap-restored"
        assert restore_kwargs["tap"] == "vmon-fresh"
        assert vm._agent.net_configs[-1]["args"] == (
            "10.42.0.2",
            30,
            "10.42.0.1",
            ["1.1.1.1"],
        )
        assert sb._network_spec == {
            "flavor": "tap",
            "guest_config": guest_config,
            "ports": [8080],
            "tunnels": {"8080": {"host": "127.0.0.1", "port": 18080}},
            "policy": {
                "egress_allow": ["10.0.0.0/8"],
                "egress_allow_domains": ["example.com"],
                "inbound_cidr_allowlist": ["203.0.113.0/24"],
            },
        }
    finally:
        sb.terminate()


def test_macos_user_net_snapshot_template_restore_passes_user_net_and_replays_fixed_config(
    monkeypatch, mvm_home
):
    sandbox_mod = _install_fakes(monkeypatch, mvm_home)
    from vmon import net

    monkeypatch.setattr(sandbox_mod.platform, "system", lambda: "Darwin")
    template_dir = mvm_home / "templates" / "t"
    (template_dir / "current-generation").write_text("1\n", encoding="utf-8")
    (template_dir / "vmstate.1.bin").touch()

    sb = sandbox_mod.Sandbox.create(
        template="t",
        name="user-restored",
        block_network=False,
        pool_size=0,
    )

    try:
        assert FakeMicroVM.booted == []
        assert FakeMicroVM.restored, "snapshot template should be restored, not cold-booted"
        template, name, restore_kwargs, vm = FakeMicroVM.restored[-1]
        assert template == template_dir
        assert name == "user-restored"
        assert restore_kwargs["user_net"] is True
        assert "tap" not in restore_kwargs
        assert vm._agent.net_configs[-1]["args"] == (
            net.USER_NET_GUEST_IP,
            net.USER_NET_PREFIX,
            net.USER_NET_GATEWAY,
            list(net.USER_NET_DNS),
        )
        assert sb._network_spec == {
            "flavor": "user",
            "guest_config": {
                "guest_ip": net.USER_NET_GUEST_IP,
                "prefix": net.USER_NET_PREFIX,
                "host_ip": net.USER_NET_GATEWAY,
                "dns": list(net.USER_NET_DNS),
            },
            "ports": [],
            "tunnels": {},
            "policy": {},
        }
    finally:
        sb.terminate()
