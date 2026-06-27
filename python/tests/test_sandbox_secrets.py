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

    def net_config(self, **kwargs: object) -> dict[str, bool]:
        self.net_configs.append(kwargs)
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

    # block_network + volumes fresh-boots (the warm-restore path cannot enumerate
    # virtio-fs devices the template snapshot lacks), so assert on the boot kwargs.
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
