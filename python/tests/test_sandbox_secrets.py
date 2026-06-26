import io
import json


class FakeSession:
    def __init__(self):
        self.stdout = io.BytesIO(b"")
        self.stderr = io.BytesIO(b"")
        self.stdin = io.BytesIO()
        self.returncode = 0

    def write_stdin(self, data):
        self.stdin.write(bytes(data))

    def close_stdin(self):
        pass

    def kill(self):
        self.returncode = -9

    def wait(self, timeout=None):
        return self.returncode

    def resize(self, rows, cols):
        self.rows = rows
        self.cols = cols


class FakeAgentConn:
    def __init__(self):
        self.closed = False
        self.exec_envs = []
        self.mounts = []
        self.net_configs = []

    def ping(self, timeout=None):
        return {"ok": True}

    def mount(self, tag, path, ro=False):
        self.mounts.append((tag, path, ro))
        return {"ok": True}

    def net_config(self, **kwargs):
        self.net_configs.append(kwargs)
        return {"ok": True}

    def exec(self, argv, cwd=None, env=None, timeout=None, tty=False):
        self.exec_envs.append(dict(env or {}))
        return FakeSession()

    def close(self):
        self.closed = True


class FakeControl:
    def extend(self, secs):
        return {"deadline_unix": secs}


class FakeMicroVM:
    restored = []

    def __init__(self, name, root):
        self.name = name
        self.dir = root / "vms" / name
        self.dir.mkdir(parents=True, exist_ok=True)
        self.rootfs_img = self.dir / "rootfs.img"
        self.control = FakeControl()
        self._agent = FakeAgentConn()
        self.stopped = False

    @classmethod
    def restore(cls, template, name=None, **kwargs):
        import vmon.sandbox as sandbox_mod

        vm = cls(name or "sb", sandbox_mod.STATE)
        cls.restored.append((template, name, dict(kwargs), vm))
        return vm

    def agent(self, connect_timeout=None):
        return self._agent

    def _save_meta(self, **kw):
        self.dir.mkdir(parents=True, exist_ok=True)
        (self.dir / "meta.json").write_text(json.dumps(kw, indent=2, sort_keys=True))

    def stop(self):
        self.stopped = True


def _install_fakes(monkeypatch, mvm_home):
    import vmon.sandbox as sandbox_mod

    FakeMicroVM.restored = []
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

    for path in vm.dir.rglob("*"):
        if path.is_file():
            assert "sekret-value" not in path.read_text(errors="ignore")

    meta = json.loads((vm.dir / "meta.json").read_text())
    assert meta["secret_names"] == ["FOO"]
    assert meta["env_names"] == ["PLAIN"]
    assert "env" not in meta
    assert "sekret-value" not in json.dumps(meta)

    sb.exec("true")
    assert vm._agent.exec_envs[-1]["FOO"] == "sekret-value"
    assert vm._agent.exec_envs[-1]["PLAIN"] == "ok"
    sb.terminate()


def test_volume_tag_collisions_get_unique_restore_tags(monkeypatch, mvm_home):
    sandbox_mod = _install_fakes(monkeypatch, mvm_home)
    from vmon.volume import Volume

    sb = sandbox_mod.Sandbox.create(
        template="t",
        volumes={"/one": Volume("a-b"), "/two": Volume("a_b")},
        pool_size=0,
        block_network=True,
    )
    restore_kwargs = FakeMicroVM.restored[-1][2]
    tags = [tag for tag, _host_dir, _read_only in restore_kwargs["volumes"]]

    assert tags == ["a_b", "a_b_2"]
    assert len(tags) == len(set(tags))
    sb.terminate()
