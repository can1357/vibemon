from pathlib import Path

import pytest


def _snapshot(mvm_home: Path, name: str) -> Path:
    snap = mvm_home / "snapshots" / name
    snap.mkdir(parents=True)
    (snap / "current-generation").write_text("1\n", encoding="utf-8")
    (snap / "vmstate.1.bin").touch()
    return snap


def test_volume_args_formats_repeatable_cli_args():
    from vmon.vmm import _volume_args

    assert _volume_args([]) == []
    assert _volume_args([("t", "/d", False), ("c", "/c", True)]) == [
        "--volume",
        "t:/d",
        "--volume",
        "c:/c:ro",
    ]


def test_volume_args_preserves_spaces_colons_and_rejects_ambiguous_ro_suffix():
    from vmon.vmm import _volume_args

    assert _volume_args([("data", "/tmp/has space/a:b", False)]) == [
        "--volume",
        "data:/tmp/has space/a:b",
    ]
    assert _volume_args([("data", "/tmp/path:ro", True)]) == [
        "--volume",
        "data:/tmp/path:ro:ro",
    ]
    with pytest.raises(ValueError):
        _volume_args([("bad-tag", "/tmp/x", False)])
    with pytest.raises(ValueError):
        _volume_args([("data", "/tmp/path:ro", False)])


def test_restore_rejects_invalid_resource_args_before_launch(monkeypatch, mvm_home):
    from vmon.vmm import MicroVM

    _snapshot(mvm_home, "snap")

    def fake_launch(self, args, **meta):
        raise AssertionError("_launch should not run for invalid resources")

    monkeypatch.setattr(MicroVM, "_launch", fake_launch)
    with pytest.raises(ValueError):
        MicroVM.restore("snap", mem=0)
    with pytest.raises(ValueError):
        MicroVM.restore("snap", cpus=0)
    with pytest.raises(ValueError):
        MicroVM.restore("snap", timeout_secs=86_401)


def test_agent_call_without_guest_socket_fails_without_socket_connect(mvm_home):
    from vmon.agent_client import AgentClosed
    from vmon.vmm import MicroVM

    vm = MicroVM("without-agent-socket")
    vm._save_meta(agent_sock=None)
    with pytest.raises(AgentClosed, match="without a guest agent"):
        vm.agent()


def test_restore_emits_volume_and_timeout_args(monkeypatch, mvm_home):
    from vmon.vmm import MicroVM

    _snapshot(mvm_home, "snap")
    holder = {}

    def fake_launch(self: object, args: list[str], **meta: object) -> None:
        holder["args"] = list(args)
        holder["meta"] = dict(meta)

    monkeypatch.setattr(MicroVM, "_launch", fake_launch)

    MicroVM.restore("snap", volumes=[("t", "/d", False)], timeout_secs=5, agent=False)

    args = holder["args"]
    assert ["--volume", "t:/d"] == args[args.index("--volume") : args.index("--volume") + 2]
    assert "--timeout-secs" in args
    assert args[args.index("--timeout-secs") + 1] == "5"
    assert holder["meta"]["volumes"] == [{"tag": "t", "dir": "/d", "ro": False}]


def test_restore_emits_tap_and_user_net_args(monkeypatch, mvm_home):
    from vmon.vmm import MicroVM

    _snapshot(mvm_home, "snap")
    holder = {}

    def fake_launch(self: object, args: list[str], **meta: object) -> None:
        holder["args"] = list(args)
        holder["meta"] = dict(meta)

    monkeypatch.setattr(MicroVM, "_launch", fake_launch)

    MicroVM.restore("snap", tap="tap7", agent=False)
    args = holder["args"]
    assert args[args.index("--tap") + 1] == "tap7"
    assert holder["meta"]["tap"] == "tap7"

    MicroVM.restore("snap", user_net=True, agent=False)
    args = holder["args"]
    assert ["--net", "user"] == args[args.index("--net") : args.index("--net") + 2]
    assert holder["meta"]["user_net"] is True

    with pytest.raises(ValueError, match="mutually exclusive"):
        MicroVM.restore("snap", tap="tap7", user_net=True, agent=False)


def test_fork_snapshot_emits_volume_and_timeout_args(monkeypatch, mvm_home):
    from vmon.vmm import MicroVM

    _snapshot(mvm_home, "base")
    holder = {}

    def fake_launch(self: object, args: list[str], **meta: object) -> None:
        holder["args"] = list(args)
        holder["meta"] = dict(meta)

    monkeypatch.setattr(MicroVM, "_launch", fake_launch)

    MicroVM.fork_snapshot("base", volumes=[("t", "/d", False)], timeout_secs=5, agent=False)

    args = holder["args"]
    assert "--fork-from" in args
    assert "--volume" in args
    assert args[args.index("--volume") + 1] == "t:/d"
    assert "--timeout-secs" in args
    assert args[args.index("--timeout-secs") + 1] == "5"
    assert holder["meta"]["volumes"] == [{"tag": "t", "dir": "/d", "ro": False}]


def test_control_snapshot_payload_includes_base_only(monkeypatch):
    from vmon.control import Control

    seen: list[tuple[str, dict | None]] = []
    monkeypatch.setattr(
        Control, "_request", lambda self, method, params=None: seen.append((method, params)) or {}
    )
    control = Control("/nonexistent.sock")
    control.snapshot("t")
    control.snapshot("t", base="b")
    assert seen[0] == ("snapshot", {"name": "t"})
    assert seen[1] == ("snapshot", {"name": "t", "base": "b"})


def test_microvm_snapshot_forwards_base(monkeypatch, mvm_home):
    from vmon.control import Control
    from vmon.vmm import MicroVM

    seen: dict[str, str | None] = {}
    monkeypatch.setattr(Control, "pause", lambda self: {})
    monkeypatch.setattr(Control, "resume", lambda self: {})
    monkeypatch.setattr(
        Control,
        "snapshot",
        lambda self, name, base=None: seen.update(base=base) or {},
    )
    monkeypatch.setattr(MicroVM, "stop", lambda self, wait=True: None)
    MicroVM("snaptest").snapshot("s", keep_running=False, base="base0")
    assert seen["base"] == "base0"


def test_prewarm_picks_nic_slot_flavor_by_platform(monkeypatch, mvm_home):
    import types

    import vmon.sandbox as sandbox_mod

    seen: dict[str, bool | None] = {}

    def fake_cached_template(image, **kw):
        seen["nic_slot"] = kw.get("nic_slot")
        return types.SimpleNamespace(
            snapshot_dir=Path(f"/tmp/tpl-{seen['nic_slot']}-{image}"),
            spec=types.SimpleNamespace(reference=image),
        )

    monkeypatch.setattr(sandbox_mod, "cached_template", fake_cached_template)
    monkeypatch.setattr(
        sandbox_mod, "WarmPool", lambda d, n: types.SimpleNamespace(size=n, shutdown=lambda: None)
    )
    try:
        monkeypatch.setattr(sandbox_mod.platform, "system", lambda: "Darwin")
        sandbox_mod.prewarm("img", count=1)
        assert seen["nic_slot"] is True
        monkeypatch.setattr(sandbox_mod.platform, "system", lambda: "Linux")
        sandbox_mod.prewarm("img2", count=1)
        assert seen["nic_slot"] is False
    finally:
        sandbox_mod.shutdown_all_pools()


def test_boot_rootfs_passes_rng_flag_only_when_requested(monkeypatch, mvm_home):
    from vmon.vmm import MicroVM

    monkeypatch.setenv("VMON_KERNEL", "/tmp/fake-kernel")
    captured: dict[str, list[str]] = {}

    def fake_launch(self, args, **meta):
        captured["args"] = list(args)

    monkeypatch.setattr(MicroVM, "_launch", fake_launch)

    MicroVM.boot_rootfs(mvm_home / "rootfs.img", rng=True)
    assert "--rng" in captured["args"]

    MicroVM.boot_rootfs(mvm_home / "rootfs.img")
    assert "--rng" not in captured["args"]
