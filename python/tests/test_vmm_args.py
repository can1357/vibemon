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


def test_agent_call_on_no_agent_vm_fails_without_socket_connect(mvm_home):
    from vmon.agent import AgentClosed
    from vmon.vmm import MicroVM

    vm = MicroVM("no-agent")
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
