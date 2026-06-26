from pathlib import Path


def _snapshot(mvm_home, name):
    snap = mvm_home / "snapshots" / name
    snap.mkdir(parents=True)
    (snap / "vmstate.bin").touch()
    return snap


def test_volume_args_formats_repeatable_cli_args():
    from vmon.vmm import _volume_args

    assert _volume_args([]) == []
    assert _volume_args([("t", "/d", False), ("c", "/c", True)]) == [
        "--volume", "t:/d", "--volume", "c:/c:ro",
    ]


def test_restore_emits_volume_and_timeout_args(monkeypatch, mvm_home):
    from vmon.vmm import MicroVM

    _snapshot(mvm_home, "snap")
    holder = {}

    def fake_launch(self, args, **meta):
        holder["args"] = list(args)
        holder["meta"] = dict(meta)

    monkeypatch.setattr(MicroVM, "_launch", fake_launch)

    MicroVM.restore("snap", volumes=[("t", "/d", False)], timeout_secs=5, agent=False)

    args = holder["args"]
    assert ["--volume", "t:/d"] == args[args.index("--volume"):args.index("--volume") + 2]
    assert "--timeout-secs" in args
    assert args[args.index("--timeout-secs") + 1] == "5"
    assert holder["meta"]["volumes"] == [{"tag": "t", "dir": "/d", "ro": False}]


def test_fork_snapshot_emits_volume_and_timeout_args(monkeypatch, mvm_home):
    from vmon.vmm import MicroVM

    _snapshot(mvm_home, "base")
    holder = {}

    def fake_launch(self, args, **meta):
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
