"""Engine-level mesh migration: eligibility gating and commit/abort lifecycle.

These exercise the host-local-state soundness gates and the
checkpoint/commit/rollback bookkeeping without a hypervisor. The happy-path
snapshot/restore across two nodes is covered by ``test_mesh.py`` (orchestration,
faked I/O) and the gated e2e suite (real VMs).
"""

from pathlib import Path

import pytest

from vmon.core import Engine, Unsupported, VMRecord


class _FakeVM:
    def __init__(self, meta=None):
        self.meta = dict(meta or {})


class _FakeSandbox:
    def __init__(
        self,
        *,
        block_network=True,
        volumes=None,
        fs_dir=None,
        secret_env=None,
        env=None,
        workdir="/work",
        timeout_secs=None,
        meta=None,
    ):
        self._block_network = block_network
        self._volumes = list(volumes or [])
        self._fs_dir = fs_dir
        self._secret_env = dict(secret_env or {})
        self.env = dict(env or {})
        self.workdir = workdir
        self._timeout_secs = timeout_secs
        self.vm = _FakeVM(meta)


def _engine_with(sandbox, *, detail=None, name="vm1"):
    eng = Engine()
    rec = VMRecord(
        id=name,
        name=name,
        status="running",
        sandbox=sandbox,
        detail=dict(detail if detail is not None else {"memory": 512, "cpus": 1}),
        tags={"team": "infra"},
    )
    eng._records[name] = rec
    return eng


def test_migrate_prepare_rejects_networked(mvm_home):
    eng = _engine_with(_FakeSandbox(block_network=False))
    with pytest.raises(Unsupported, match="block-network"):
        eng.migrate_prepare("vm1")


def test_prepare_restore_params_carries_volumes(mvm_home):
    meta = {"volumes": {"/data": {"name": "vol1", "tag": "vol1", "read_only": True}}}
    eng = _engine_with(_FakeSandbox(volumes=[object()], meta=meta))
    _carried_meta, params = eng._prepare_restore_params("vm1")
    assert params["volumes"] == {"/data": {"name": "vol1", "read_only": True}}


def test_migrate_prepare_rejects_fs_dir(mvm_home):
    eng = _engine_with(_FakeSandbox(fs_dir="/host/share"))
    with pytest.raises(Unsupported, match="fs_dir"):
        eng.migrate_prepare("vm1")


def test_migrate_prepare_rejects_rehydrated(mvm_home):
    eng = _engine_with(None)
    with pytest.raises(Unsupported, match="live in-process"):
        eng.migrate_prepare("vm1")


def test_migrate_prepare_rejects_unknown_memory(mvm_home):
    eng = _engine_with(_FakeSandbox(), detail={})
    with pytest.raises(Unsupported, match="memory size"):
        eng.migrate_prepare("vm1")


def test_migrate_commit_removes_source_and_deletes_checkpoint(mvm_home, monkeypatch):
    eng = _engine_with(_FakeSandbox())
    removed: list[str] = []
    monkeypatch.setattr(eng, "remove", lambda name: removed.append(name) or {})
    import vmon.cas as cas

    dropped: list[str] = []
    monkeypatch.setattr(cas, "drop_digest", lambda digest: dropped.append(digest))
    snap = Path(mvm_home) / "snapshots" / "migrate-vm1-abc"
    snap.mkdir(parents=True)
    (snap / "memory.0.bin").write_bytes(b"x")

    eng.migrate_commit("vm1", str(snap), "deadbeef")

    assert removed == ["vm1"]
    assert dropped == ["deadbeef"]
    assert not snap.exists()  # the source's checkpoint is deleted on a clean commit


def test_migrate_abort_restores_source_and_keeps_checkpoint(mvm_home, monkeypatch):
    eng = _engine_with(_FakeSandbox())
    removed: list[str] = []
    created: list[dict] = []
    monkeypatch.setattr(eng, "remove", lambda name: removed.append(name) or {})
    monkeypatch.setattr(
        eng,
        "create",
        lambda params: (
            created.append(params)
            or VMRecord(id=params["name"], name=params["name"], status="running")
        ),
    )
    import vmon.cas as cas

    dropped: list[str] = []
    monkeypatch.setattr(cas, "drop_digest", lambda digest: dropped.append(digest))
    snap = Path(mvm_home) / "snapshots" / "migrate-vm1-abc"
    snap.mkdir(parents=True)
    params = {"name": "vm1", "block_network": True, "memory": 512}

    eng.migrate_abort("vm1", str(snap), "deadbeef", params)

    assert removed == ["vm1"]
    # The source is restored locally from the same checkpoint, preserving its id.
    assert created and created[0]["template"] == str(snap)
    assert created[0]["name"] == "vm1"
    assert dropped == ["deadbeef"]
    assert snap.exists()  # the dir is kept: the restored VM may page from it
