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


def test_prepare_restore_params_carries_linux_tap_network_restore_state(mvm_home):
    guest_config = {
        "tap": "tap-source",
        "guest_ip": "10.42.0.2",
        "prefix": 30,
        "host_ip": "10.42.0.1",
        "dns": ["1.1.1.1"],
    }

    class _LiveNetwork:
        def __init__(self):
            self.guest_config = guest_config

        def tunnels(self):
            return {8080: ("127.0.0.1", 18080)}

    sandbox = _FakeSandbox(block_network=False)
    sandbox._network = _LiveNetwork()
    sandbox._network_policy = {
        "egress_allow": ["10.0.0.0/8"],
        "egress_allow_domains": ["example.com"],
        "inbound_cidr_allowlist": ["203.0.113.0/24"],
    }
    eng = _engine_with(sandbox)

    _carried_meta, params = eng._prepare_restore_params("vm1")

    assert params["block_network"] is False
    assert params["ports"] == [8080]
    assert params["egress_allow"] == ["10.0.0.0/8"]
    assert params["egress_allow_domains"] == ["example.com"]
    assert params["inbound_cidr_allowlist"] == ["203.0.113.0/24"]
    assert params["network"] == {
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


def test_prepare_restore_params_carries_user_net_network_flavor(mvm_home):
    from vmon import net

    eng = _engine_with(_FakeSandbox(block_network=False, meta={"user_net": True}))

    _carried_meta, params = eng._prepare_restore_params("vm1")

    assert params["block_network"] is False
    assert params["network"] == {
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


def test_restore_from_template_strips_internal_network_before_create(mvm_home, monkeypatch):
    import vmon.core as core_mod

    eng = Engine()
    seen: dict[str, dict] = {}
    monkeypatch.setattr(core_mod.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(eng, "materialize_volumes", lambda template_dir, params, quorum_ok: [])

    def fake_create(params):
        seen["params"] = dict(params)
        return VMRecord(id=params["name"], name=params["name"], status="running")

    monkeypatch.setattr(eng, "create", fake_create)

    restored = eng.restore_from_template(
        {
            "name": "vm1",
            "block_network": False,
            "memory": 512,
            "ports": [],
            "network": {"flavor": "user"},
        },
        str(Path(mvm_home) / "snapshots" / "checkpoint"),
        quorum_ok=True,
    )

    assert restored.name == "vm1"
    assert "network" not in seen["params"]
    assert seen["params"]["block_network"] is False
    assert seen["params"]["ports"] == []
    assert seen["params"]["template"] == str(Path(mvm_home) / "snapshots" / "checkpoint")


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


def test_user_net_checkpoint_uses_normal_snapshot_path(mvm_home, monkeypatch):
    eng = _engine_with(_FakeSandbox(block_network=False, meta={"user_net": True}))
    seen: dict[str, tuple[str, str, bool]] = {}

    def fake_snapshot_template(name, snapshot, stop=False):
        seen["snapshot"] = (name, snapshot, stop)
        snap = Path(mvm_home) / "snapshots" / snapshot
        snap.mkdir(parents=True)
        return str(snap)

    monkeypatch.setattr(eng, "snapshot_template", fake_snapshot_template)
    monkeypatch.setattr(eng, "_stamp_template_marker", lambda snap, meta: None)
    monkeypatch.setattr(eng, "_capture_volumes", lambda snap, volumes: None)
    import vmon.cas as cas

    monkeypatch.setattr(cas, "template_digest", lambda snap: "digest")
    monkeypatch.setattr(cas, "index_template", lambda snap, digest: digest)

    checkpoint = eng._checkpoint_for("vm1", kind="ha", stop=False)

    assert seen["snapshot"][0] == "vm1"
    assert seen["snapshot"][2] is False
    assert checkpoint["params"]["network"]["flavor"] == "user"


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
