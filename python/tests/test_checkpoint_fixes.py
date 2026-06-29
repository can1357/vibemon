import json


def test_restore_forwards_mem_and_cpus_to_persisted_launch_meta(monkeypatch, mvm_home):
    import vmon.vmm as vmm_mod

    launches: list[dict[str, object]] = []

    def fake_launch(self: object, args: list[str], **meta: object) -> None:
        launches.append(dict(meta))

    monkeypatch.setattr(vmm_mod, "_snapshot_state_present", lambda *_a, **_k: True)
    monkeypatch.setattr(vmm_mod.MicroVM, "_launch", fake_launch)
    monkeypatch.setattr(vmm_mod.MicroVM, "_record_reconstruct", lambda self, *a, **k: None)
    (mvm_home / "snapshots" / "some-snap").mkdir(parents=True)

    vmm_mod.MicroVM.restore("some-snap", name="r1", mem=2048, cpus=3)

    assert launches[-1]["mem"] == 2048
    assert launches[-1]["cpus"] == 3

    vmm_mod.MicroVM.restore("some-snap", name="r2", mem=None, cpus=None)

    assert "mem" in launches[-1]
    assert "cpus" in launches[-1]
    assert launches[-1]["mem"] is None
    assert launches[-1]["cpus"] is None


def test_stamp_template_marker_copies_source_marker_without_content_digest(tmp_path):
    import vmon.core as core_mod

    src = tmp_path / "tpl"
    src.mkdir()
    (src / "agent-ready.json").write_text(
        json.dumps({"boot": 1, "content_digest": "DEADBEEF"}),
        encoding="utf-8",
    )
    snap = tmp_path / "snap"
    snap.mkdir()

    core_mod.Engine._stamp_template_marker(object(), snap, {"template": str(src)})

    marker = snap / "agent-ready.json"
    assert marker.is_file()
    data = json.loads(marker.read_text(encoding="utf-8"))
    assert data["boot"] == 1
    assert "content_digest" not in data


def test_stamp_template_marker_without_source_does_not_create_marker(tmp_path):
    import vmon.core as core_mod

    snap = tmp_path / "snap"
    snap.mkdir()

    core_mod.Engine._stamp_template_marker(object(), snap, {})

    assert not (snap / "agent-ready.json").exists()


def test_stamp_template_marker_keeps_existing_marker_untouched(tmp_path):
    import vmon.core as core_mod

    src = tmp_path / "tpl"
    src.mkdir()
    (src / "agent-ready.json").write_text(
        json.dumps({"boot": 1, "content_digest": "DEADBEEF"}),
        encoding="utf-8",
    )
    snap = tmp_path / "snap"
    snap.mkdir()
    marker = snap / "agent-ready.json"
    existing = "not-json and must stay"
    marker.write_text(existing, encoding="utf-8")

    core_mod.Engine._stamp_template_marker(object(), snap, {"template": str(src)})

    assert marker.read_text(encoding="utf-8") == existing
