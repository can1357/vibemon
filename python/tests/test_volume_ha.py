"""Volume (stateful) HA: checkpoint capture + collision/quorum-guarded restore.

Engine-level unit tests with no hypervisor. They exercise the capture, coercion,
and restore guards that keep cross-node volume replication from clobbering an
unrelated local volume or creating two divergent writers under a partition.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from vmon import volume as volume_mod
from vmon.core import Engine, Unsupported, _coerce_create_params
from vmon.volume import Volume


@pytest.fixture
def engine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Engine:
    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.setattr(volume_mod, "VOLUME_DIR", tmp_path / "volumes")
    return Engine()


def _stage(template: Path, name: str, payload: str) -> None:
    """Write a checkpoint-carried volume payload under ``<template>/volumes/<name>``."""
    vol = template / "volumes" / name
    vol.mkdir(parents=True)
    (vol / "sentinel.txt").write_text(payload, encoding="utf-8")


# -- coercion -----------------------------------------------------------------


def test_coerce_preserves_read_only() -> None:
    coerced = _coerce_create_params({"volumes": {"/data": {"name": "v1", "read_only": True}}})
    vol, ro = coerced["volumes"]["/data"]
    assert isinstance(vol, Volume)
    assert vol.name == "v1"
    assert ro is True


def test_coerce_accepts_bare_name() -> None:
    coerced = _coerce_create_params({"volumes": {"/data": "v1"}})
    assert isinstance(coerced["volumes"]["/data"], Volume)


def test_coerce_reuses_one_volume_object_per_name() -> None:
    coerced = _coerce_create_params(
        {"volumes": {"/a": {"name": "shared", "read_only": True}, "/b": {"name": "shared"}}}
    )
    assert coerced["volumes"]["/a"][0] is coerced["volumes"]["/b"][0]


# -- capture ------------------------------------------------------------------


def test_capture_copies_data_excluding_lock(engine: Engine, tmp_path: Path) -> None:
    src = volume_mod.VOLUME_DIR / "vol1"
    src.mkdir(parents=True)
    (src / "data.txt").write_text("hello", encoding="utf-8")
    (src / ".lock").write_text("x", encoding="utf-8")
    snap = tmp_path / "snap"
    snap.mkdir()
    engine._capture_volumes(snap, {"/data": {"name": "vol1", "read_only": True}})
    assert (snap / "volumes" / "vol1" / "data.txt").read_text() == "hello"
    assert not (snap / "volumes" / "vol1" / ".lock").exists()


def test_capture_fails_fast_on_missing_source(engine: Engine, tmp_path: Path) -> None:
    snap = tmp_path / "snap"
    snap.mkdir()
    with pytest.raises(Unsupported, match="no host directory"):
        engine._capture_volumes(snap, {"/data": {"name": "ghost", "read_only": True}})


# -- materialize guards -------------------------------------------------------


def test_materialize_read_only_without_quorum(engine: Engine, tmp_path: Path) -> None:
    tmpl = tmp_path / "tmpl"
    _stage(tmpl, "rovol", "RO_OK")
    created = engine.materialize_volumes(
        str(tmpl), {"volumes": {"/data": {"name": "rovol", "read_only": True}}}, quorum_ok=False
    )
    assert (volume_mod.VOLUME_DIR / "rovol" / "sentinel.txt").read_text() == "RO_OK"
    assert created == [volume_mod.VOLUME_DIR / "rovol"]


def test_materialize_writable_requires_quorum(engine: Engine, tmp_path: Path) -> None:
    tmpl = tmp_path / "tmpl"
    _stage(tmpl, "rwvol", "RW")
    params = {"volumes": {"/data": {"name": "rwvol", "read_only": False}}}
    with pytest.raises(Unsupported, match="without restore quorum"):
        engine.materialize_volumes(str(tmpl), params, quorum_ok=False)
    assert not (volume_mod.VOLUME_DIR / "rwvol").exists()  # nothing materialized on refusal
    engine.materialize_volumes(str(tmpl), params, quorum_ok=True)
    assert (volume_mod.VOLUME_DIR / "rwvol" / "sentinel.txt").read_text() == "RW"


def test_materialize_writable_if_any_mountpoint_rw(engine: Engine, tmp_path: Path) -> None:
    tmpl = tmp_path / "tmpl"
    _stage(tmpl, "mixed", "X")
    # Same volume mounted RO at /a and RW at /b -> writable -> needs quorum.
    params = {
        "volumes": {
            "/a": {"name": "mixed", "read_only": True},
            "/b": {"name": "mixed", "read_only": False},
        }
    }
    with pytest.raises(Unsupported, match="without restore quorum"):
        engine.materialize_volumes(str(tmpl), params, quorum_ok=False)


def test_materialize_refuses_name_collision(engine: Engine, tmp_path: Path) -> None:
    existing = volume_mod.VOLUME_DIR / "shared"
    existing.mkdir(parents=True)
    (existing / "local.txt").write_text("MINE", encoding="utf-8")
    tmpl = tmp_path / "tmpl"
    _stage(tmpl, "shared", "INCOMING")
    params = {"volumes": {"/data": {"name": "shared", "read_only": True}}}
    with pytest.raises(Unsupported, match="already exists"):
        engine.materialize_volumes(str(tmpl), params, quorum_ok=True)
    assert (existing / "local.txt").read_text() == "MINE"  # untouched, not clobbered


def test_materialize_missing_checkpoint_data(engine: Engine, tmp_path: Path) -> None:
    tmpl = tmp_path / "tmpl"
    tmpl.mkdir()
    params = {"volumes": {"/data": {"name": "ghost", "read_only": True}}}
    with pytest.raises(Unsupported, match="missing data"):
        engine.materialize_volumes(str(tmpl), params, quorum_ok=True)


def test_materialize_dedupes_same_name_across_mountpoints(engine: Engine, tmp_path: Path) -> None:
    tmpl = tmp_path / "tmpl"
    _stage(tmpl, "dup", "ONE")
    params = {
        "volumes": {
            "/a": {"name": "dup", "read_only": True},
            "/b": {"name": "dup", "read_only": True},
        }
    }
    created = engine.materialize_volumes(str(tmpl), params, quorum_ok=False)
    assert created == [volume_mod.VOLUME_DIR / "dup"]  # copied once, no false collision


# -- restore rollback ---------------------------------------------------------


def test_restore_rolls_back_volumes_on_create_failure(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tmpl = tmp_path / "tmpl"
    _stage(tmpl, "rbk", "DATA")

    def boom(_params: dict) -> None:
        raise RuntimeError("create failed")

    monkeypatch.setattr(engine, "create", boom)
    params = {"name": "x", "volumes": {"/data": {"name": "rbk", "read_only": True}}}
    with pytest.raises(RuntimeError, match="create failed"):
        engine.restore_from_template(params, str(tmpl), quorum_ok=False)
    # The freshly-materialized volume is removed so a retry is not poisoned.
    assert not (volume_mod.VOLUME_DIR / "rbk").exists()
