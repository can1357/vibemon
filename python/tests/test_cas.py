from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

from vmon import cas


def _make_template(path: Path) -> Path:
    path.mkdir(parents=True)
    (path / "rootfs.img").write_bytes(b"rootfs")
    (path / "agent-ready.json").write_text(
        json.dumps(
            {
                "image": "example:latest",
                "digest": "image-digest",
                "disk_mb": 64,
                "boot_version": 5,
                "kernel_sha": "kernel",
                "fs_slots": 0,
                "memory": 512,
                "cpus": 1,
                "host_slot": False,
                "nic_slot": False,
            }
        ),
        encoding="utf-8",
    )
    (path / "current-generation").write_text("1", encoding="utf-8")
    (path / "vmstate.1.bin").write_bytes(b"state")
    return path


def test_template_digest_stable_and_mtime_independent(tmp_path: Path) -> None:
    template = _make_template(tmp_path / "tpl")

    first = cas.template_digest(template)
    os.utime(template / "rootfs.img", (1, 1))
    os.utime(template / "vmstate.1.bin", (2, 2))
    marker = template / "agent-ready.json"
    marker_data = json.loads(marker.read_text(encoding="utf-8"))
    marker_data["content_digest"] = "0" * 64
    marker.write_text(json.dumps(marker_data), encoding="utf-8")

    assert cas.template_digest(template) == first


def test_template_digest_changes_when_file_content_changes(tmp_path: Path) -> None:
    template = _make_template(tmp_path / "tpl")
    first = cas.template_digest(template)

    (template / "vmstate.1.bin").write_bytes(b"changed")

    assert cas.template_digest(template) != first


def test_template_digest_changes_when_file_added(tmp_path: Path) -> None:
    template = _make_template(tmp_path / "tpl")
    first = cas.template_digest(template)

    (template / "device-state.json").write_text('{"device": true}', encoding="utf-8")

    assert cas.template_digest(template) != first


def test_index_resolve_and_list_round_trip(tmp_path: Path) -> None:
    template = _make_template(tmp_path / "tpl")

    digest = cas.index_template(template)

    assert cas.resolve_digest(digest) == template
    assert cas.list_digests() == {digest: str(template)}


def test_resolve_digest_prunes_missing_or_incomplete_template(tmp_path: Path) -> None:
    template = _make_template(tmp_path / "tpl")
    digest = cas.index_template(template)
    (template / "rootfs.img").unlink()

    assert cas.resolve_digest(digest) is None
    assert not (cas.cas_root() / f"{digest}.json").exists()


def test_resolve_digest_prunes_missing_template(tmp_path: Path) -> None:
    template = _make_template(tmp_path / "tpl")
    digest = cas.index_template(template)
    shutil.rmtree(template)

    assert cas.resolve_digest(digest) is None
    assert not (cas.cas_root() / f"{digest}.json").exists()
