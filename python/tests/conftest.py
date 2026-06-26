from pathlib import Path
import os
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(autouse=True)
def mvm_home(tmp_path, monkeypatch):
    home = tmp_path / "vmon-home"
    home.mkdir()
    monkeypatch.setenv("VMON_HOME", str(home))

    # Some vmon modules cache STATE at import time. Keep already-imported modules
    # pointed at this test's isolated home while still letting monkeypatch restore.
    volume_mod = sys.modules.get("vmon.volume")
    if volume_mod is not None:
        monkeypatch.setattr(volume_mod, "STATE", home, raising=False)
        monkeypatch.setattr(volume_mod, "VOLUME_DIR", home / "volumes", raising=False)

    vmm_mod = sys.modules.get("vmon.vmm")
    if vmm_mod is not None:
        monkeypatch.setattr(vmm_mod, "STATE", home, raising=False)

    sandbox_mod = sys.modules.get("vmon.sandbox")
    if sandbox_mod is not None:
        monkeypatch.setattr(sandbox_mod, "STATE", home, raising=False)

    yield home
