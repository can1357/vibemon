import shutil
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(autouse=True)
def mvm_home(monkeypatch):
    """Isolated ``$VMON_HOME`` for every test.

    Rooted at ``/tmp`` rather than pytest's ``tmp_path``: the latter is short
    enough for Unix-socket transports on macOS, and ``vmon.volume`` caches a
    couple of legacy path constants at import time.
    """
    home = Path(tempfile.mkdtemp(prefix="vmont", dir="/tmp"))
    monkeypatch.setenv("VMON_HOME", str(home))

    volume_mod = sys.modules.get("vmon.volume")
    if volume_mod is not None:
        monkeypatch.setattr(volume_mod, "STATE", home, raising=False)
        monkeypatch.setattr(volume_mod, "VOLUME_DIR", home / "volumes", raising=False)
    try:
        yield home
    finally:
        shutil.rmtree(home, ignore_errors=True)
