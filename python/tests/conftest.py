import shutil
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(autouse=True)
def mvm_home(monkeypatch):
    """Isolated ``$VMON_HOME`` for every test.

    Rooted at ``/tmp`` rather than pytest's ``tmp_path``: the latter is too deep
    for an ``$VMON_HOME``-relative Unix socket path (``sun_path`` is ~104 bytes
    on macOS), so the daemon and real-VM tests that bind such sockets would
    overflow it. Some vmon modules cache ``STATE`` at import time, so repoint the
    already-imported ones at this home while still letting monkeypatch restore.
    """
    home = Path(tempfile.mkdtemp(prefix="vmont", dir="/tmp"))
    monkeypatch.setenv("VMON_HOME", str(home))

    volume_mod = sys.modules.get("vmon.volume")
    if volume_mod is not None:
        monkeypatch.setattr(volume_mod, "STATE", home, raising=False)
        monkeypatch.setattr(volume_mod, "VOLUME_DIR", home / "volumes", raising=False)
    for name in ("vmon.vmm", "vmon.sandbox"):
        mod = sys.modules.get(name)
        if mod is not None:
            monkeypatch.setattr(mod, "STATE", home, raising=False)
    try:
        yield home
    finally:
        shutil.rmtree(home, ignore_errors=True)
