"""Daemon/client tests: auto-start, round-trip, streaming, single-owner, rehydrate.

No KVM required: a fake Sandbox backend is injected for in-process daemon tests
(``vmon.core.Engine._sandbox_class``), mirroring ``test_server.py``. The fake
persists a ``meta.json`` with a real dummy child PID so the engine's pid-liveness
checks (``ps``, rehydrate, stop/rm teardown) behave like the real backend without
ever signalling the test process.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import suppress
from pathlib import Path

import pytest

import vmon.core as core
from vmon.agent import ByteStream
from vmon.client import DaemonClient, DaemonError
from vmon.daemon import Daemon, acquire_owner_lock, daemon_paths, release_owner_lock
from vmon.vmm import MicroVM
from vmon.volume import Volume


class FakeProc:
    """A stand-in exec process emitting fixed stdout chunks then exiting 0."""

    def __init__(self, chunks, rc=0):
        self.stdout = ByteStream()
        self.stderr = ByteStream()
        self._rc = rc
        self._chunks = chunks
        self.stdin_closed = False
        self._exit = threading.Event()
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        for stream, data in self._chunks:
            (self.stdout if stream == "stdout" else self.stderr).feed(data)
        self.stdout.close()
        self.stderr.close()
        self._exit.set()

    @property
    def returncode(self):
        return self._rc if self._exit.is_set() else None

    def wait(self, timeout=None):
        self._exit.wait(timeout)
        return self._rc

    def kill(self, *a):
        self._exit.set()

    def write_stdin(self, data):
        pass

    def close_stdin(self):
        self.stdin_closed = True

    def resize(self, rows, cols):
        pass


class FakeSandbox:
    """Fake backend that persists meta (with a real dummy PID) like the real one."""

    instances: dict[str, FakeSandbox] = {}
    last_create_kwargs: dict | None = None
    last_proc: FakeProc | None = None

    def __init__(self, name):
        self.name = name
        self.tags = {}
        self.returncode = None
        self.terminated = False
        self.image_spec = None
        self._child = None
        self.vm = MicroVM(name)

    @classmethod
    def create(cls, **kwargs):
        cls.last_create_kwargs = dict(kwargs)
        name = kwargs.get("name") or "sb-fake"
        sb = cls(name)
        sb._child = subprocess.Popen(["sleep", "300"])
        sb.vm._save_meta(
            pid=sb._child.pid, image=kwargs.get("image") or "fake", sandbox=True, status="running"
        )
        cls.instances[name] = sb
        return sb

    @classmethod
    def attach(cls, name):
        return cls.instances.get(name) or cls(name)

    def exec(self, *cmd, **kwargs):
        proc = FakeProc([("stdout", b"hello\n"), ("stdout", b"world\n")], rc=0)
        type(self).last_proc = proc
        return proc

    def terminate(self, *a, **k):
        self.terminated = True
        if self._child is not None:
            self._child.terminate()
            with suppress(Exception):
                self._child.wait(timeout=2)


@pytest.fixture
def short_home(monkeypatch):
    """A short ``$VMON_HOME`` so the daemon's Unix socket fits AF_UNIX path limits.

    pytest's ``tmp_path`` is too deep for ``sun_path`` (~104 bytes) on macOS, so
    point VMON_HOME at a short ``/tmp`` dir and repoint the cached module STATEs.
    """
    home = tempfile.mkdtemp(prefix="vmont", dir="/tmp")
    monkeypatch.setenv("VMON_HOME", home)
    import vmon.vmm as vmm_mod
    import vmon.volume as volume_mod

    monkeypatch.setattr(vmm_mod, "STATE", Path(home))
    monkeypatch.setattr(volume_mod, "STATE", Path(home))
    monkeypatch.setattr(volume_mod, "VOLUME_DIR", Path(home) / "volumes")
    try:
        yield home
    finally:
        shutil.rmtree(home, ignore_errors=True)


@pytest.fixture
def fake_engine(monkeypatch, short_home):
    monkeypatch.setattr("vmon.core.Engine._sandbox_class", staticmethod(lambda: FakeSandbox))
    FakeSandbox.instances = {}
    return core.Engine()


@pytest.fixture
def client(fake_engine):
    """An in-process daemon (own thread) over the fake engine + a connected client."""
    sock = str(daemon_paths()["sock"])
    daemon = Daemon(fake_engine, sock_path=sock)
    ready = threading.Event()
    threading.Thread(target=daemon.serve_forever, kwargs={"ready": ready}, daemon=True).start()
    assert ready.wait(2)
    try:
        yield DaemonClient(sock_path=sock, autostart=False)
    finally:
        daemon.shutdown()


def test_round_trip_run_ps_stop_rm(client):
    assert client.call("ps") == {"vms": []}

    started = client.call("run", image="alpine", detach=True, name="vm1")
    assert started["name"] == "vm1"
    assert started["detached"] is True

    rows = client.call("ps")["vms"]
    assert [r["name"] for r in rows] == ["vm1"]
    assert rows[0]["status"] == "running"

    client.call("stop", name="vm1")
    client.call("rm", name="vm1")
    assert client.call("ps") == {"vms": []}


def test_run_gives_payload_eof_like_docker(fake_engine):
    """Foreground `vmon run` closes the payload's stdin so it gets EOF.

    `vmon run` forwards no stdin, so a default `/bin/sh` would block forever
    waiting for input; closing stdin mirrors `docker run` without -i.
    """
    FakeSandbox.last_proc = None
    result = fake_engine.run({"image": "alpine", "detach": False}, on_output=lambda *_: None)
    assert result["returncode"] == 0
    assert FakeSandbox.last_proc is not None
    assert FakeSandbox.last_proc.stdin_closed is True


def test_exec_streams_ordered_base64_then_exit(client):
    client.call("run", image="alpine", detach=True, name="vm2")
    frames = []
    result = client.stream(
        "exec", lambda stream, data: frames.append((stream, data)), name="vm2", cmd=["echo", "hi"]
    )
    assert frames == [("stdout", b"hello\n"), ("stdout", b"world\n")]
    assert result == {"returncode": 0}
    client.call("rm", name="vm2")


def test_exec_streams_stdout_stderr_bytes_faithfully(client, monkeypatch):
    def exec_bytes(self, *cmd, **kwargs):
        return FakeProc([("stdout", b"\x00\xffline\n"), ("stderr", b"\xfeerr\x00")], rc=7)

    monkeypatch.setattr(FakeSandbox, "exec", exec_bytes)
    client.call("run", image="alpine", detach=True, name="vm-bytes")
    frames = []
    result = client.stream(
        "exec", lambda stream, data: frames.append((stream, data)), name="vm-bytes", cmd=["bytes"]
    )
    assert sorted(frames) == sorted(
        [
            ("stdout", b"\x00\xffline\n"),
            ("stderr", b"\xfeerr\x00"),
        ]
    )
    assert result == {"returncode": 7}
    client.call("rm", name="vm-bytes")


def test_unknown_vm_raises_not_found(client):
    with pytest.raises(DaemonError) as excinfo:
        client.call("stop", name="ghost")
    assert excinfo.value.code == "not_found"


def test_resume_stopped_vm_raises_not_running(client):
    client.call("run", image="alpine", detach=True, name="vm-stopped")
    client.call("stop", name="vm-stopped")
    try:
        with pytest.raises(DaemonError) as excinfo:
            client.call("resume", name="vm-stopped")
        assert excinfo.value.code == "not_running"
    finally:
        client.call("rm", name="vm-stopped")


def test_terminate_preserves_status_returncode_across_ps_and_rehydrate(fake_engine):
    record = fake_engine.create({"name": "api-dead", "image": "alpine"})
    record.sandbox.returncode = 23
    try:
        terminated = fake_engine.terminate("api-dead")
        assert terminated.view()["status"] == "terminated"
        assert terminated.view()["returncode"] == 23

        rows = fake_engine.ps()
        assert rows == [
            {
                "name": "api-dead",
                "status": "terminated",
                "pid": record.pid,
                "source": "alpine",
            }
        ]
        assert terminated.view()["status"] == "terminated"
        assert terminated.view()["returncode"] == 23

        rehydrated = core.Engine()
        revived = rehydrated.get("api-dead")
        assert revived.view()["status"] == "terminated"
        assert revived.view()["returncode"] == 23
    finally:
        with suppress(core.EngineError):
            fake_engine.remove("api-dead")


def test_shell_boots_ephemeral_streams_and_cleans_up(client):
    frames = []
    result = client.interactive(
        "shell", lambda s, d: frames.append((s, d)), tty=False, image="alpine"
    )
    assert result["returncode"] == 0
    assert result["name"]  # the daemon reports the ephemeral VM's name
    assert ("stdout", b"hello\n") in frames
    # Ephemeral: the VM is gone by the time the result is delivered.
    assert client.call("ps") == {"vms": []}


def test_shell_cleanup_failure_is_not_reported_as_success(client, fake_engine, monkeypatch):
    def cleanup():
        raise RuntimeError("cleanup boom")

    def start_shell(params):
        return FakeProc([], rc=0), "ephemeral", cleanup

    monkeypatch.setattr(fake_engine, "start_shell", start_shell)
    with pytest.raises(DaemonError) as excinfo:
        client.interactive("shell", lambda stream, data: None, tty=False, image="alpine")
    assert excinfo.value.code == "internal"
    assert "cleanup boom" in excinfo.value.message


def test_start_shell_attaches_without_removing(fake_engine):
    fake_engine.run({"image": "alpine", "detach": True, "name": "live"})
    proc, name, cleanup = fake_engine.start_shell({"ref": "live"})
    assert name == "live"
    cleanup()  # attach cleanup is a no-op
    assert [r["name"] for r in fake_engine.ps()] == ["live"]


def test_start_shell_image_path_records_then_cleanup_removes(fake_engine):
    proc, name, cleanup = fake_engine.start_shell({"image": "alpine"})
    assert name in [r["name"] for r in fake_engine.ps()]
    cleanup()
    assert name not in [r["name"] for r in fake_engine.ps()]


def test_start_shell_uses_block_network_for_fresh_sandbox(fake_engine):
    _proc, _name, cleanup = fake_engine.start_shell({"image": "alpine"})
    try:
        assert FakeSandbox.last_create_kwargs is not None
        assert FakeSandbox.last_create_kwargs["block_network"] is True
    finally:
        cleanup()


class _PtyStream:
    """A stdin/stdout stand-in backed by a PTY slave fd, with a byte ``buffer``.

    ``buffer`` mirrors a real ``sys.stdout.buffer`` so ``cli._write_event`` (the
    exact path ``vmon shell`` uses to paint output) writes bytes straight to the
    terminal — letting the test read them back from the PTY master.
    """

    class _Buffer:
        def __init__(self, fd):
            self._fd = fd

        def write(self, data):
            os.write(self._fd, bytes(data))

        def flush(self):
            pass

    def __init__(self, fd):
        self._fd = fd
        self.buffer = self._Buffer(fd)

    def fileno(self):
        return self._fd

    def isatty(self):
        return True


class _RecordingProc:
    """Exec proc that records stdin/resize and exits once a line is typed."""

    def __init__(self):
        self.stdout = ByteStream()
        self.stderr = ByteStream()
        self.stdin_data = bytearray()
        self.resizes = []
        self.returncode = None
        self._exit = threading.Event()
        self.stdout.feed(b"vm$ ")  # a prompt, like an interactive shell

    def write_stdin(self, data):
        self.stdin_data += bytes(data)
        if b"\n" in bytes(data):
            self.stdout.feed(b"ok\n")
            self._finish()

    def close_stdin(self):
        self._finish()

    def resize(self, rows, cols):
        self.resizes.append((rows, cols))

    def kill(self, *a):
        self._finish()

    def _finish(self):
        if not self._exit.is_set():
            self.stdout.close()
            self.stderr.close()
            self.returncode = 0
            self._exit.set()

    def wait(self, timeout=None):
        self._exit.wait(timeout)
        return 0


def test_interactive_pty_round_trip_raw_mode_and_resize(client, monkeypatch):
    """A real PTY exercises raw mode, the initial resize, stdin forwarding, and
    terminal restoration end to end against the in-process daemon.

    ``interactive()`` runs on the main thread (so its ``SIGWINCH`` handler can
    install); a feeder writes ``hi\\n`` once the shell is live, which the
    recording proc treats as end-of-input. A watchdog kills the proc so a
    regression can never hang the suite.
    """
    import select
    import termios

    import vmon.cli as cli

    pty = pytest.importorskip("pty")
    master, slave = pty.openpty()
    # A bare pty reports a 0x0 winsize; seed a realistic size so the client's
    # initial resize forwards a non-zero terminal size like a real TTY.
    import fcntl
    import struct
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
    proc = _RecordingProc()
    monkeypatch.setattr(FakeSandbox, "exec", lambda self, *c, **k: proc)
    monkeypatch.setattr(sys, "stdin", _PtyStream(slave))
    monkeypatch.setattr(sys, "stdout", _PtyStream(slave))
    before = termios.tcgetattr(slave)

    # Drain the master so the guest's stdout (written to the slave via the real
    # cli._write_event -> sys.stdout.buffer path) lands somewhere we can assert.
    seen = bytearray()
    reading = threading.Event()
    reading.set()

    def drain():
        while reading.is_set():
            r, _, _ = select.select([master], [], [], 0.1)
            if r:
                try:
                    seen.extend(os.read(master, 65536))
                except OSError:
                    return

    drainer = threading.Thread(target=drain, daemon=True)
    drainer.start()

    live = threading.Event()
    # Feed input only after raw mode is established (setraw's TCSAFLUSH has run),
    # else the typed bytes are discarded before the forwarder reads them.
    threading.Thread(target=lambda: live.wait(5) and os.write(master, b"hi\n"), daemon=True).start()
    threading.Thread(target=lambda: (time.sleep(8), proc.kill()), daemon=True).start()
    try:
        result = client.interactive(
            "shell", cli._write_event, tty=True, on_ready=lambda name: live.set(), image="alpine"
        )
    finally:
        termios.tcsetattr(slave, termios.TCSANOW, before)
        reading.clear()
        drainer.join(timeout=2)
        os.close(master)
        os.close(slave)

    assert result["returncode"] == 0
    assert bytes(proc.stdin_data) == b"hi\n"  # raw stdin was forwarded
    assert proc.resizes and proc.resizes[0][0] > 0  # initial winsize was sent
    assert b"vm$ " in bytes(seen) and b"ok" in bytes(seen)  # output reached the TTY


def test_autostart_spawns_daemon(short_home):
    """A bare client with no running daemon spawns one and binds the socket."""
    client = DaemonClient()
    try:
        assert client.call("ps") == {"vms": []}
        assert daemon_paths()["sock"].exists()
        # A second client reuses the same daemon (no double-spawn / rebind error).
        assert client.call("ping") == {"pong": True}
    finally:
        with suppress(DaemonError):
            client.stop_daemon()


def test_single_owner_second_daemon_exits_without_rebinding(short_home):
    paths = daemon_paths()
    paths["home"].mkdir(parents=True, exist_ok=True)
    lock_fd = acquire_owner_lock(paths["lock"])
    assert lock_fd is not None
    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "vmon.daemon"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        assert proc.wait(timeout=10) == 0
        # Lock was held, so the second daemon must not have bound the socket.
        assert not paths["sock"].exists()
    finally:
        release_owner_lock(lock_fd)


def test_rehydrate_lists_disk_vms_and_reacquires_volume_locks(monkeypatch):
    monkeypatch.setattr("vmon.core.Engine._sandbox_class", staticmethod(lambda: FakeSandbox))
    child = subprocess.Popen(["sleep", "300"])
    try:
        MicroVM("rvm")._save_meta(
            pid=child.pid,
            image="alpine",
            sandbox=True,
            status="running",
            volumes={"/data": {"name": "vol_r", "tag": "vol_r", "read_only": False}},
        )
        Volume("vol_r").ensure()

        engine = core.Engine()  # rehydrates from disk

        rows = engine.ps()
        assert any(r["name"] == "rvm" and r["status"] == "running" for r in rows)

        # The running VM's volume lock must have been re-acquired by the engine,
        # so an independent acquire now fails.
        with pytest.raises(RuntimeError):
            Volume("vol_r").acquire()
    finally:
        child.terminate()
        with suppress(Exception):
            child.wait(timeout=2)
