"""CLI tests: Modal-style help rendering, argument parsing, and command routing.

These exercise :mod:`vmon.cli` against a recording fake of
:class:`vmon.client.DaemonClient`, so no daemon or VM is involved — they assert
how the CLI parses args and which daemon method/params it would send.
"""

import os
import threading
import time

import pytest

import vmon.cli as cli
import vmon.client as client


class RecordingClient:
    """Stand-in for ``DaemonClient`` that records the last request it received."""

    last: dict[str, object] = {}
    returncode = 0

    def __init__(self, *args, **kwargs):
        pass

    def call(self, method, **params):
        RecordingClient.last = {"method": method, "params": params}
        return {
            "name": params.get("name") or "vm",
            "pid": 1,
            "image": params.get("image"),
            "socket": "/tmp/x/vmond.sock",
            "version": "1",
            "dir": "/snap",
            "clones": [],
            "reconstruct_ms": 5,
            "restore_ms": 9,
        }

    def stream(self, method, on_event, stdin=None, **params):
        RecordingClient.last = {"method": method, "params": params, "stream": True, "stdin": stdin}
        return {"returncode": RecordingClient.returncode}

    def interactive(self, method, on_event, *, tty=True, on_ready=None, **params):
        RecordingClient.last = {"method": method, "params": params, "tty": tty}
        if on_ready is not None:
            on_ready("vm")
        return {"returncode": RecordingClient.returncode, "name": "vm"}

    def ensure_running(self):
        return {"pid": 1, "socket": "/tmp/x/vmond.sock"}

    def stop_daemon(self):
        return {"pid": 1}


@pytest.fixture
def rec(monkeypatch):
    monkeypatch.setattr(cli, "DaemonClient", RecordingClient)
    RecordingClient.last = {}
    RecordingClient.returncode = 0
    return RecordingClient


# -- help rendering --------------------------------------------------------


def test_group_help_renders_grouped_panels(capsys):
    rc = cli.main(["--help"])
    out = capsys.readouterr().out
    assert rc == 0
    # Modal-style grouped panels, in declaration order.
    for title in ("Commands", "Lifecycle", "Snapshots", "Daemon"):
        assert title in out
    for command in ("run", "shell", "exec", "snapshot", "fork"):
        assert command in out


def test_command_help_has_arguments_and_options(capsys):
    rc = cli.main(["shell", "--help"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Arguments" in out and "Options" in out
    assert "--pty" in out and "--no-pty" in out


def test_version(capsys):
    rc = cli.main(["--version"])
    assert rc == 0
    assert "vmon" in capsys.readouterr().out


# -- run: REMAINDER parsing ------------------------------------------------


def test_run_strips_dashdash_separator(rec):
    assert cli.main(["run", "alpine", "--", "sh", "-c", "echo hi"]) == 0
    assert rec.last["method"] == "run"
    assert rec.last["params"]["image"] == "alpine"
    assert rec.last["params"]["cmd"] == ["sh", "-c", "echo hi"]


def test_run_captures_trailing_command_and_its_flags(rec):
    # allow_interspersed_args=False: everything after the image is the command,
    # including its own ``-la`` flag, even without a ``--`` separator.
    assert cli.main(["run", "alpine", "ls", "-la"]) == 0
    assert rec.last["params"]["cmd"] == ["ls", "-la"]


def test_run_detach_uses_call(rec):
    assert cli.main(["run", "-d", "--name", "web", "nginx"]) == 0
    assert rec.last["method"] == "run"
    assert rec.last["params"]["detach"] is True
    assert rec.last["params"]["name"] == "web"


def test_run_block_network_flag(rec):
    # The flag must precede the image: `run` uses allow_interspersed_args=False,
    # so anything after IMAGE is captured as the guest command, not a CLI option.
    assert cli.main(["run", "alpine"]) == 0
    assert rec.last["params"]["block_network"] is False
    assert cli.main(["run", "--block-network", "alpine"]) == 0
    assert rec.last["params"]["block_network"] is True


def test_run_without_image_or_dockerfile_is_usage_error(rec, capsys):
    assert cli.main(["run"]) == 2
    assert "image" in capsys.readouterr().err.lower()


# -- exec: tty routing -----------------------------------------------------


def test_exec_without_tty_forwards_real_stdin(rec, monkeypatch):
    import io
    import sys
    import types

    # A non-interactive stdin (pipe/redirect) must be forwarded to the guest.
    buf = io.BytesIO(b"piped-input")
    monkeypatch.setattr(sys, "stdin", types.SimpleNamespace(isatty=lambda: False, buffer=buf))
    assert cli.main(["exec", "vm1", "--", "sh", "-lc", "echo hi"]) == 0
    assert rec.last["method"] == "exec"
    assert rec.last.get("stream") is True
    assert rec.last["params"]["cmd"] == ["sh", "-lc", "echo hi"]
    assert rec.last["stdin"] is buf


def test_exec_tty_terminal_sends_immediate_eof_stdin(rec, monkeypatch):
    import sys
    import types

    # A non-`-t` exec from an interactive terminal must NOT forward the TTY (the
    # reader thread would block at interpreter shutdown -> SIGABRT). Instead an
    # empty, immediate-EOF stream is sent so the guest's stdin is closed and
    # readers like `cat` exit rather than hang.
    monkeypatch.setattr(sys, "stdin", types.SimpleNamespace(isatty=lambda: True))
    assert cli.main(["exec", "vm1", "--", "sh", "-lc", "echo hi"]) == 0
    assert rec.last.get("stream") is True
    stdin = rec.last["stdin"]
    assert stdin is not None
    assert stdin.read() == b""


def test_forward_stdin_fd_source_stops_without_eof():
    class FakeConn:
        def __init__(self):
            self.chunks = []
            self.eof = False

        def send_stdin(self, chunk):
            self.chunks.append(chunk)

        def send_eof(self):
            self.eof = True

    rfd, wfd = os.pipe()
    stdin = os.fdopen(rfd, "rb", buffering=0)
    stop = threading.Event()
    conn = FakeConn()
    worker = threading.Thread(
        target=client.DaemonClient._forward_stdin,
        args=(conn, stdin, stop),
    )
    try:
        worker.start()
        time.sleep(0.1)
        stop.set()
        worker.join(timeout=2)
        assert not worker.is_alive()
        assert conn.chunks == []
        assert conn.eof is False
    finally:
        stop.set()
        os.close(wfd)
        stdin.close()
        if worker.is_alive():
            worker.join(timeout=1)


def test_exec_tty_routes_to_interactive(rec):
    assert cli.main(["exec", "-t", "vm1", "--", "bash"]) == 0
    assert rec.last["method"] == "exec"
    assert rec.last["tty"] is True
    assert rec.last["params"]["cmd"] == ["bash"]


def test_exec_requires_command(rec, capsys):
    assert cli.main(["exec", "vm1"]) == 2
    assert "command" in capsys.readouterr().err.lower()


# -- shell -----------------------------------------------------------------


def test_shell_wraps_cmd_and_disables_pty_without_tty(rec):
    # stdin/stdout are not a TTY under pytest, so PTY auto-resolves off.
    rec.returncode = 7
    assert cli.main(["shell", "--image", "alpine", "-c", "echo hi"]) == 7
    assert rec.last["method"] == "shell"
    assert rec.last["tty"] is False
    assert rec.last["params"]["image"] == "alpine"
    assert rec.last["params"]["cmd"] == ["/bin/sh", "-c", "echo hi"]


def test_shell_empty_cmd_is_still_a_command(rec):
    assert cli.main(["shell", "--image", "alpine", "-c", ""]) == 0
    assert rec.last["params"]["cmd"] == ["/bin/sh", "-c", ""]


def test_shell_default_command_without_pty_needs_tty_fails(rec, capsys):
    assert cli.main(["shell", "myvm"]) == 2
    assert "terminal" in capsys.readouterr().err.lower()


def test_shell_default_command_with_no_pty_works(rec):
    rec.returncode = 9
    assert cli.main(["shell", "myvm", "--no-pty"]) == 9
    assert rec.last["params"]["ref"] == "myvm"
    assert rec.last["params"]["cmd"] is None  # daemon picks bash/sh
    assert rec.last["tty"] is False


def test_shell_default_command_is_interactive_shell_mocked_tty(rec, monkeypatch):
    import sys

    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    assert cli.main(["shell", "myvm"]) == 0
    assert rec.last["params"]["ref"] == "myvm"
    assert rec.last["params"]["cmd"] is None  # daemon picks bash/sh
    assert rec.last["tty"] is True


def test_shell_force_pty(rec):
    assert cli.main(["shell", "--pty", "--image", "alpine"]) == 0
    assert rec.last["tty"] is True


def test_shell_env_pairs_parsed(rec):
    assert cli.main(["shell", "--image", "alpine", "--no-pty", "-e", "A=1", "-e", "B=2"]) == 0
    assert rec.last["params"]["env"] == {"A": "1", "B": "2"}


def test_shell_bare_env_copies_host_value(rec, monkeypatch):
    monkeypatch.setenv("FROM_HOST", "copied")
    assert cli.main(["shell", "--image", "alpine", "--no-pty", "-e", "FROM_HOST"]) == 0
    assert rec.last["params"]["env"] == {"FROM_HOST": "copied"}


def test_shell_bare_env_missing_is_usage_error(rec, monkeypatch, capsys):
    monkeypatch.delenv("VMON_TEST_MISSING", raising=False)
    assert cli.main(["shell", "--image", "alpine", "--no-pty", "-e", "VMON_TEST_MISSING"]) == 2
    assert "not set" in capsys.readouterr().err.lower()


def test_shell_bad_env_is_usage_error(rec, capsys):
    assert cli.main(["shell", "--image", "alpine", "--no-pty", "-e", "=bad"]) == 2
    assert "env" in capsys.readouterr().err.lower()


# -- misc ------------------------------------------------------------------


def test_unknown_command_is_usage_error(capsys):
    assert cli.main(["bogus"]) == 2


def test_daemon_status_reports_socket(rec, capsys):
    assert cli.main(["daemon", "status"]) == 0
    out = capsys.readouterr().out
    assert "running" in out and "vmond.sock" in out


# -- inspect / stats / extend ----------------------------------------------


def test_inspect_routes_to_inspect(rec):
    assert cli.main(["inspect", "vm1"]) == 0
    assert rec.last["method"] == "inspect"
    assert rec.last["params"]["name"] == "vm1"


def test_stats_routes_to_metrics(rec):
    assert cli.main(["stats", "vm1"]) == 0
    assert rec.last["method"] == "metrics"
    assert rec.last["params"]["name"] == "vm1"


def test_extend_routes_with_int_secs(rec):
    assert cli.main(["extend", "vm1", "120"]) == 0
    assert rec.last["method"] == "extend"
    assert rec.last["params"] == {"name": "vm1", "secs": 120}


def test_extend_rejects_non_int_secs(rec, capsys):
    assert cli.main(["extend", "vm1", "abc"]) == 2
