"""CLI tests: Modal-style help rendering, argument parsing, and command routing.

These exercise :mod:`vmon.cli` against a recording fake of
:class:`vmon.client.DaemonClient`, so no daemon or VM is involved — they assert
how the CLI parses args and which daemon method/params it would send.
"""

import pytest

import vmon.cli as cli


class RecordingClient:
    """Stand-in for ``DaemonClient`` that records the last request it received."""

    last: dict = {}
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
        RecordingClient.last = {"method": method, "params": params, "stream": True}
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


def test_run_without_image_or_dockerfile_is_usage_error(rec, capsys):
    assert cli.main(["run"]) == 2
    assert "image" in capsys.readouterr().err.lower()


# -- exec: tty routing -----------------------------------------------------


def test_exec_without_tty_streams_and_forwards_stdin(rec):
    assert cli.main(["exec", "vm1", "--", "sh", "-lc", "echo hi"]) == 0
    assert rec.last["method"] == "exec"
    assert rec.last.get("stream") is True
    assert rec.last["params"]["cmd"] == ["sh", "-lc", "echo hi"]


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
