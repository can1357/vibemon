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
import vmon.doctor as doctor


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


def test_run_without_image_is_usage_error(rec, capsys):
    assert cli.main(["run"]) == 2
    assert "image" in capsys.readouterr().err.lower()


def test_run_dockerfile_fails_before_rpc(rec, capsys):
    assert cli.main(["run", "-f", "Dockerfile"]) == 2
    err = capsys.readouterr().err
    assert "Dockerfile builds are not supported" in err
    assert rec.last == {}


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


def test_collect_checks_reports_missing_vmm(monkeypatch, tmp_path):
    def missing_binary():
        raise RuntimeError("missing")

    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.setattr(doctor, "find_binary", missing_binary)
    monkeypatch.setattr(doctor, "default_kernel", lambda: str(tmp_path / "kernel"))
    monkeypatch.setattr(doctor.platform, "system", lambda: "Other")
    monkeypatch.setattr(doctor.platform, "machine", lambda: "arm64")
    monkeypatch.setattr(doctor.shutil, "which", lambda name: None)

    checks = {check.name: check for check in doctor.collect_checks()}
    assert checks["vmm binary"].status == "fail"
    assert "cargo build" in checks["vmm binary"].hint
    assert checks["image tools"].status == "warn"
    assert checks["mkfs.ext4"].status == "warn"


def test_collect_checks_reports_present_prereqs(monkeypatch, tmp_path):
    vmm = tmp_path / "vmm"
    kernel = tmp_path / "kernel"
    vmm.touch()
    kernel.touch()

    def which(name):
        return {
            "skopeo": "/usr/bin/skopeo",
            "umoci": "/usr/bin/umoci",
            "mkfs.ext4": "/sbin/mkfs.ext4",
        }.get(name)

    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.setattr(doctor, "find_binary", lambda: str(vmm))
    monkeypatch.setattr(doctor, "default_kernel", lambda: str(kernel))
    monkeypatch.setattr(doctor.platform, "system", lambda: "Other")
    monkeypatch.setattr(doctor.platform, "machine", lambda: "arm64")
    monkeypatch.setattr(doctor.shutil, "which", which)

    checks = {check.name: check for check in doctor.collect_checks()}
    assert checks["vmm binary"].status == "ok"
    assert checks["vmm binary"].detail == str(vmm)
    assert checks["image tools"].status == "ok"
    assert checks["mkfs.ext4"].status == "ok"
    assert checks["guest kernel"].status == "ok"


def test_doctor_command_returns_failure_status(monkeypatch):
    monkeypatch.setattr(doctor.ui, "doctor_table", lambda checks: None)
    monkeypatch.setattr(doctor, "collect_checks", lambda: [doctor.Check("env", "ok", "ready")])
    assert cli.main(["doctor"]) == 0

    monkeypatch.setattr(
        doctor, "collect_checks", lambda: [doctor.Check("vmm binary", "fail", "missing")]
    )
    assert cli.main(["doctor"]) == 1


def test_doctor_table_renders_statuses_and_hints(capsys):
    from vmon import console

    console.doctor_table([doctor.Check("vmm binary", "fail", "missing", "cargo build")])
    out = capsys.readouterr().out
    assert "fail" in out
    assert "vmm binary" in out
    assert "cargo build" in out


def test_completion_scripts_and_bad_shell(capsys):
    assert cli.main(["completion", "bash"]) == 0
    bash_out = capsys.readouterr().out
    assert "_VMON_COMPLETE" in bash_out
    assert 'eval "$(vmon completion bash)"' in bash_out

    assert cli.main(["completion", "fish"]) == 0
    fish_out = capsys.readouterr().out
    assert "complete --no-files --command vmon" in fish_out

    assert cli.main(["completion", "nope"]) == 2
    assert "unsupported shell" in capsys.readouterr().err


def test_daemon_start_failure_hints_doctor(monkeypatch, capsys):
    class FailingClient:
        def __init__(self, *args, **kwargs):
            pass

        def call(self, method, **params):
            raise client.DaemonError("vmond exited (code 1) on startup", code="start_failed")

    monkeypatch.setattr(cli, "DaemonClient", FailingClient)
    assert cli.main(["ps"]) == 1
    captured = capsys.readouterr()
    assert "vmond exited" in captured.err
    assert "vmon doctor" in captured.out


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


# -- ls --------------------------------------------------------------------


def test_ls_routes_to_fs_list(rec):
    assert cli.main(["ls", "vm1:/etc"]) == 0
    assert rec.last["method"] == "fs_list"
    assert rec.last["params"] == {"name": "vm1", "path": "/etc"}


def test_ls_defaults_path_to_root(rec):
    assert cli.main(["ls", "vm1"]) == 0
    assert rec.last["params"] == {"name": "vm1", "path": "/"}


def test_ls_requires_a_name(rec):
    assert cli.main(["ls", ":/etc"]) == 2


def test_ls_falls_back_to_stat_for_a_file(monkeypatch, capsys):
    """``ls NAME:FILE`` stats the path when listing it as a directory fails."""

    class FileClient:
        def __init__(self, *a, **k):
            pass

        def call(self, method, **params):
            if method == "fs_list":
                raise client.DaemonError("fs_list /etc/hosts: Not a directory")
            assert method == "fs_stat"
            return {"type": "file", "size": 12, "mode": 0o100644, "mtime": 0}

    monkeypatch.setattr(cli, "DaemonClient", FileClient)
    assert cli.main(["ls", "vm1:/etc/hosts"]) == 0
    assert "hosts" in capsys.readouterr().out


def test_ls_reports_error_for_a_missing_path(monkeypatch, capsys):
    class MissingClient:
        def __init__(self, *a, **k):
            pass

        def call(self, method, **params):
            raise client.DaemonError("fs_stat /nope: No such file or directory")

    monkeypatch.setattr(cli, "DaemonClient", MissingClient)
    assert cli.main(["ls", "vm1:/nope"]) == 1
    assert "No such file" in capsys.readouterr().err


def test_fs_table_renders_dirs_first_with_modes_and_sizes(capsys):
    """The listing sorts directories first and mirrors ``ls -l`` columns."""
    from vmon import console

    entries = [
        {"name": "zzz.txt", "type": "file", "size": 2048, "mode": 0o100644, "mtime": 1_700_000_000},
        {"name": "run.sh", "type": "file", "size": 10, "mode": 0o100755, "mtime": 0},
        {"name": "adir", "type": "dir", "size": 0, "mode": 0o40755, "mtime": 0},
    ]
    console.fs_table("vm", "/", entries)
    out = capsys.readouterr().out
    assert out.index("adir") < out.index("run.sh") < out.index("zzz.txt")
    assert "adir/" in out and "run.sh*" in out
    assert "drwxr-xr-x" in out and "2.0K" in out
    assert "2023-" in out


def test_stats_table_flattens_metric_groups(capsys):
    """Nested VMM counter groups render as ``group.field`` rows, not dict reprs."""
    from vmon import console

    metrics = {
        "boot_duration_ms": 142,
        "control_requests": 12,
        "vm_exits": {"io_in": 3, "total": 17},
        "pager": {"resident_pages": 1280},
    }
    console.stats_table("vm", metrics)
    out = capsys.readouterr().out
    assert "vm_exits.total" in out and "vm_exits.io_in" in out
    assert "pager.resident_pages" in out
    assert "boot_duration_ms" in out
    # The whole point: groups are expanded, never dumped as a Python dict repr.
    assert "{" not in out and "}" not in out


def test_mesh_migrate_cli_posts_to_gateway(monkeypatch):
    """``vmon mesh migrate NAME NODE`` calls the per-sandbox migrate gateway route."""
    import vmon.cli as cli

    calls = []

    def fake_call(method, path, payload=None):
        calls.append((method, path, payload))
        return {}

    monkeypatch.setattr(cli, "_mesh_call", fake_call)
    assert cli.main(["mesh", "migrate", "web", "nodeB"]) == 0
    assert calls == [("POST", "/v1/sandboxes/web/migrate", {"target": "nodeB"})]


def test_mesh_leave_drain_cli_passes_flag(monkeypatch):
    """``vmon mesh leave --drain`` asks the gateway to drain before leaving."""
    import vmon.cli as cli

    calls = []

    def fake_call(method, path, payload=None):
        calls.append((method, path, payload))
        return {"drained": []}

    monkeypatch.setattr(cli, "_mesh_call", fake_call)
    assert cli.main(["mesh", "leave", "--drain"]) == 0
    assert calls == [("POST", "/v1/mesh/leave", {"drain": True})]
