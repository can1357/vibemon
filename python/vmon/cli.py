"""`vmon` — a Modal-style CLI that drives microVMs through the ``vmond`` daemon.

Every command is a request to a local daemon (auto-started on first use); the
daemon owns the registry and spawns the per-VM ``vmon`` VMM processes. The CLI
speaks to :class:`vmon.client.DaemonClient` and renders everything through
:mod:`vmon.console` (the only place :mod:`click`/:mod:`rich` are imported), so the
SDK and daemon stay dependency-light.
"""

import io
import sys
from pathlib import Path
from typing import BinaryIO, cast

import click

from . import __version__
from . import console as ui
from .client import DaemonClient, DaemonError, GatewayClient
from .console import RichGroup

# ``-h`` as well as ``--help``; PTY/REMAINDER commands stop option parsing at the
# first positional so a trailing command keeps its own flags (Docker-style).
_CTX = {"help_option_names": ["-h", "--help"]}
_REMAINDER = {**_CTX, "ignore_unknown_options": True, "allow_interspersed_args": False}

# click ≥8.2 reports an empty group invocation (``no_args_is_help``) as a
# ``NoArgsIsHelpError`` whose message *is* the rendered help. Empty on click 8.1,
# where the same case echoes help and exits 0 (an ``isinstance`` no-op).
_NO_ARGS_HELP = getattr(click.exceptions, "NoArgsIsHelpError", ())


def _write_event(stream: str, data: bytes) -> None:
    """Write a streamed daemon frame to stdout/stderr (bytes-faithful)."""
    target = sys.stderr if stream == "stderr" else sys.stdout
    buffer = getattr(target, "buffer", None)
    if buffer is not None:
        buffer.write(data)
        buffer.flush()
    else:
        target.write(data.decode("utf-8", "replace"))
        target.flush()


def _strip_dashdash(cmd: tuple[str, ...]) -> list[str]:
    """Drop a leading ``--`` separator click may leave in REMAINDER args."""
    argv = list(cmd)
    if argv and argv[0] == "--":
        argv = argv[1:]
    return argv


def _parse_env(pairs: tuple[str, ...]) -> dict[str, str]:
    """``KEY=VALUE`` injects a value; bare ``KEY`` copies it from the host env."""
    import os

    env: dict[str, str] = {}
    for item in pairs:
        key, sep, value = item.partition("=")
        if not key:
            raise click.BadParameter(f"invalid --env {item!r}; expected KEY=VALUE")
        if sep:
            env[key] = value
            continue
        if key not in os.environ:
            raise click.BadParameter(f"host env {key!r} is not set")
        env[key] = os.environ[key]
    return env


def _exit_code(result: dict[str, object]) -> int:
    """Translate daemon ``returncode`` into a valid process exit status."""
    value = result.get("returncode")
    if not isinstance(value, (int, str)):
        return 1
    try:
        code = int(value)
    except ValueError:
        return 1
    if code < 0:
        return min(255, 128 + abs(code))
    return min(255, code)


def _parse_remote(spec: str) -> tuple[str, str] | None:
    if ":" not in spec:
        return None
    name, path = spec.split(":", 1)
    if not name or not path:
        return None
    return name, path


def _mesh_call(
    method: str, path: str, payload: dict[str, object] | None = None
) -> dict[str, object]:
    """Call the local HTTP gateway's mesh API using the shared bearer token."""
    import json
    import os
    import urllib.error
    import urllib.request

    base = os.environ.get("VMON_SERVER", "http://127.0.0.1:8000").rstrip("/")
    token = os.environ.get("VMON_API_TOKEN")
    if not token:
        raise click.ClickException("VMON_API_TOKEN is required for mesh commands")
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise click.ClickException(f"mesh API error {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise click.ClickException(
            f"cannot reach local gateway at {base}: {exc.reason}; is `vmon serve` running?"
        ) from exc
    return json.loads(raw or b"{}")


def _client() -> DaemonClient | GatewayClient:
    """Select daemon or gateway transport for one CLI request."""
    import json
    import os

    if os.environ.get("VMON_REMOTE"):
        return DaemonClient()
    server = os.environ.get("VMON_SERVER")
    if server:
        return GatewayClient(server)
    home = Path(os.environ.get("VMON_HOME", str(Path.home() / ".vmon")))
    mesh_path = home / "mesh.json"
    if mesh_path.exists():
        try:
            data = json.loads(mesh_path.read_text(encoding="utf-8"))
        except OSError, json.JSONDecodeError:
            data = {}
        if data.get("enabled") is True and data.get("advertise"):
            return GatewayClient(str(data["advertise"]))
    return DaemonClient()


@click.group(
    cls=RichGroup,
    context_settings=_CTX,
    help="Hardware-isolated microVMs: boot containers, drop into a shell, "
    "snapshot, and fork — all through a zero-config local daemon.",
)
@click.version_option(__version__, "-v", "--version", message="vmon %(version)s")
def cli() -> None:
    pass


# -- core commands ---------------------------------------------------------


@cli.command(
    panel="Commands",
    context_settings=_REMAINDER,
    help="Boot a container image or Dockerfile as an agent sandbox and "
    "stream its output (use -d to leave it running in the background).",
)
@click.argument("image", required=False)
@click.argument("cmd", nargs=-1, type=click.UNPROCESSED, metavar="[-- CMD ...]")
@click.option("-f", "--dockerfile", help="build & run this Dockerfile instead of an image")
@click.option("--context", default=".", show_default=True, help="Docker build context")
@click.option("--name", help="microVM name (default: auto)")
@click.option("--mem", type=int, default=512, show_default=True, help="guest RAM in MiB")
@click.option("--cpus", type=int, default=1, show_default=True, help="vCPUs")
@click.option(
    "--disk-mb", type=int, default=1024, show_default=True, help="sandbox disk size in MiB"
)
@click.option(
    "--timeout", type=float, default=300.0, show_default=True, help="create timeout in seconds"
)
@click.option("-d", "--detach", is_flag=True, help="run in background")
@click.option(
    "--block-network",
    is_flag=True,
    help="boot without networking (no NIC; needs no root and works on macOS)",
)
def run(image, cmd, dockerfile, context, name, mem, cpus, disk_mb, timeout, detach, block_network):
    if not image and not dockerfile:
        raise click.UsageError("provide an image (vmon run alpine) or -f Dockerfile")
    client = _client()
    params = {
        "image": image,
        "dockerfile": dockerfile,
        "context": context,
        "name": name,
        "mem": mem,
        "cpus": cpus,
        "disk_mb": disk_mb,
        "timeout": timeout,
        "cmd": _strip_dashdash(cmd) or None,
        "detach": detach,
        "block_network": block_network,
    }
    if detach:
        r = client.call("run", **params)
        vm = r.get("name")
        ui.success(f"started [vmon.command]{vm}[/] (pid {r.get('pid')})  image={r.get('image')}")
        ui.hint(f"vmon exec {vm} … │ vmon snapshot {vm} <tpl> │ vmon stop {vm}")
        return 0
    r = client.stream("run", _write_event, **params)
    return _exit_code(r)


@cli.command(
    panel="Commands",
    context_settings=_CTX,
    help="Run a command or an interactive ephemeral shell inside a microVM.\n\n"
    "REF attaches to a running VM (instant) or warm-boots a snapshot; "
    "otherwise a fresh sandbox is booted from --image (or REF as an image, "
    "default debian) and removed on exit. A PTY is used automatically when "
    "attached to a terminal.",
)
@click.argument("ref", required=False)
@click.option("-c", "--cmd", "command", help="command to run (default: an interactive shell)")
@click.option("--image", help="container image for a fresh shell (if REF is not given)")
@click.option(
    "-e",
    "--env",
    "env",
    multiple=True,
    metavar="KEY=VALUE",
    help="set an env var (repeatable; bare KEY copies from the host)",
)
@click.option("--mem", type=int, default=512, show_default=True, help="guest RAM in MiB")
@click.option("--cpus", type=int, default=1, show_default=True, help="vCPUs")
@click.option(
    "--disk-mb", type=int, default=1024, show_default=True, help="sandbox disk size in MiB"
)
@click.option(
    "--timeout", type=float, default=300.0, show_default=True, help="create timeout in seconds"
)
@click.option("--pty/--no-pty", default=None, help="force a PTY on/off (default: auto by TTY)")
def shell(ref, command, image, env, mem, cpus, disk_mb, timeout, pty):
    interactive = command is None  # no -c -> an interactive REPL
    if pty is None:
        tty = sys.stdin.isatty() and sys.stdout.isatty()
        if interactive and not tty:
            # A bare `vmon shell` is a REPL; without a terminal the guest shell
            # would read EOF and exit at once. Fail loudly instead of silently.
            ui.error("vmon shell needs a terminal for an interactive shell")
            ui.hint("run it in a TTY, pass -c '<cmd>' for a one-off, or --no-pty")
            return 2
    else:
        tty = pty
    argv = ["/bin/sh", "-c", command] if command is not None else None
    params = {
        "ref": ref,
        "image": image,
        "cmd": argv,
        "env": _parse_env(env) or None,
        "mem": mem,
        "cpus": cpus,
        "disk_mb": disk_mb,
        "timeout": timeout,
    }
    client = _client()
    spinner = (
        ui.console.status("[vmon.accent]Booting microVM…[/]", spinner="dots")
        if ui.console.is_terminal
        else None
    )
    state = {"ready": False}

    def on_ready(name: str) -> None:
        if spinner is not None and not state["ready"]:
            spinner.stop()
        state["ready"] = True

    if spinner is not None:
        spinner.start()
    try:
        r = client.interactive("shell", _write_event, tty=tty, on_ready=on_ready, **params)
    finally:
        if spinner is not None and not state["ready"]:
            spinner.stop()
    return _exit_code(r)


@cli.command(
    panel="Commands",
    context_settings=_REMAINDER,
    help="Run a command inside an agent-enabled microVM (use -t for a PTY).",
)
@click.argument("name")
@click.argument("cmd", nargs=-1, type=click.UNPROCESSED, metavar="-- CMD ...")
@click.option("-t", "--tty", is_flag=True, help="allocate a PTY (interactive)")
def exec(name, cmd, tty):
    argv = _strip_dashdash(cmd)
    if not argv:
        raise click.UsageError("exec requires a command (vmon exec NAME -- <cmd>)")
    client = _client()
    if tty:
        r = client.interactive("exec", _write_event, tty=True, name=name, cmd=argv)
        return _exit_code(r)
    # Pick the stdin source forwarded to the guest. A piped/redirected stdin is
    # forwarded as-is. An interactive terminal must NOT be: the daemon
    # stdin-reader thread would block on the TTY read at interpreter shutdown,
    # holding the buffer lock -> fatal `_enter_buffered_busy` / SIGABRT (rc=-6).
    # Use an empty stream so it forwards an immediate `eof` (closing the guest's
    # stdin so readers like `cat` don't hang) and the reader thread exits at
    # once. Matches `docker exec` without `-i`: stdin is closed, not interactive.
    stdin = (
        cast(BinaryIO, io.BytesIO(b""))
        if sys.stdin.isatty()
        else cast(BinaryIO, getattr(sys.stdin, "buffer", sys.stdin))
    )
    r = client.stream("exec", _write_event, stdin=stdin, name=name, cmd=argv)
    return _exit_code(r)


@cli.command(
    panel="Commands",
    context_settings=_CTX,
    help="Copy a file between host and guest: <name>:<path> <local> or reverse.",
)
@click.argument("src")
@click.argument("dst")
def cp(src, dst):
    import base64

    client = _client()
    src_remote = _parse_remote(src)
    dst_remote = _parse_remote(dst)
    if bool(src_remote) == bool(dst_remote):
        raise click.UsageError("cp requires exactly one remote operand of the form <name>:<path>")
    if src_remote:
        name, remote_path = src_remote
        data = base64.b64decode(client.call("cp_read", name=name, path=remote_path)["data"])
        out = Path(dst)
        if out.is_dir():
            out = out / Path(remote_path).name
        out.write_bytes(data)
    else:
        assert dst_remote is not None
        name, remote_path = dst_remote
        data = Path(src).read_bytes()
        client.call(
            "cp_write", name=name, path=remote_path, data=base64.b64encode(data).decode("ascii")
        )
    return 0


@cli.command(panel="Commands", context_settings=_CTX, help="List microVMs.")
def ps():
    vms = _client().call("ps").get("vms") or []
    if not vms:
        ui.console.print(
            "[vmon.muted]no microVMs[/] — boot one with [vmon.command]vmon run <image>[/]"
        )
        return 0
    ui.vm_table(vms)
    return 0


@cli.command(panel="Commands", context_settings=_CTX, help="Show a microVM's console.")
@click.argument("name")
@click.option("-f", "--follow", is_flag=True, help="follow output")
def logs(name, follow):
    _client().stream("logs", _write_event, name=name, follow=follow)
    return 0


# -- lifecycle -------------------------------------------------------------


@cli.command(panel="Lifecycle", context_settings=_CTX, help="Suspend a running microVM.")
@click.argument("name")
def pause(name):
    _client().call("pause", name=name)
    ui.success(f"paused [vmon.command]{name}[/]")
    return 0


@cli.command(panel="Lifecycle", context_settings=_CTX, help="Resume a suspended microVM.")
@click.argument("name")
def resume(name):
    _client().call("resume", name=name)
    ui.success(f"resumed [vmon.command]{name}[/]")
    return 0


@cli.command(panel="Lifecycle", context_settings=_CTX, help="Stop a running microVM.")
@click.argument("name")
def stop(name):
    _client().call("stop", name=name)
    ui.success(f"stopped [vmon.command]{name}[/]")
    return 0


@cli.command(panel="Lifecycle", context_settings=_CTX, help="Remove a microVM.")
@click.argument("name")
def rm(name):
    _client().call("rm", name=name)
    ui.success(f"removed [vmon.command]{name}[/]")
    return 0


# -- snapshots -------------------------------------------------------------


@cli.command(
    panel="Snapshots",
    context_settings=_CTX,
    help="Snapshot a microVM's machine state into a named template.",
)
@click.argument("name")
@click.argument("snapshot")
@click.option("--stop", is_flag=True, help="stop the VM after snapshotting")
def snapshot(name, snapshot, stop):
    r = _client().call("snapshot", name=name, snapshot=snapshot, stop=stop)
    ui.success(f"snapshot [vmon.command]{snapshot}[/] → {r.get('dir')}")
    return 0


@cli.command(
    panel="Snapshots", context_settings=_CTX, help="Warm-boot a microVM from a snapshot (<200ms)."
)
@click.argument("snapshot")
@click.option("--name", help="new VM name")
@click.option("--agent", is_flag=True, help="attach an agent socket")
@click.option("-d", "--detach", is_flag=True, help="run in background")
def restore(snapshot, name, agent, detach):
    client = _client()
    params = {"snapshot": snapshot, "name": name, "agent": agent, "detach": detach}
    r = (
        client.call("restore", **params)
        if detach
        else client.stream("restore", _write_event, **params)
    )
    rec = r.get("reconstruct_ms")
    rec_str = f"{rec}ms" if rec is not None else "n/a"
    ui.success(
        f"warm-booted [vmon.command]{r.get('name')}[/] from {snapshot}  "
        f"(reconstruct={rec_str}, end-to-end={r.get('restore_ms')}ms)"
    )
    return 0 if detach else _exit_code(r)


@cli.command(
    panel="Snapshots", context_settings=_CTX, help="Fork N copy-on-write clones from a snapshot."
)
@click.argument("snapshot")
@click.option("--count", type=int, default=2, show_default=True, help="number of clones")
def fork(snapshot, count):
    clones = _client().call("fork", snapshot=snapshot, count=count).get("clones") or []
    ui.info(f"forked {len(clones)} CoW clone(s) from [vmon.command]{snapshot}[/]:")
    for c in clones:
        # Name first so scripts can ``line.split()[0]``; keep the ``(pid`` marker.
        ui.console.print(
            f"[vmon.command]{c['name']}[/]  "
            f"(pid {c.get('pid')}, CoW reconstruct={c.get('reconstruct_ms')}ms)"
        )
    return 0


# -- mesh ------------------------------------------------------------------


@cli.group(
    cls=RichGroup,
    panel="Mesh",
    context_settings=_CTX,
    help="Form and manage a server cluster.",
)
def mesh() -> None:
    pass


@mesh.command(
    context_settings=_CTX, help="Initialize this node as a cluster and print a join blob."
)
@click.option("--advertise", help="URL peers use to reach this node")
@click.option("--region", default="", help="locality label for same-region routing")
@click.option("--max-vcpus", type=int, help="advertised vCPU capacity")
@click.option("--max-mem-mib", type=int, help="advertised RAM capacity in MiB")
def setup(advertise, region, max_vcpus, max_mem_mib):
    payload = {
        "advertise": advertise,
        "region": region,
        "max_vcpus": max_vcpus,
        "max_mem_mib": max_mem_mib,
    }
    result = _mesh_call("POST", "/v1/mesh/setup", payload)
    ui.success(f"cluster ready on {result['advertise']}")
    ui.info("share this with other nodes:")
    ui.hint(f"vmon mesh join {result['blob']}")
    return 0


@mesh.command(context_settings=_CTX, help="Join an existing cluster using its join blob.")
@click.argument("blob")
@click.option("--advertise", help="URL peers use to reach this node")
@click.option("--region", default="", help="locality label for same-region routing")
def join(blob, advertise, region):
    result = _mesh_call(
        "POST", "/v1/mesh/join", {"blob": blob, "advertise": advertise, "region": region}
    )
    ui.success(f"joined cluster ({len(result.get('peers', []))} peer(s))")
    return 0


@mesh.command(context_settings=_CTX, help="Show cluster members, health, and capacity.")
def status():
    ui.mesh_table(_mesh_call("GET", "/v1/mesh/status"))
    return 0


@mesh.command(context_settings=_CTX, help="Leave the cluster (does not migrate running VMs).")
def leave():
    _mesh_call("POST", "/v1/mesh/leave")
    ui.success("left the cluster")
    return 0


# -- daemon ----------------------------------------------------------------


@cli.command(panel="Daemon", context_settings=_CTX, help="Manage the local vmond daemon.")
@click.argument("action", type=click.Choice(["start", "stop", "status"]))
def daemon(action):
    if action == "status":
        try:
            info = DaemonClient(autostart=False).call("info")
        except DaemonError as e:
            if e.code in ("unreachable", "closed"):
                ui.console.print("[vmon.warn]vmond[/]: not running")
                return 1
            raise
        ui.console.print(
            f"[vmon.success]vmond[/]: running (pid {info.get('pid')})  "
            f"socket={info.get('socket')} version={info.get('version')}"
        )
        return 0
    if action == "stop":
        try:
            info = DaemonClient(autostart=False).stop_daemon()
        except DaemonError as e:
            if e.code in ("unreachable", "closed"):
                ui.console.print("[vmon.warn]vmond[/]: not running")
                return 0
            raise
        ui.success(f"vmond stopped (pid {info.get('pid')})")
        return 0
    info = DaemonClient().ensure_running()
    ui.success(f"vmond running (pid {info.get('pid')})  socket={info.get('socket')}")
    return 0


@cli.command(
    panel="Daemon",
    context_settings=_CTX,
    help="Serve the HTTP sandbox API (requires vmon[server]).",
)
@click.option("--host", default="127.0.0.1", show_default=True, help="bind host")
@click.option("--port", type=int, default=8000, show_default=True, help="bind port")
@click.option("--token", help="bearer token (or VMON_API_TOKEN)")
@click.option(
    "--idle-timeout",
    type=float,
    default=300.0,
    show_default=True,
    help="terminate idle sandboxes after this many seconds",
)
def serve(host, port, token, idle_timeout):
    try:
        from .server import serve as serve_http
    except ModuleNotFoundError as e:
        if e.name in {"fastapi", "uvicorn"}:
            ui.error("vmon serve requires the server extra: pip install 'vmon[server]'")
            return 1
        raise
    serve_http(host=host, port=port, token=token, idle_timeout=idle_timeout)
    return 0


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``vmon`` command and ``python -m vmon``.

    Runs the click CLI in non-standalone mode so command return values become
    the process exit code and daemon/usage errors render through
    :mod:`vmon.console` instead of click's defaults.
    """
    try:
        return cli.main(args=argv, prog_name="vmon", standalone_mode=False) or 0
    except SystemExit as e:  # --help / --version call ctx.exit()
        code = e.code
        return code if isinstance(code, int) else (0 if code is None else 1)
    except click.UsageError as e:
        # ``vmon`` with no command: print the help verbatim (it is the error's
        # message) instead of dressing it up with a ✗ and re-rendering it through
        # a Rich console, which mangles the box borders. Exit 0, matching click 8.1.
        if isinstance(e, _NO_ARGS_HELP):
            print(e.format_message())
            return 0
        ui.error(e.format_message())
        if e.ctx is not None:
            ui.hint(
                f"try 'vmon {e.ctx.command_path.removeprefix('vmon ')} --help'"
                if e.ctx.command_path != "vmon"
                else "try 'vmon --help'"
            )
        return e.exit_code
    except click.ClickException as e:
        ui.error(e.format_message())
        return e.exit_code
    except click.exceptions.Abort:
        ui.error("aborted")
        return 130
    except KeyboardInterrupt:
        return 130
    except BrokenPipeError:
        return 1
    except DaemonError as e:
        ui.error(str(e))
        return 1
    except (RuntimeError, FileNotFoundError, TimeoutError, ValueError) as e:
        ui.error(str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
