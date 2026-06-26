"""Rich-rendered, Modal-style help and colored output for the ``vmon`` CLI.

This is the CLI's *only* presentation layer. The SDK and engine
(:mod:`vmon.core`, :mod:`vmon.vmm`, :mod:`vmon.client`, :mod:`vmon.daemon`) never
import it, so ``import vmon`` and the daemon stay dependency-light — only the
``vmon`` command pulls in :mod:`click`/:mod:`rich`.

It provides two things:

* colored status output (:func:`error`, :func:`success`, :func:`info`,
  :func:`hint`, :func:`vm_table`), and
* a Modal-style help renderer (:class:`RichGroup`, :class:`RichCommand`) that
  draws grouped command panels and per-command argument/option panels in
  rounded boxes with a green accent.

Color and box-drawing are emitted only when the stream is a real terminal; piped
output (tests, ``vmon ps | grep``) stays plain text.
"""

from __future__ import annotations

import inspect
import io
import os
import shutil
import sys
from collections.abc import Callable, Sequence
from typing import Any

import click
from rich.box import ROUNDED
from rich.console import Console, Group as Renderables, RenderableType
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.theme import Theme

# Modal's signature lime-green accent.
ACCENT = "#7fee64"

#: Cap help/usage width so panels stay readable and never sprawl (or wrap and
#: shred their box borders) on very wide terminals. Below this, help tracks the
#: real terminal width.
MAX_WIDTH = 100

_THEME = Theme(
    {
        "vmon.accent": ACCENT,
        "vmon.command": f"bold {ACCENT}",
        "vmon.option": "bold cyan",
        "vmon.metavar": "dim cyan",
        "vmon.title": "bold default",
        "vmon.muted": "dim",
        "vmon.error": "bold red",
        "vmon.success": f"bold {ACCENT}",
        "vmon.warn": "bold yellow",
    }
)

#: Help/output console (stdout); errors go to :data:`err_console` (stderr).
console = Console(theme=_THEME, highlight=False)
err_console = Console(theme=_THEME, highlight=False, stderr=True)


# -- status output ---------------------------------------------------------


def error(message: str) -> None:
    """Print a red ``✗`` error line to stderr."""
    err_console.print(f"[vmon.error]✗[/] {message}")


def success(message: str) -> None:
    """Print a green ``✔`` success line to stdout."""
    console.print(f"[vmon.success]✔[/] {message}")


def info(message: str) -> None:
    """Print a green ``▸`` status line to stdout."""
    console.print(f"[vmon.accent]▸[/] {message}")


def hint(message: str) -> None:
    """Print a muted follow-up hint (e.g. the next command to run)."""
    console.print(f"  [vmon.muted]{message}[/]")


def vm_table(rows: list[dict]) -> None:
    """Render ``vmon ps`` as a borderless, pipe-parseable table.

    ``box=None`` keeps each row's first whitespace-delimited token equal to the
    VM name so scripts (and the e2e driver) can ``line.split()`` it; color is
    dropped automatically when stdout is not a terminal.
    """
    table = Table(box=None, pad_edge=False, padding=(0, 2, 0, 0), header_style="vmon.title")
    table.add_column("NAME", style="vmon.command", no_wrap=True)
    table.add_column("STATUS", no_wrap=True)
    table.add_column("PID", no_wrap=True)
    table.add_column("IMAGE/SOURCE", overflow="fold")
    for vm in rows:
        pid = vm.get("pid")
        table.add_row(
            vm["name"],
            _status_text(str(vm.get("status") or "-")),
            str(pid if pid is not None else "-"),
            str(vm.get("source") or "-"),
        )
    console.print(table)


def _status_text(status: str) -> Text:
    palette = {
        "running": "vmon.success",
        "paused": "vmon.warn",
        "stopped": "vmon.muted",
        "exited": "vmon.muted",
    }
    return Text(status, style=palette.get(status, "default"))


# -- Modal-style help ------------------------------------------------------


def _stdout_isatty() -> bool:
    """Whether the *current* stdout is a terminal (help is echoed to stdout)."""
    try:
        return bool(sys.stdout.isatty())
    except (AttributeError, ValueError):
        return False


def _term_size() -> tuple[int, int]:
    """The live terminal size, measured from the current stdout.

    Uses an ``ioctl`` on the actual stdout fd (not a cached file or a possibly
    stale ``COLUMNS``) so the render matches the stream the help is echoed to;
    otherwise an over-wide render wraps and shreds the box borders. Falls back to
    ``COLUMNS``/80×24 when stdout is not a terminal (piped/redirected).
    """
    if _stdout_isatty():
        try:
            size = os.get_terminal_size(sys.stdout.fileno())
            return size.columns, size.lines
        except (OSError, ValueError, AttributeError):
            pass
    fallback = shutil.get_terminal_size((80, 24))
    return fallback.columns, fallback.lines


def _capture(*renderables: RenderableType) -> str:
    """Render *renderables* to a string sized to the real terminal.

    Rich only honors an explicit width when height is *also* pinned, and a plain
    ``capture()`` drops ANSI unless color was active at the global console's
    construction. So we render through a throwaway recording console with both
    dimensions fixed and export with styles iff the *current* stdout is a TTY —
    colored, terminal-width boxes interactively; plain text when piped.
    """
    width, height = _term_size()
    width = min(width - 4, MAX_WIDTH)
    is_tty = _stdout_isatty()
    recorder = Console(
        theme=_THEME,
        highlight=False,
        width=width,
        height=height,
        record=True,
        file=io.StringIO(),
        force_terminal=is_tty or None,
        color_system="auto" if is_tty else None,
    )
    for item in renderables:
        recorder.print(item)
    return recorder.export_text(styles=is_tty).rstrip("\n")


def _usage_text(ctx: click.Context, command: click.Command) -> Text:
    pieces = command.collect_usage_pieces(ctx)
    text = Text()
    text.append("Usage: ", style="vmon.title")
    text.append(ctx.command_path, style="vmon.command")
    for piece in pieces:
        text.append(" ")
        text.append(piece, style="vmon.metavar")
    return text


def _option_names(param: click.Option, ctx: click.Context) -> Text:
    """``-f, --flag`` (+ ``/--no-flag`` secondary opts) styled like Modal."""
    text = Text()
    for i, opt in enumerate(param.opts):
        if i:
            text.append(", ")
        text.append(opt, style="vmon.option")
    for opt in param.secondary_opts:
        text.append(" / ")
        text.append(opt, style="vmon.option")
    metavar = _metavar(param, ctx)
    if metavar and not param.is_flag:
        text.append(" ")
        text.append(metavar, style="vmon.metavar")
    return text


def _argument_label(param: click.Argument, ctx: click.Context) -> Text:
    return Text(_metavar(param, ctx), style="vmon.option")


def _metavar(param: click.Parameter, ctx: click.Context) -> str:
    make_metavar: Callable[..., str] = param.make_metavar
    if inspect.signature(make_metavar).parameters:
        return make_metavar(ctx)  # click >= 8.2
    return make_metavar()  # click 8.1


def _help_text(param: click.Option, ctx: click.Context) -> Text:
    """An option's help, with click's trailing ``[default: …]``/``[required]`` dimmed."""
    record = param.get_help_record(ctx)
    help_str = record[1] if record else ""
    head, sep, tail = help_str.partition("  [")
    text = Text(head, style="default")
    if sep:
        text.append("  [" + tail, style="vmon.muted")
    return text


def _param_table(params: Sequence[click.Parameter], ctx: click.Context) -> Table:
    table = Table(box=None, pad_edge=False, padding=(0, 2, 0, 0), show_header=False)
    table.add_column(no_wrap=True)
    table.add_column(overflow="fold")
    for param in params:
        if isinstance(param, click.Argument):
            table.add_row(_argument_label(param, ctx), Text(""))
        elif isinstance(param, click.Option):
            table.add_row(_option_names(param, ctx), _help_text(param, ctx))
    return table


def _panel(body: RenderableType, title: str) -> Panel:
    return Panel(
        body,
        title=f"[vmon.title]{title}[/]",
        title_align="left",
        box=ROUNDED,
        border_style="vmon.muted",
        padding=(0, 1),
    )


class RichCommand(click.Command):
    """A click command whose ``--help`` renders Modal-style rich panels.

    ``panel`` names the group panel this command appears under in its parent's
    help (see :class:`RichGroup`); it has no effect on the command's own help.
    """

    def __init__(self, *args: Any, panel: str = "Commands", **kwargs: Any) -> None:
        self.panel = panel
        super().__init__(*args, **kwargs)

    def get_help(self, ctx: click.Context) -> str:
        parts: list[RenderableType] = [_usage_text(ctx, self), ""]
        if self.help:
            parts.append(Text(self.help.strip(), style="default"))
            parts.append("")
        params = self.get_params(ctx)
        args = [p for p in params if isinstance(p, click.Argument)]
        opts = [p for p in params if isinstance(p, click.Option) and not p.hidden]
        if args:
            parts.append(_panel(_param_table(args, ctx), "Arguments"))
        if opts:
            parts.append(_panel(_param_table(opts, ctx), "Options"))
        return _capture(Renderables(*parts))


class RichGroup(click.Group):
    """A click group with Modal-style grouped command panels in ``--help``.

    Subcommands default to :class:`RichCommand`; each command's ``panel`` puts it
    in a titled panel, rendered in first-seen panel order.
    """

    command_class = RichCommand

    def list_commands(self, ctx: click.Context) -> list[str]:
        # Registration order, not click's default alphabetical sort, so panels
        # and the commands within them read top-down as declared.
        return list(self.commands)

    def get_help(self, ctx: click.Context) -> str:
        parts: list[RenderableType] = [_usage_text(ctx, self), ""]
        if self.help:
            parts.append(Text(self.help.strip(), style="default"))
            parts.append("")
        opts = [p for p in self.get_params(ctx) if isinstance(p, click.Option) and not p.hidden]
        if opts:
            parts.append(_panel(_param_table(opts, ctx), "Options"))
        for title, commands in self._panels(ctx).items():
            table = Table(box=None, pad_edge=False, padding=(0, 2, 0, 0), show_header=False)
            table.add_column(style="vmon.command", no_wrap=True)
            table.add_column(overflow="fold")
            for name, cmd in commands:
                table.add_row(name, Text(cmd.get_short_help_str(), style="default"))
            parts.append(_panel(table, title))
        return _capture(Renderables(*parts))

    def _panels(self, ctx: click.Context) -> dict[str, list[tuple[str, click.Command]]]:
        panels: dict[str, list[tuple[str, click.Command]]] = {}
        for name in self.list_commands(ctx):
            cmd = self.get_command(ctx, name)
            if cmd is None or cmd.hidden:
                continue
            title = getattr(cmd, "panel", "Commands")
            panels.setdefault(title, []).append((name, cmd))
        return panels
