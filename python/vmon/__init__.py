"""vmon — thin Python SDK for the Rust vmon API.

Example
-------
    from vmon import Sandbox

    sb = Sandbox.create(image="alpine")
    proc = sb.exec("sh", "-c", "echo hi")
    proc.wait()
    snap = sb.snapshot_filesystem("template")
"""

__version__ = "0.2.0"

from ._transport import DaemonError, WebSocketConnection
from .context import (
    LOCAL,
    Context,
    ContextStore,
    context_token_path,
    contexts_path,
    roster_from_status,
)
from .sandbox import (
    ConsoleStream,
    EventStream,
    ExecResult,
    LogStream,
    Process,
    RemoteFunction,
    RemoteFunctionError,
    Sandbox,
    WarmPoolHandle,
    daemon_metrics,
    events,
    function,
    health,
    list_snapshots,
    openapi_schema,
    pool_inventory,
    prewarm,
    server_info,
    shell,
    shutdown_all_pools,
    shutdown_prewarms,
)
from .secret import Secret
from .volume import Volume

__all__ = [
    "ConsoleStream",
    "Context",
    "ContextStore",
    "DaemonError",
    "EventStream",
    "ExecResult",
    "LOCAL",
    "LogStream",
    "Process",
    "RemoteFunction",
    "RemoteFunctionError",
    "Sandbox",
    "Secret",
    "Volume",
    "WarmPoolHandle",
    "WebSocketConnection",
    "context_token_path",
    "contexts_path",
    "daemon_metrics",
    "events",
    "function",
    "health",
    "list_snapshots",
    "openapi_schema",
    "pool_inventory",
    "prewarm",
    "roster_from_status",
    "server_info",
    "shell",
    "shutdown_all_pools",
    "shutdown_prewarms",
]
