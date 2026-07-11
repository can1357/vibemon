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

from ._transport import DaemonError
from .context import (
    LOCAL,
    Context,
    ContextStore,
    context_token_path,
    contexts_path,
    roster_from_status,
)
from .sandbox import RemoteFunction, RemoteFunctionError, Sandbox, function
from .secret import Secret
from .volume import Volume

__all__ = [
    "Sandbox",
    "Secret",
    "Volume",
    "Context",
    "ContextStore",
    "LOCAL",
    "contexts_path",
    "context_token_path",
    "roster_from_status",
    "DaemonError",
    "RemoteFunction",
    "RemoteFunctionError",
    "function",
]
