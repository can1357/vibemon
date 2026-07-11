"""Object-oriented Python client for the Rust vmon API.

Example
-------
    import vmon

    with vmon.connect("vmon://node-a,node-b") as client:
        sandbox = client.sandboxes.create(image="alpine")
        result = sandbox.run("echo", "hi")
"""

__version__ = "0.2.0"

from ._endpoint import WebSocketConnection
from .client import Client, Pool, connect
from .cls import RemoteClass, RemoteObject, cls, enter, exit, method
from .context import Context, ContextStore
from .driver import Driver, MeshDriver, parse_dsn
from .errors import APIError, ProtocolError, RemoteFunctionError, TransportError
from .models import ExecExit, MeshNode, MeshStatus, SandboxInfo
from .process import ConsoleStream, EventStream, LogStream, Process
from .remote import FunctionCall, RemoteFunction, Retries, function, is_remote
from .sandbox import ExecResult, Files, Sandbox
from .secret import Secret
from .volume import Volume

__all__ = [
    "APIError",
    "Client",
    "ConsoleStream",
    "Context",
    "ContextStore",
    "Driver",
    "EventStream",
    "ExecExit",
    "ExecResult",
    "Files",
    "FunctionCall",
    "LogStream",
    "MeshDriver",
    "MeshNode",
    "MeshStatus",
    "Pool",
    "Process",
    "ProtocolError",
    "RemoteClass",
    "RemoteFunction",
    "RemoteFunctionError",
    "RemoteObject",
    "Retries",
    "Sandbox",
    "SandboxInfo",
    "Secret",
    "TransportError",
    "Volume",
    "WebSocketConnection",
    "cls",
    "connect",
    "enter",
    "exit",
    "function",
    "is_remote",
    "method",
    "parse_dsn",
]
