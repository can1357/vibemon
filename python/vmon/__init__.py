"""vmon — a friendly Python SDK + CLI for vmon microVMs.

Example
-------
    from vmon import Sandbox

    sb = Sandbox.create(image="alpine")
    proc = sb.exec("sh", "-c", "echo hi")
    proc.wait()
    snap = sb.snapshot_filesystem("template")
"""

__version__ = "0.2.0"

from .control import Control
from .image import ImageSpec
from .sandbox import RemoteFunction, RemoteFunctionError, Sandbox, function
from .vmm import MicroVM, default_kernel, find_binary

__all__ = [
    "Sandbox",
    "MicroVM",
    "Control",
    "ImageSpec",
    "RemoteFunction",
    "RemoteFunctionError",
    "function",
    "connect",
    "find_binary",
    "default_kernel",
]


def __getattr__(name: str):
    if name == "connect":
        from .remote import connect

        return connect
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
