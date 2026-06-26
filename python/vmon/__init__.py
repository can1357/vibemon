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
from .sandbox import Sandbox
from .vmm import MicroVM, default_kernel, find_binary

__all__ = [
    "Sandbox",
    "MicroVM",
    "Control",
    "ImageSpec",
    "find_binary",
    "default_kernel",
]
