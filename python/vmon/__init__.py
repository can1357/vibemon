"""vmon — a friendly Python SDK + CLI for vmon microVMs.

Example
-------
    from vmon import MicroVM

    vm = MicroVM.run(image="alpine", cmd=["sh", "-c", "echo hi"])
    vm.snapshot("template", keep_running=False)     # pause + snapshot
    fast = MicroVM.restore("template")              # warm-boot in <200ms
    clones = fast.fork("template", count=3)         # CoW clones
"""

__version__ = "0.2.0"

from .control import Control
from .image import ImageSpec, build_rootfs
from .vmm import MicroVM, default_kernel, find_binary

__all__ = [
    "MicroVM",
    "Control",
    "ImageSpec",
    "build_rootfs",
    "find_binary",
    "default_kernel",
]
