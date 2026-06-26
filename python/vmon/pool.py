"""Warm pool: pre-forked, agent-pinged microVM clones for instant claim."""

from __future__ import annotations

import os
import threading
from collections import deque

from .vmm import MicroVM


class WarmPool:
    """Keep ``size`` ready clones of a template warm for single-digit-ms claims.

    A background daemon thread forks (or restores) clones from ``template``,
    pings the guest agent so each clone is booted and parked, and tops the
    ready pool back up whenever :meth:`claim` drains it. ``claim`` is O(1) and
    never blocks on a spawn -- the refiller absorbs all the latency offline.
    """

    def __init__(self, template: str | os.PathLike[str], size: int, *,
                 fork: bool = True, agent: bool = True,
                 ping_timeout: float = 30.0):
        self.template = template
        self.size = size
        self.fork = fork
        self.agent = agent
        self.ping_timeout = ping_timeout
        self.hits = 0
        self.misses = 0
        self._ready: deque[MicroVM] = deque()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._refiller = threading.Thread(
            target=self._refill_loop, name="warmpool-refiller", daemon=True)
        self._refiller.start()

    # -- spawning -----------------------------------------------------------

    def _spawn_one(self) -> MicroVM | None:
        """Fork/restore one clone and ping its agent; tear down + ``None`` on error."""
        vm: MicroVM | None = None
        try:
            if self.fork:
                vm = MicroVM.fork_snapshot(self.template, agent=self.agent)
            else:
                vm = MicroVM.restore(self.template, agent=self.agent)
            if self.agent:
                # Drive the clone all the way to a live, parked guest agent so a
                # claimer inherits a warm VM rather than one still coming up.
                vm.agent(connect_timeout=self.ping_timeout).ping(timeout=self.ping_timeout)
            return vm
        except Exception:
            if vm is not None:
                self._teardown(vm)
            return None

    @staticmethod
    def _teardown(vm: MicroVM) -> None:
        """Best-effort stop + remove; never raises."""
        try:
            vm.stop()
        except Exception:
            pass
        try:
            vm.remove()
        except Exception:
            pass

    def _refill_loop(self) -> None:
        """Top ``_ready`` back to ``size`` until stopped; spawn outside the lock."""
        while not self._stop.is_set():
            try:
                with self._lock:
                    short = len(self._ready) < self.size
                if short:
                    vm = self._spawn_one()  # outside the lock: claimers never block
                    if vm is not None:
                        with self._lock:
                            stopping = self._stop.is_set()
                            if not stopping:
                                self._ready.append(vm)
                        if stopping:
                            self._teardown(vm)  # raced shutdown: don't leak the clone
            except Exception:
                pass  # a bad iteration must never kill the refiller
            self._stop.wait(0.1)  # interruptible: returns at once once stopped

    # -- claiming -----------------------------------------------------------

    def claim(self) -> MicroVM | None:
        """Pop a ready clone (O(1), non-blocking) or ``None`` on a cold miss.

        The refiller tops the pool back up afterwards; ``claim`` itself does no
        spawning and is effectively instant, so no latency is recorded here.
        """
        with self._lock:
            if self._ready:
                self.hits += 1
                return self._ready.popleft()
            self.misses += 1
            return None

    def stats(self) -> dict[str, int]:
        """Snapshot of hit/miss counters and the current ready depth."""
        with self._lock:
            return {"hits": self.hits, "misses": self.misses,
                    "ready_count": len(self._ready)}

    def shutdown(self) -> None:
        """Stop the refiller and tear down every parked clone (best-effort)."""
        self._stop.set()
        self._refiller.join(timeout=self.ping_timeout)
        with self._lock:
            parked = list(self._ready)
            self._ready.clear()
        for vm in parked:
            self._teardown(vm)
