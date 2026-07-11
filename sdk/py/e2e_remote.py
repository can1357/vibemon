"""Importable callables exercised by the real-VM durable-function smoke tests."""

from __future__ import annotations

import datetime
import time
from collections.abc import Iterator


def remote_double(value: int) -> int:
    print(f"doubling {value}")
    if value < 0:
        raise ValueError("value must be non-negative")
    return value * 2


def remote_shift(
    stamp: datetime.datetime,
    pair: tuple[int, int],
) -> tuple[datetime.datetime, tuple[int, int]]:
    return stamp + datetime.timedelta(days=1), (pair[1], pair[0])


def remote_ticks() -> Iterator[int]:
    for index in range(3):
        yield index
        time.sleep(0.2)


def remote_add(a: int, b: int) -> int:
    return a + b


class RemoteCounter:
    """Stateful actor used by the real-VM SDK smoke test."""

    def __init__(self, start: int = 0) -> None:
        self.value: int = start
        self.entered: bool = False

    def _mark(self) -> None:
        self.entered = True

    def bump(self) -> int:
        assert self.entered, "enter hook must run before methods"
        self.value += 1
        return self.value

    def _farewell(self) -> None:
        print("counter exiting")
