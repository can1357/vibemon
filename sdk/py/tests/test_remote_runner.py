"""Direct NDJSON-protocol tests for the guest session runner (no stub, no VM)."""

from __future__ import annotations

import base64
import hashlib
import json
import pickle
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from vmon._remote_runner import _SESSION_RUNNER

MODULE_SOURCE = """\
import datetime

from dataclasses import dataclass


@dataclass
class Point:
    x: int
    y: int


def double(value):
    print("doubling", value)
    if value < 0:
        raise ValueError("value must be non-negative")
    return value * 2


def make_point(x, y):
    return Point(x, y)


def shift_day(stamp):
    return stamp + datetime.timedelta(days=1)


def swap(pair):
    return (pair[1], pair[0])


def counter(n):
    for index in range(n):
        yield index * 10


def whine():
    import sys as _sys

    print("to stdout")
    print("to stderr", file=_sys.stderr)
    return "done"


def big(size):
    return "x" * size


class CustomBoom(Exception):
    def __init__(self, message, code):
        super().__init__(message)
        self.code = code


def boom_custom():
    raise CustomBoom("custom failure", 7)


class Unpicklable(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.socket = lambda: None


def boom_unpicklable():
    raise Unpicklable("cannot pickle me")
"""

MODULE_HASH = hashlib.sha256(MODULE_SOURCE.encode()).hexdigest()

CLASS_SOURCE = """\
class Greeter:
    def __init__(self, name):
        self.name = name
        self.entered = 0

    def warm(self):
        self.entered += 1

    def greet(self):
        return f"hi {self.name} ({self.entered})"

    def farewell(self):
        print("bye", self.name)
"""

CLASS_HASH = hashlib.sha256(CLASS_SOURCE.encode()).hexdigest()


class Runner:
    """One live runner subprocess speaking the NDJSON protocol."""

    def __init__(self, tmp_path: Path, env: dict[str, str] | None = None):
        script = tmp_path / "runner.py"
        script.write_text(_SESSION_RUNNER)
        merged = dict(**__import__("os").environ)
        merged.update(env or {})
        self.proc = subprocess.Popen(
            [sys.executable, "-u", str(script)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=merged,
        )
        self.ids = iter(range(1, 10_000))
        self.sent_hashes: set[str] = set()
        self.hello = self.recv()

    def send(self, op: dict[str, Any]) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(op) + "\n")
        self.proc.stdin.flush()

    def recv(self) -> dict[str, Any]:
        assert self.proc.stdout is not None
        line = self.proc.stdout.readline()
        assert line, f"runner died: {self.proc.stderr.read() if self.proc.stderr else ''}"
        return json.loads(line)

    def call(
        self,
        name: str,
        args: list[Any] | dict[str, Any],
        *,
        source: str = MODULE_SOURCE,
        source_hash: str = MODULE_HASH,
        module: str = "runnermod",
        mode: str = "value",
        self_call: bool = False,
        serializer: str = "auto",
    ) -> list[dict[str, Any]]:
        """Send one call op and collect events through result/error."""
        op: dict[str, Any] = {
            "op": "call",
            "id": next(self.ids),
            "hash": source_hash,
            "name": name,
            "args": args if isinstance(args, dict) else {"json": args},
            "kwargs": {"json": {}},
            "mode": mode,
            "self": self_call,
            "serializer": serializer,
        }
        if source_hash not in self.sent_hashes:
            op["source"] = source
            op["module"] = module
            self.sent_hashes.add(source_hash)
        self.send(op)
        events = []
        while True:
            event = self.recv()
            events.append(event)
            if event["event"] in {"result", "error"}:
                return events

    def close(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
        self.proc.wait(timeout=5)


@pytest.fixture
def runner(tmp_path):
    instance = Runner(tmp_path)
    yield instance
    instance.close()


def decode(frame: dict[str, Any]) -> Any:
    if "json" in frame:
        return frame["json"]
    if "pickle" in frame:
        return pickle.loads(base64.b64decode(frame["pickle"]))
    raise AssertionError(f"unexpected value frame {frame}")


def test_hello_reports_python_and_pickle(runner) -> None:
    assert runner.hello["event"] == "hello"
    assert runner.hello["py"][0] == 3
    assert runner.hello["pickle"] >= 4


def test_json_round_trip_and_stdout_ordering(runner) -> None:
    events = runner.call("double", [21])
    assert [event["event"] for event in events] == ["out", "result"]
    assert events[0]["stream"] == "stdout" and "doubling 21" in events[0]["data"]
    assert events[1]["json"] == 42


def test_source_hash_cached_second_call_omits_source(runner) -> None:
    runner.call("double", [1])
    events = runner.call("double", [2])  # helper omits source for known hash
    assert events[-1]["json"] == 4


def test_unknown_hash_without_source_errors(runner) -> None:
    runner.sent_hashes.add("deadbeef")
    events = runner.call("double", [1], source_hash="deadbeef")
    assert events[-1]["event"] == "error"
    assert "unknown source hash" in events[-1]["message"]


def test_tuple_and_datetime_fidelity_via_pickle(runner) -> None:
    stamp = datetime(2026, 7, 11, 12, 0, 0)
    args = {"pickle": base64.b64encode(pickle.dumps([stamp])).decode()}
    events = runner.call("shift_day", args)
    assert decode(events[-1]) == datetime(2026, 7, 12, 12, 0, 0)

    events = runner.call("swap", {"pickle": base64.b64encode(pickle.dumps([(1, 2)])).decode()})
    value = decode(events[-1])
    assert value == (2, 1) and isinstance(value, tuple)


def test_dataclass_instances_unpickle_via_module_registration() -> None:
    # The runner registers shipped source under its real module name, so
    # pickled instances of shipped classes resolve on both sides.
    import os
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        runner = Runner(Path(tmp))
        try:
            events = runner.call("make_point", [3, 4])
            frame = events[-1]
            assert "pickle" in frame, frame
            raw = base64.b64decode(frame["pickle"])
            assert b"runnermod" in raw, "instance must reference the registered module"
            os.environ  # keep flake quiet about unused import style
        finally:
            runner.close()


def test_generator_streams_in_order_then_null_result(runner) -> None:
    events = runner.call("counter", [3], mode="iter")
    yields = [event["json"] for event in events if event["event"] == "yield"]
    assert yields == [0, 10, 20]
    assert events[-1] == {"event": "result", "id": events[-1]["id"], "json": None}


def test_value_mode_on_generator_result_tells_user_to_stream(runner) -> None:
    events = runner.call("counter", [2], mode="value")
    assert events[-1]["event"] == "error"
    assert "remote_gen" in events[-1]["message"]


def test_iter_mode_on_plain_result_errors(runner) -> None:
    events = runner.call("double", [1], mode="iter")
    assert events[-1]["event"] == "error"
    assert "did not return a generator" in events[-1]["message"]


def test_builtin_error_event_carries_pickle_and_traceback(runner) -> None:
    events = runner.call("double", [-1])
    error = events[-1]
    assert error["event"] == "error"
    assert error["etype"] == "ValueError" and error["emod"] == "builtins"
    assert "double" in error["traceback"]
    restored = pickle.loads(base64.b64decode(error["pickle"]))
    assert isinstance(restored, ValueError)
    assert str(restored) == "value must be non-negative"


def test_custom_exception_pickles_with_attributes(runner) -> None:
    events = runner.call("boom_custom", [])
    error = events[-1]
    assert error["etype"] == "CustomBoom"
    restored = pickle.loads(base64.b64decode(error["pickle"]))
    assert restored.code == 7


def test_unpicklable_exception_falls_back_to_null_pickle(runner) -> None:
    events = runner.call("boom_unpicklable", [])
    error = events[-1]
    assert error["event"] == "error"
    assert error["etype"] == "Unpicklable"
    assert error["pickle"] is None
    assert "cannot pickle me" in error["message"]


def test_stdout_and_stderr_frames_precede_result(runner) -> None:
    events = runner.call("whine", [])
    kinds = [(event["event"], event.get("stream")) for event in events]
    assert kinds[-1] == ("result", None)
    assert ("out", "stdout") in kinds[:-1]
    assert ("out", "stderr") in kinds[:-1]
    assert events[-1]["json"] == "done"


def test_json_serializer_rejects_unserializable_result(runner) -> None:
    events = runner.call("make_point", [1, 2], serializer="json")
    error = events[-1]
    assert error["event"] == "error"
    assert error["message"] == "remote function result must be JSON-serializable"


def test_large_results_spill_to_files(tmp_path) -> None:
    runner = Runner(tmp_path, env={"VMON_FN_INLINE_LIMIT": "64"})
    try:
        events = runner.call("big", [4096])
        frame = events[-1]
        assert frame["event"] == "result"
        assert frame.get("format") == "json" and "file" in frame
        # Spill files live guest-side; here "guest" is the local machine.
        assert json.loads(Path(frame["file"]).read_bytes()) == "x" * 4096
    finally:
        runner.close()


def test_init_enter_call_shutdown_lifecycle(runner) -> None:
    init_id = next(runner.ids)
    runner.send(
        {
            "op": "init",
            "id": init_id,
            "hash": CLASS_HASH,
            "source": CLASS_SOURCE,
            "module": "greetermod",
            "name": "Greeter",
            "args": {"json": ["ada"]},
            "kwargs": {"json": {}},
            "enter": ["warm"],
            "exit": ["farewell"],
            "serializer": "auto",
        }
    )
    assert runner.recv() == {"event": "result", "id": init_id, "json": None}
    runner.sent_hashes.add(CLASS_HASH)

    events = runner.call("greet", [], source_hash=CLASS_HASH, self_call=True)
    assert events[-1]["json"] == "hi ada (1)", "enter hook must have run exactly once"

    shutdown_id = next(runner.ids)
    runner.send({"op": "shutdown", "id": shutdown_id})
    events = []
    while True:
        event = runner.recv()
        events.append(event)
        if event["event"] == "result":
            break
    assert any(
        event["event"] == "out" and "bye ada" in event["data"] for event in events
    ), "exit hook output must stream before the shutdown ack"
    assert runner.proc.wait(timeout=5) == 0


def test_self_call_without_init_errors(runner) -> None:
    events = runner.call("greet", [], source=CLASS_SOURCE, source_hash=CLASS_HASH, self_call=True)
    assert events[-1]["event"] == "error"
    assert "no initialized instance" in events[-1]["message"]
