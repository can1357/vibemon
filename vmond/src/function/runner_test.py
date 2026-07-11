"""Focused subprocess smoke test for the protocol-v2 guest runner."""
import base64
import hashlib
import json
import os
import selectors
import subprocess
import sys
import socket
import tempfile
import textwrap
import time
import unittest
from pathlib import Path

RUNNER = Path(__file__).with_name("runner.py")


def envelope(value):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return {
        "format": "json",
        "version": 1,
        "compression": "none",
        "uncompressed_size": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "inline_data": base64.b64encode(raw).decode(),
    }


class Client:
    def __init__(self, process):
        self.process = process
        self.selector = selectors.DefaultSelector()
        self.selector.register(process.stdout, selectors.EVENT_READ)

    def send(self, frame):
        frame.setdefault("protocol", 2)
        self.process.stdin.write(json.dumps(frame, separators=(",", ":")) + "\n")
        self.process.stdin.flush()

    def read(self, timeout=5):
        ready = self.selector.select(timeout)
        if not ready:
            self.fail("runner response timed out")
        line = self.process.stdout.readline()
        if not line:
            stderr = self.process.stderr.read()
            self.fail("runner exited early: %s" % stderr)
        return json.loads(line)

    def until(self, request_id, terminal=("result", "error", "cancelled"), timeout=5):
        frames = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            frame = self.read(deadline - time.monotonic())
            frames.append(frame)
            if frame.get("request_id") == request_id and frame.get("type") in terminal:
                return frames
        self.fail("no terminal response for %s: %r" % (request_id, frames))

    def fail(self, message):
        raise AssertionError(message)


class RunnerSmokeTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        (root / "work.py").write_text(textwrap.dedent("""
            import asyncio
            import vmon

            @vmon.function()
            def unary(value):
                assert vmon.is_remote()
                assert vmon.current_call()["call_id"] == "call-unary"
                print("user-log")
                return value + 1

            async def async_call(value):
                await asyncio.sleep(0.01)
                return value * 2

            def generate(value):
                for index in range(value):
                    yield index

            def fail():
                raise ValueError("boom")

            async def wait():
                await asyncio.sleep(30)

            @vmon.cls()
            class Counter:
                def __init__(self, value=0):
                    self.value = value
                    self.restores = 0

                def add(self, amount):
                    self.value += amount
                    return self.value

                def get(self):
                    return self.value

                @vmon.before_snapshot()
                def prepare(self):
                    self.prepared = True

                @vmon.after_restore()
                def restored(self):
                    self.restores += 1
        """), encoding="utf-8")
        self.process = subprocess.Popen(
            [sys.executable, str(RUNNER), "--max-sync-threads", "2", "--max-async-tasks", "8"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1)
        self.client = Client(self.process)
        hello = self.client.read()
        self.assertEqual(hello["type"], "hello")
        self.assertEqual(hello["protocol"], 2)
        self.root = str(root)

    def tearDown(self):
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=2)
        for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
            if stream and not stream.closed:
                stream.close()
        self.temporary.cleanup()

    def define(self, name, target):
        request = "define-" + name
        self.client.send({"type": "define", "request_id": request, "definition_id": name,
                          "revision": "r1", "definition": {"mode": "package",
                          "root": self.root, "target": "work:" + target}})
        frames = self.client.until(request, terminal=("status", "error"))
        self.assertEqual(frames[-1]["status"], "initialized")

    def call(self, request, definition, args=None, **extra):
        frame = {"type": "call", "request_id": request, "call_id": "call-" + request,
                 "input_id": "input-" + request, "attempt": 2, "parent_call_id": "parent",
                 "definition_id": definition, "revision": "r1", "args": envelope(args or [])}
        frame.update(extra)
        self.client.send(frame)
        return self.client.until(request)

    def test_protocol_functions_and_durable_actors(self):
        for name, target in (("unary", "unary"), ("async", "async_call"),
                             ("gen", "generate"), ("fail", "fail"), ("wait", "wait"),
                             ("counter", "Counter")):
            self.define(name, target)

        unary = self.call("unary", "unary", [4])
        self.assertTrue(any(item.get("type") == "log" and item.get("message") == "user-log"
                            for item in unary))
        self.assertEqual(unary[-1]["input_id"], "input-unary")
        self.assertEqual(unary[-1]["attempt"], 2)
        self.assertEqual(unary[-1]["parent_call_id"], "parent")
        self.assertEqual(unary[-1]["value"]["format"], "json")

        self.assertEqual(self.call("async", "async", [3])[-1]["type"], "result")
        generated = self.call("gen", "gen", [3], execution_mode="generator")
        self.assertEqual([item["index"] for item in generated if item["type"] == "yield"], [0, 1, 2])

        failed = self.call("fail", "fail")[-1]
        self.assertEqual(failed["error"]["type"], "ValueError")
        self.assertIn("work.py", failed["error"]["traceback"])

        deadline = self.call("deadline", "wait", deadline_unix_ms=int(time.time() * 1000) - 1)[-1]
        self.assertEqual(deadline["type"], "cancelled")
        self.client.send({"type": "call", "request_id": "cancel", "call_id": "call-cancel",
                          "input_id": "input-cancel", "attempt": 1, "parent_call_id": None,
                          "definition_id": "wait", "args": envelope([])})
        self.client.send({"type": "cancel", "request_id": "cancel-command",
                          "target_request_id": "cancel"})
        self.assertEqual(self.client.until("cancel")[-1]["type"], "cancelled")

        self.client.send({"type": "actor_create", "request_id": "create", "actor_id": "a",
                          "definition_id": "counter", "args": envelope([1])})
        self.assertEqual(self.client.until("create")[-1]["type"], "result")

        def actor(request, operation, actor_id="a", **fields):
            frame = {"type": operation, "request_id": request, "call_id": "c-" + request,
                     "input_id": "i-" + request, "attempt": 1, "parent_call_id": None,
                     "actor_id": actor_id}
            frame.update(fields)
            self.client.send(frame)
            return self.client.until(request)[-1]

        actor("add5", "actor_call", method="add", args=envelope([5]))
        checkpoint = actor("checkpoint", "actor_checkpoint", checkpoint_id="cp")
        self.assertEqual(checkpoint["value"]["format"], "cloudpickle")
        actor("add2", "actor_call", method="add", args=envelope([2]))
        actor("restore", "actor_restore", checkpoint_id="cp")
        actor("fork", "actor_fork", child_actor_id="b")
        actor("child-add", "actor_call", actor_id="b", method="add", args=envelope([1]))
        parent = actor("parent-get", "actor_call", method="get", args=envelope([]))
        self.assertEqual(json.loads(base64.b64decode(parent["value"]["inline_data"])), 6)

        self.client.send({"type": "before_snapshot", "request_id": "before",
                          "call_id": "lifecycle-before", "input_id": "lifecycle",
                          "attempt": 1, "parent_call_id": None})
        self.assertEqual(self.client.until("before", terminal=("status", "error"))[-1]["status"],
                         "before_snapshot_complete")
        self.client.send({"type": "after_restore", "request_id": "after",
                          "call_id": "lifecycle-after", "input_id": "lifecycle",
                          "attempt": 1, "parent_call_id": None})
        self.assertEqual(self.client.until("after", terminal=("status", "error"))[-1]["status"],
                         "after_restore_complete")

        self.client.send({"type": "shutdown", "request_id": "stop"})
        self.process.stdin.close()
        self.process.wait(timeout=5)
        self.assertEqual(self.process.returncode, 0, self.process.stderr.read())


class SocketReconnectTest(unittest.TestCase):
    def test_actor_state_survives_transport_reconnect(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "actor_module.py").write_text(textwrap.dedent("""
                class Counter:
                    def __init__(self):
                        self.value = 0
                    def add(self, amount):
                        self.value += amount
                        return self.value
                    def get(self):
                        return self.value
            """), encoding="utf-8")
            socket_path = str(root / "runner.sock")
            process = subprocess.Popen(
                [sys.executable, str(RUNNER), "--socket", socket_path],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                deadline = time.monotonic() + 5
                while not os.path.exists(socket_path):
                    if process.poll() is not None:
                        self.fail("socket runner exited: %s" % process.stderr.read())
                    if time.monotonic() >= deadline:
                        self.fail("socket runner did not create its socket")
                    time.sleep(0.01)

                def connect():
                    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    connection.settimeout(5)
                    connection.connect(socket_path)
                    stream = connection.makefile("rwb", buffering=0)
                    hello = json.loads(stream.readline())
                    self.assertEqual(hello["type"], "hello")
                    return connection, stream

                def transact(stream, frame, terminal=("result", "error", "status")):
                    frame.setdefault("protocol", 2)
                    stream.write((json.dumps(frame, separators=(",", ":")) + "\n").encode())
                    while True:
                        response = json.loads(stream.readline())
                        if (response.get("request_id") == frame.get("request_id") and
                                response.get("type") in terminal):
                            return response

                connection, stream = connect()
                response = transact(stream, {
                    "type": "define", "request_id": "define", "definition_id": "counter",
                    "revision": "r1", "definition": {"mode": "package", "root": str(root),
                    "target": "actor_module:Counter"}})
                self.assertEqual(response["status"], "initialized")
                transact(stream, {"type": "actor_create", "request_id": "create",
                                   "actor_id": "durable", "definition_id": "counter",
                                   "args": envelope([])})
                transact(stream, {"type": "actor_call", "request_id": "add",
                                   "actor_id": "durable", "method": "add",
                                   "args": envelope([9])})
                stream.close()
                connection.close()

                connection, stream = connect()
                response = transact(stream, {"type": "actor_call", "request_id": "get",
                                              "actor_id": "durable", "method": "get",
                                              "args": envelope([])})
                value = json.loads(base64.b64decode(response["value"]["inline_data"]))
                self.assertEqual(value, 9)
                transact(stream, {"type": "shutdown", "request_id": "stop"},
                         terminal=("status",))
                stream.close()
                connection.close()
                process.wait(timeout=5)
                self.assertEqual(process.returncode, 0, process.stderr.read())
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=2)
                process.stdout.close()
                process.stderr.close()


if __name__ == "__main__":
    unittest.main()
