"""Focused subprocess smoke test for the protocol-v2 guest runner."""
import base64
import cbor2
import cloudpickle
import hashlib
import io
import json
import os
import selectors
import subprocess
import sys
import socket
import pickle
import tempfile
import textwrap
import time
import unittest
import zipfile
import zstandard
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
def raw_json_envelope(raw):
    if isinstance(raw, str):
        raw = raw.encode()
    return {
        "format": "json", "version": 1, "compression": "none",
        "uncompressed_size": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
        "inline_data": base64.b64encode(raw).decode(),
    }


def binary_envelope(data, value_format, **metadata):
    result = {
        "format": value_format, "version": 1, "compression": "none",
        "uncompressed_size": len(data), "sha256": hashlib.sha256(data).hexdigest(),
        "inline_data": base64.b64encode(data).decode(),
    }
    result.update(metadata)
    return result


class CloudValue:
    def __init__(self, value):
        self.value = value


class EvilReducer:
    marker = ""

    def __reduce__(self):
        return os.system, ("touch %s" % self.marker,)




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
            import os
            import time
            import vmon

            @vmon.function()
            def unary(value):
                assert vmon.is_remote()
                assert vmon.current_call().call_id == "call-unary"
                print("user-log")
                return value + 1

            def native_log():
                os.write(1, b"native-log\\n")
                time.sleep(0.02)
                return True

            async def async_call(value):
                await asyncio.sleep(0.01)
                return value * 2

            async def context_call(delay):
                await asyncio.sleep(delay)
                call = vmon.current_call()
                return {
                    "call_id": call.call_id, "function_id": call.function_id,
                    "input_id": call.input_id, "input_index": call.input_index,
                }

            def generate(value):
                for index in range(value):
                    yield index

            def big():
                return "x" * 600000

            def big_generate():
                yield "y" * 600000


            def batch_values(values):
                return [value * 2 for value in values]

            def bad_batch(values):
                return values[:-1]
            def fail(message):
                raise ValueError(message)


            async def suppress_cancel():
                try:
                    await asyncio.sleep(30)
                except asyncio.CancelledError:
                    return "ignored cancellation"

            def sync_wait():
                time.sleep(0.2)
                return "late success"
            def capture(*args, **kwargs):
                return {"args": list(args), "kwargs": kwargs}
            async def wait():
                await asyncio.sleep(30)

            @vmon.cls()
            class Counter:
                def __init__(self, value=0):
                    self.value = value
                    self.restores = 0
                    self.secret = None

                @vmon.enter
                def entered(self):
                    self.enters = getattr(self, "enters", 0) + 1

                @vmon.method
                def lifecycle_counts(self):
                    return [self.enters, self.restores]

                @vmon.method
                def add(self, amount=0, *, bonus=0):
                    self.value += amount + bonus
                    return self.value

                @vmon.method
                def get(self):
                    return self.value

                @vmon.method
                def fill(self):
                    self.blob = "z" * 600000
                    return len(self.blob)

                @vmon.method
                def set_secret(self):
                    self.secret = "actor-secret"
                    return True

                @vmon.method
                def clear_secret(self):
                    self.secret = None
                    return self.value

                @vmon.method
                def has_secret(self):
                    return self.secret == "actor-secret"

                @vmon.method
                def unavailable(self):
                    raise RuntimeError("database unavailable")

                @vmon.enter(snapshot=True)
                def prepare(self):
                    self.prepared = True

                @vmon.on_restore
                def restored(self):
                    self.restores += 1
                    self.secret = None

            @vmon.service()
            class StatefulService:
                def __init__(self, base=0, *, exit_path=""):
                    self.value = base
                    self.exit_path = exit_path

                @vmon.enter
                def start(self):
                    self.value += 1

                @vmon.method
                def add(self, amount=0, *, bonus=0):
                    self.value += amount + bonus
                    return self.value

                @vmon.method
                async def async_add(self, amount):
                    await asyncio.sleep(0)
                    self.value += amount
                    return self.value

                @vmon.method
                def fail(self):
                    raise RuntimeError("service failure")

                @vmon.exit
                def stop(self):
                    if self.exit_path:
                        with open(self.exit_path, "a", encoding="utf-8") as output:
                            output.write("exit\\n")
        """), encoding="utf-8")
        environment = os.environ.copy()
        environment["VMON_RUNNER_SPILL_ROOT"] = str(root / "spills")
        environment["VMON_RUNNER_MAX_VALUE_BYTES"] = str(2 * 1024 * 1024)
        environment["VMON_RUNNER_MAX_PENDING_TASKS"] = "9"
        self.function_root = root / "function-root"
        self.function_root.mkdir()
        self.function_root = self.function_root.resolve()
        environment["VMON_RUNNER_FUNCTION_ROOT"] = str(self.function_root)
        self.process = subprocess.Popen(
            [sys.executable, str(RUNNER), "--max-sync-threads", "2", "--max-async-tasks", "8"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1, env=environment)
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

    def define(self, name, target, secrets=None):
        request = "define-" + name
        self.client.send({"type": "define", "request_id": request, "definition_id": name,
                          "revision": "r1", "secrets": secrets or {},
                          "definition": {"mode": "package", "root": self.root,
                                         "target": "work:" + target}})
        frames = self.client.until(request, terminal=("status", "error"))
        self.assertIn(frames[-1]["status"], ("initialized", "already_initialized"))
        return frames[-1]
    def call(self, request, definition, args=None, **extra):
        frame = {"type": "call", "request_id": request, "call_id": "call-" + request,
                 "input_id": "input-" + request, "attempt": 2, "parent_call_id": "parent",
                 "definition_id": definition, "revision": "r1", "args": envelope(args or [])}
        frame.update(extra)
        self.client.send(frame)
        return self.client.until(request)

    def test_protocol_functions_and_durable_actors(self):
        expected_kinds = {
            "unary": "sync", "native": "sync", "async": "async", "context": "async",
            "capture": "sync", "batch": "sync", "bad-batch": "sync",
            "suppress": "async", "sync-wait": "sync", "gen": "generator",
            "big": "sync", "biggen": "generator", "wait": "async",
            "counter": "sync", "service": "sync",
        }
        for name, target in (("unary", "unary"), ("native", "native_log"),
                             ("async", "async_call"), ("context", "context_call"),
                             ("capture", "capture"), ("batch", "batch_values"),
                             ("bad-batch", "bad_batch"), ("suppress", "suppress_cancel"),
                             ("sync-wait", "sync_wait"), ("gen", "generate"),
                             ("big", "big"), ("biggen", "big_generate"),
                             ("wait", "wait"), ("counter", "Counter"),
                             ("service", "StatefulService")):
            status = self.define(name, target,
                                 {"ACTOR_SECRET": "actor-secret"} if name == "counter" else None)
            self.assertEqual(status["callable_kind"], expected_kinds[name])
        self.assertEqual(self.define("fail", "fail", {"TOKEN": "top-secret"})["callable_kind"],
                         "sync")

        duplicate = self.define("async", "async_call")
        self.assertEqual(duplicate["status"], "already_initialized")
        self.assertEqual(duplicate["callable_kind"], "async")
        archive_buffer = io.BytesIO()
        with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_STORED) as archive:
            archive.writestr("archived.py", "def call(value):\n    return value + 10\n")
            archive.writestr("padding.bin", os.urandom(1024 * 1024 + 1))
        archive_bytes = archive_buffer.getvalue()
        self.assertGreater(len(archive_bytes), 1024 * 1024)
        archive_path = self.function_root / "package.zip"
        archive_path.write_bytes(archive_bytes)
        self.client.send({
            "type": "define", "request_id": "define-archive", "definition_id": "archive",
            "revision": "r1", "definition": {"mode": "package",
            "archive_path": str(archive_path),
            "archive_sha256": hashlib.sha256(archive_bytes).hexdigest(),
            "target": "archived:call"}})
        archive_status = self.client.until("define-archive", terminal=("status", "error"))[-1]
        self.assertEqual(archive_status["callable_kind"], "sync")
        self.assertTrue(archive_path.exists())
        self.assertEqual(self.call("archive", "archive", [2])[-1]["type"], "result")

        outside = Path(self.root) / "outside.zip"
        outside.write_bytes(archive_bytes)
        self.client.send({
            "type": "define", "request_id": "define-traversal", "definition_id": "bad-path",
            "revision": "r1", "definition": {"mode": "package",
            "archive_path": str(self.function_root / ".." / "outside.zip"),
            "archive_sha256": hashlib.sha256(archive_bytes).hexdigest(),
            "target": "archived:call"}})
        traversal = self.client.until("define-traversal")[-1]
        self.assertEqual(traversal["type"], "error")
        self.assertTrue(outside.exists())

        symlink = self.function_root / "link.zip"
        symlink.symlink_to(outside)
        self.client.send({
            "type": "define", "request_id": "define-symlink", "definition_id": "bad-link",
            "revision": "r1", "definition": {"mode": "package",
            "archive_path": str(symlink),
            "archive_sha256": hashlib.sha256(archive_bytes).hexdigest(),
            "target": "archived:call"}})
        linked = self.client.until("define-symlink")[-1]
        self.assertEqual(linked["type"], "error")
        unary = self.call("unary", "unary", [4])
        self.assertTrue(any(item.get("type") == "log" and item.get("message") == "user-log"
                            for item in unary))
        self.assertEqual(unary[-1]["input_id"], "input-unary")
        self.assertEqual(unary[-1]["attempt"], 2)
        self.assertEqual(unary[-1]["parent_call_id"], "parent")
        self.assertEqual(unary[-1]["value"]["format"], "json")
        native = self.call("native", "native")
        native_log = next(item for item in native
                          if item.get("type") == "log" and item.get("message") == "native-log")
        self.assertEqual(native_log["request_id"], "native")
        self.assertEqual(native_log["call_id"], "call-native")

        def invoke_capture(request_id, **payload):
            frame = {"type": "call", "request_id": request_id,
                     "call_id": "call-" + request_id, "definition_id": "capture"}
            frame.update(payload)
            self.client.send(frame)
            response = self.client.until(request_id)[-1]
            self.assertEqual(response["type"], "result", response)
            return json.loads(base64.b64decode(response["value"]["inline_data"]))

        self.assertEqual(invoke_capture(
            "tag-zero", input=envelope(
                {"__vmon_python_call__": 1, "args": [], "kwargs": {}})),
            {"args": [], "kwargs": {}})
        self.assertEqual(invoke_capture(
            "tag-mixed", input=envelope(
                {"__vmon_python_call__": 1, "args": [1, 2], "kwargs": {"named": 3}})),
            {"args": [1, 2], "kwargs": {"named": 3}})
        collision = {"args": [1], "kwargs": {"x": 2}}
        self.assertEqual(invoke_capture("untagged-dict", input=envelope(collision)),
                         {"args": [collision], "kwargs": {}})
        self.assertEqual(invoke_capture(
            "native-arguments", positional=[envelope(4), envelope({"map": True})],
            named={"flag": envelope("yes")}),
            {"args": [4, {"map": True}], "kwargs": {"flag": "yes"}})

        self.assertEqual(self.call("async", "async", [3])[-1]["type"], "result")

        safe_integer = (1 << 53) - 1
        self.assertEqual(
            invoke_capture("ijson-safe", input=envelope(safe_integer))["args"],
            [safe_integer])
        self.client.send({"type": "call", "request_id": "ijson-unsafe",
                          "call_id": "ijson-unsafe", "definition_id": "capture",
                          "input": envelope(1 << 53)})
        unsafe_result = self.client.until("ijson-unsafe")[-1]
        self.assertEqual(unsafe_result["error"]["code"], "serialization_error")
        for request_id, raw in (
                ("ijson-duplicate", '{"x":1,"x":2}'),
                ("ijson-nonfinite", '{"x":NaN}')):
            self.client.send({"type": "call", "request_id": request_id,
                              "call_id": request_id, "definition_id": "capture",
                              "input": raw_json_envelope(raw)})
            invalid = self.client.until(request_id)[-1]
            self.assertEqual(invalid["error"]["code"], "serialization_error")
        self.client.send({"type": "call", "request_id": "ijson-output",
                          "call_id": "call-unary", "definition_id": "unary",
                          "args": envelope([safe_integer])})
        unsafe_output = self.client.until("ijson-output")[-1]
        self.assertEqual(unsafe_output["type"], "error", unsafe_output)
        self.assertEqual(unsafe_output["error"]["code"], "serialization_error")

        concurrent_frames = (
            {"type": "call", "request_id": "context-1", "call_id": "call-one",
             "function_id": "public-one", "definition_id": "context",
             "input_id": "stable-one", "input_index": 11, "attempt": 3,
             "parent_request_id": "request-parent-one", "parent_call_id": "parent-one",
             "args": envelope([0.02])},
            {"type": "call", "request_id": "context-2", "call_id": "call-two",
             "function_id": "public-two", "definition_id": "context",
             "input_id": "stable-two", "input_index": 12, "attempt": 4,
             "parent_request_id": "request-parent-two", "parent_call_id": "parent-two",
             "args": envelope([0.01])},
        )
        for frame in concurrent_frames:
            self.client.send(frame)
        context_results = {}
        while len(context_results) < 2:
            response = self.client.read()
            if response.get("type") == "result" and response.get("request_id", "").startswith("context-"):
                context_results[response["request_id"]] = response
        for index, request_id in enumerate(("context-1", "context-2"), start=1):
            response = context_results[request_id]
            context = json.loads(base64.b64decode(response["value"]["inline_data"]))
            self.assertEqual(context["call_id"], "call-%s" % ("one" if index == 1 else "two"))
            self.assertEqual(context["function_id"], "public-%s" %
                             ("one" if index == 1 else "two"))
            self.assertEqual(response["input_id"], "stable-%s" %
                             ("one" if index == 1 else "two"))
            self.assertEqual(response["input_index"], 10 + index)
            self.assertEqual(response["parent_request_id"], "request-parent-%s" %
                             ("one" if index == 1 else "two"))
        generated = self.call("gen", "gen", [3], execution_mode="generator")
        self.assertEqual([item["index"] for item in generated if item["type"] == "yield"], [0, 1, 2])

        batch_inputs = [
            {"request_id": "batch-item-%d" % index, "function_id": "batch",
             "call_id": "batch-call-%d" % index, "input_id": "batch-input-%d" % index,
             "input_index": index, "attempt": 1, "input": envelope(index)}
            for index in range(3)
        ]
        self.client.send({"type": "batch", "request_id": "batch-request",
                          "definition_id": "batch", "revision": "r1",
                          "inputs": batch_inputs})
        batch_result = self.client.until(
            "batch-request", terminal=("batch_result", "error"))[-1]
        self.assertEqual(batch_result["type"], "batch_result")
        self.assertEqual([item["input_index"] for item in batch_result["results"]], [0, 1, 2])
        self.assertEqual([
            json.loads(base64.b64decode(item["value"]["inline_data"]))
            for item in batch_result["results"]], [0, 2, 4])
        self.client.send({"type": "batch", "request_id": "bad-batch-request",
                          "definition_id": "bad-batch", "revision": "r1",
                          "inputs": batch_inputs})
        bad_batch = self.client.until("bad-batch-request")[-1]
        self.assertEqual(bad_batch["type"], "error")
        self.assertIn("cardinality", bad_batch["error"]["message"])

        def consume_spill(value, expected_character):
            self.assertNotIn("inline_data", value)
            self.assertTrue(value["remove_after_read"])
            self.assertTrue(os.path.isabs(value["path"]))
            self.assertEqual(os.stat(value["path"]).st_mode & 0o777, 0o600)
            with open(value["path"], "rb") as source:
                raw = source.read()
            self.assertEqual(hashlib.sha256(raw).hexdigest(), value["sha256"])
            self.assertEqual(json.loads(raw)[0], expected_character)
            os.remove(value["path"])

        big = self.call("big", "big")[-1]
        consume_spill(big["value"], "x")
        big_generated = self.call("biggen", "biggen", execution_mode="generator")
        big_yield = next(item for item in big_generated if item["type"] == "yield")
        consume_spill(big_yield["value"], "y")

        failed = self.call("fail", "fail", ["top-secret"])[-1]
        self.assertEqual(failed["error"]["type"], "ValueError")
        self.assertIn("work.py", failed["error"]["traceback"])
        self.assertNotIn("top-secret", json.dumps(failed))
        self.assertIn("[REDACTED]", failed["error"]["message"])
        rotated = self.define("fail", "fail", {"TOKEN": "rotated-secret"})
        self.assertEqual(rotated["status"], "already_initialized")
        rotated_failure = self.call("rotated-fail", "fail", ["rotated-secret"])[-1]
        self.assertNotIn("rotated-secret", json.dumps(rotated_failure))
        self.assertIn("[REDACTED]", rotated_failure["error"]["message"])

        deadline = self.call("deadline", "wait", deadline_unix_ms=int(time.time() * 1000) - 1)[-1]
        self.assertEqual(deadline["reason"], "deadline_exceeded")
        self.client.send({"type": "call", "request_id": "cancel", "call_id": "call-cancel",
                          "input_id": "input-cancel", "attempt": 1, "parent_call_id": None,
                          "definition_id": "wait", "args": envelope([])})
        self.client.send({"type": "cancel", "request_id": "cancel-command",
                          "target_request_id": "cancel"})
        self.assertEqual(self.client.until("cancel")[-1]["type"], "cancelled")
        self.client.send({"type": "call", "request_id": "suppress",
                          "call_id": "call-suppress", "definition_id": "suppress",
                          "args": envelope([])})
        self.client.send({"type": "cancel", "request_id": "cancel-suppress",
                          "target_request_id": "suppress"})
        suppressed = self.client.until("suppress")[-1]
        self.assertEqual(suppressed["type"], "cancelled")
        self.assertFalse(suppressed["worker_kill_required"])
        sync_deadline = self.call(
            "sync-deadline", "sync-wait",
            deadline_unix_ms=int(time.time() * 1000) + 20)[-1]
        self.assertEqual(sync_deadline["type"], "cancelled")
        self.assertTrue(sync_deadline["worker_kill_required"])
        queued_ids = ["queued-%d" % index for index in range(9)]
        for queued_id in queued_ids:
            self.client.send({"type": "call", "request_id": queued_id,
                              "call_id": "call-" + queued_id, "input_id": queued_id,
                              "definition_id": "wait", "args": envelope([])})
        self.client.send({"type": "call", "request_id": "overflow",
                          "call_id": "call-overflow", "input_id": "overflow",
                          "definition_id": "wait", "args": envelope([])})
        overflow = self.client.until("overflow")[-1]
        self.assertEqual(overflow["error"]["code"], "runner_overloaded")
        self.client.send({"type": "cancel", "request_id": "cancel-queued",
                          "target_request_id": queued_ids[-1]})
        queued_cancel = self.client.until(queued_ids[-1])[-1]
        self.assertEqual(queued_cancel["type"], "cancelled")
        for queued_id in queued_ids[:-1]:
            self.client.send({"type": "cancel", "request_id": "cancel-" + queued_id,
                              "target_request_id": queued_id})

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
        created_counts = actor("created-counts", "actor_call",
                               method="lifecycle_counts", args=envelope([]))
        self.assertIn("value", created_counts, created_counts)
        self.assertEqual(json.loads(base64.b64decode(
            created_counts["value"]["inline_data"])), [1, 0])

        actor("add5", "actor_call", method="add",
              positional=[envelope(2)], named={"bonus": envelope(3)})
        unavailable = actor("unavailable", "actor_call", method="unavailable",
                            positional=[], named={})
        self.assertEqual(unavailable["error"]["type"], "RuntimeError")
        self.assertNotEqual(unavailable["error"].get("code"), "actor_lost")
        still_alive = actor("still-alive", "actor_call", method="get",
                            positional=[], named={})
        self.assertEqual(json.loads(base64.b64decode(
            still_alive["value"]["inline_data"])), 6)
        checkpoint = actor("checkpoint", "actor_checkpoint", checkpoint_id="cp")
        self.assertEqual(checkpoint["value"]["format"], "cloudpickle")
        actor("fill", "actor_call", method="fill", args=envelope([]))
        big_checkpoint = actor("big-checkpoint", "actor_checkpoint", checkpoint_id="big-cp")
        self.assertNotIn("inline_data", big_checkpoint["value"])
        checkpoint_path = big_checkpoint["value"]["path"]
        with open(checkpoint_path, "rb") as source:
            checkpoint_bytes = source.read()
        self.assertEqual(hashlib.sha256(checkpoint_bytes).hexdigest(),
                         big_checkpoint["value"]["sha256"])
        self.assertNotIn(b"actor-secret", checkpoint_bytes)
        os.remove(checkpoint_path)
        actor("big-restore", "actor_restore", checkpoint_id="big-cp")
        actor("set-secret", "actor_call", method="set_secret", positional=[], named={})
        secret_checkpoint = actor(
            "secret-checkpoint", "actor_checkpoint", checkpoint_id="secret-cp")
        self.assertEqual(secret_checkpoint["error"]["code"], "checkpoint_contains_secret")
        self.assertNotIn("actor-secret", json.dumps(secret_checkpoint))
        has_secret = actor("has-secret", "actor_call", method="has_secret",
                           positional=[], named={})
        self.assertTrue(json.loads(base64.b64decode(
            has_secret["value"]["inline_data"])))
        actor("clear-secret", "actor_call", method="clear_secret", positional=[], named={})
        actor("add2", "actor_call", method="add", args=envelope([2]))
        actor("restore", "actor_restore", checkpoint_id="cp")
        actor("fork", "actor_fork", child_actor_id="b")
        actor("child-add", "actor_call", actor_id="b", method="add", args=envelope([1]))
        parent = actor("parent-get", "actor_call", method="get", args=envelope([]))
        self.assertEqual(json.loads(base64.b64decode(parent["value"]["inline_data"])), 6)

        parent_counts = actor("parent-counts", "actor_call",
                              method="lifecycle_counts", args=envelope([]))
        child_counts = actor("child-counts", "actor_call", actor_id="b",
                             method="lifecycle_counts", args=envelope([]))
        self.assertEqual(json.loads(base64.b64decode(
            parent_counts["value"]["inline_data"])), [1, 1])
        self.assertEqual(json.loads(base64.b64decode(
            child_counts["value"]["inline_data"])), [1, 2])

        exit_path = str(Path(self.root) / "service-exits.txt")

        def service(request, key, method, constructor=None, positional=None, named=None):
            frame = {
                "type": "service_call", "request_id": request,
                "call_id": "service-" + request, "definition_id": "service",
                "revision": "r1", "service_key": key, "method": method,
                "constructor": constructor or {}, "positional": positional or [],
                "named": named or {},
            }
            self.client.send(frame)
            return self.client.until(request)[-1]

        constructor = {
            "positional": [envelope(10)],
            "named": {"exit_path": envelope(exit_path)},
        }
        first_service = service(
            "service-first", "key-one", "add", constructor,
            [envelope(2)], {"bonus": envelope(3)})
        self.assertEqual(json.loads(base64.b64decode(
            first_service["value"]["inline_data"])), 16)
        second_service = service(
            "service-async", "key-one", "async_add", constructor,
            [envelope(4)], {})
        self.assertEqual(json.loads(base64.b64decode(
            second_service["value"]["inline_data"])), 20)
        independent = service(
            "service-independent", "key-two", "add",
            {"positional": [envelope(0)], "named": {"exit_path": envelope(exit_path)}},
            [envelope(1)], {})
        self.assertEqual(json.loads(base64.b64decode(
            independent["value"]["inline_data"])), 2)
        failed_service = service(
            "service-fail", "key-one", "fail", constructor, [], {})
        self.assertEqual(failed_service["error"]["type"], "RuntimeError")
        recreated = service(
            "service-recreated", "key-one", "add", constructor, [envelope(1)], {})
        self.assertEqual(json.loads(base64.b64decode(
            recreated["value"]["inline_data"])), 12)


        self.client.send({"type": "before_snapshot", "request_id": "secret-snapshot",
                          "call_id": "secret-snapshot", "input_id": "lifecycle",
                          "attempt": 1, "parent_call_id": None})
        secret_snapshot = self.client.until(
            "secret-snapshot", terminal=("status", "error"))[-1]
        self.assertEqual(secret_snapshot["error"]["code"],
                         "snapshot_with_secrets_unsupported")
        self.define("counter", "Counter", {})
        self.define("fail", "fail", {})

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
        exits = Path(exit_path).read_text(encoding="utf-8").splitlines()
        self.assertEqual(exits, ["exit", "exit", "exit"])


class SocketReconnectTest(unittest.TestCase):
    def test_actor_state_survives_transport_reconnect(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "actor_module.py").write_text(textwrap.dedent("""
                import vmon
                class Counter:
                    def __init__(self):
                        self.value = 0
                    @vmon.method
                    def add(self, amount):
                        self.value += amount
                        return self.value
                    @vmon.method
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
