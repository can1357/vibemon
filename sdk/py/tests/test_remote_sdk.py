from __future__ import annotations

import asyncio
import time
import uuid

import pytest
from v1_stub import V1StubServer, configure_context

from vmon import APIError, RemoteFunctionError, Retries, cls, connect, enter, exit, function, method


def test_bound_sandbox_round_trip_over_v1_stub(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        with connect() as client:
            sandbox = client.sandboxes.create(
                image="python:3.14-slim",
                context=".",
                name="sdk-1",
                cpus=2,
                memory=1024,
                env={"HELLO": "world"},
                tags={"suite": "remote"},
            )

            create_payload = server.last("POST", "/v1/sandboxes").json
            assert create_payload["cpus"] == 2
            assert create_payload["memory"] == 1024
            assert create_payload["env"] == {"HELLO": "world"}
            assert sandbox.id == "sdk-1"

            sandbox.files.write_text("/tmp/hello.txt", "hello")
            assert sandbox.files.read_text("/tmp/hello.txt") == "hello"
            assert sandbox.files.read_bytes("/tmp/hello.txt") == b"hello"
            assert sandbox.files.stat("/tmp/hello.txt").size == 5
            assert [item.size for item in sandbox.files.list("/tmp")] == [5]

            process = sandbox.exec("cat", "/tmp/hello.txt")
            assert process.wait(timeout=5).code == 0
            assert process.stdout.read() == b"hello"
            assert sandbox.metrics() == {"vcpu_exits": 7}
            assert sandbox.extend(30).raw["deadline_unix"] == 1_800_000_000
            assert sandbox.snapshot("snap-1") == "snap-1"
            assert sandbox.snapshot_filesystem("fs-1") == "fs-1"
            assert sandbox.logs() == "created\n"

            sandbox.remove()
            assert "sdk-1" not in server.sandboxes


def test_api_error_preserves_server_code_and_status(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        with connect() as client:
            with pytest.raises(APIError) as exc_info:
                client.sandboxes.get("missing")

        assert exc_info.value.code == "not_found"
        assert exc_info.value.status == 404
        assert "unknown sandbox missing" in str(exc_info.value)


def test_function_decorator_reuses_one_warm_session(monkeypatch, mvm_home, capsys) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        @function(image="python:3.14-slim")
        def add(left: int, right: int) -> dict[str, int]:
            print("remote ran")
            return {"sum": left + right}

        assert add.remote(2, 5) == {"sum": 7}
        assert add.remote(3, 4) == {"sum": 7}
        add.terminate()

        assert capsys.readouterr().out == "remote ran\nremote ran\n"

        create_request = server.last("POST", "/v1/sandboxes")
        assert create_request.json["image"] == "python:3.14-slim"
        runner_writes = [
            request
            for request in server.recorded("PUT", "/v1/sandboxes/sb-1/files")
            if request.query["path"] == ["/tmp/vmon-fn-runner.py"]
        ]
        assert len(runner_writes) == 1
        session_execs = server.recorded("WS", "/v1/sandboxes/sb-1/exec")
        assert len(session_execs) == 1, "two remote() calls must share one exec session"
        assert session_execs[0].json["cmd"][:2] == ["python3", "-u"]
        assert len(server.recorded("POST", "/v1/sandboxes")) == 1
        assert server.last("POST", "/v1/sandboxes/sb-1/terminate").json is None


def test_client_function_map_uses_ephemeral_workers_and_terminates_them(
    monkeypatch, mvm_home
) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        with connect() as client:

            def triple(value: int) -> dict[str, int]:
                return {"value": value, "triple": value * 3}

            remote = client.function(triple, image="python:3.14-slim")
            assert list(remote.map([1, 2, 3, 4, 5], concurrency=3)) == [
                {"value": 1, "triple": 3},
                {"value": 2, "triple": 6},
                {"value": 3, "triple": 9},
                {"value": 4, "triple": 12},
                {"value": 5, "triple": 15},
            ]

        created = server.recorded("POST", "/v1/sandboxes")
        assert 1 <= len(created) <= 3
        for index in range(1, len(created) + 1):
            terminates = server.recorded("POST", f"/v1/sandboxes/sb-{index}/terminate")
            assert len(terminates) == 1, f"worker sb-{index} was not terminated"


def test_function_decorator_reraises_original_exception(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        @function(image="python:3.14-slim")
        def explode() -> None:
            raise ValueError("boom")

        with pytest.raises(ValueError) as exc_info:
            explode.remote()
        explode.terminate()

        assert "boom" in str(exc_info.value)
        notes = "\n".join(getattr(exc_info.value, "__notes__", ()))
        assert "[vmon remote traceback]" in notes
        assert "explode" in notes


def test_map_streams_lazily_and_supports_unordered_and_exceptions(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        @function(image="python:3.14-slim")
        def flaky(value: int) -> int:
            if value == 3:
                raise RuntimeError(f"bad {value}")
            return value * 10

        consumed: list[int] = []

        def feed():
            for value in [1, 2, 4]:
                consumed.append(value)
                yield value

        stream = flaky.map(feed(), concurrency=1)
        first = next(stream)
        assert first == 10
        assert consumed[0] == 1, "feeder must start"
        rest = list(stream)
        assert rest == [20, 40]

        unordered = sorted(flaky.map([1, 2, 4], concurrency=2, order_outputs=False))
        assert unordered == [10, 20, 40]

        mixed = list(flaky.map([1, 3, 4], return_exceptions=True, concurrency=1))
        assert mixed[0] == 10 and mixed[2] == 40
        assert isinstance(mixed[1], RuntimeError) and "bad 3" in str(mixed[1])

        with pytest.raises(RuntimeError, match="bad 3"):
            list(flaky.map([3, 1], concurrency=1))
        flaky.terminate()


def test_map_fail_fast_terminates_every_worker_sandbox(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        @function(image="python:3.14-slim")
        def explode_on(value: int) -> int:
            if value >= 2:
                raise RuntimeError(f"bad {value}")
            return value

        with pytest.raises(RuntimeError):
            list(explode_on.map([1, 2, 3, 4], concurrency=2))
        explode_on.terminate()

        created = server.recorded("POST", "/v1/sandboxes")
        assert created, "map must have created worker sandboxes"
        for index in range(1, len(created) + 1):
            terminates = server.recorded("POST", f"/v1/sandboxes/sb-{index}/terminate")
            assert terminates, f"worker sb-{index} leaked after fail-fast"


def test_starmap_unpacks_argument_tuples(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        @function(image="python:3.14-slim")
        def combine(left: int, right: int) -> int:
            return left * 100 + right

        assert list(combine.starmap([(1, 2), (3, 4)], concurrency=1)) == [102, 304]
        combine.terminate()


def test_spawn_get_cancel_and_gather(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        @function(image="python:3.14-slim")
        def compute(value: int) -> int:
            return value + 1

        @function(image="python:3.14-slim")
        def sleepy() -> None:
            import time as _time

            _time.sleep(60)

        from vmon import FunctionCall

        first = compute.spawn(1)
        second = compute.spawn(41)
        assert FunctionCall.gather(first, second) == [2, 42]
        assert first.done() and second.done()

        hung = sleepy.spawn()
        hung.cancel()
        with pytest.raises(RemoteFunctionError, match="cancelled"):
            hung.get(timeout=10)
        compute.terminate()
        sleepy.terminate()


def test_remote_gen_streams_and_rejects_mismatched_calls(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        @function(image="python:3.14-slim")
        def counter(n: int):
            for i in range(n):
                yield i * 2

        @function(image="python:3.14-slim")
        def plain() -> int:
            return 1

        with pytest.raises(ValueError, match="remote_gen"):
            counter.remote(3)
        with pytest.raises(ValueError, match="remote_gen"):
            counter.spawn(3)
        with pytest.raises(ValueError, match="\\.remote\\(\\)"):
            plain.remote_gen()

        assert list(counter.remote_gen(4)) == [0, 2, 4, 6]
        counter.terminate()
        plain.terminate()


def test_retries_int_form_uses_fixed_one_second_delays(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        @function(image="python:3.14-slim", retries=2)
        def eventually(marker: str) -> str:
            import os as _os

            path = f"/tmp/vmon-retry-{marker}"
            attempt = 1
            if _os.path.exists(path):
                with open(path) as fh:
                    attempt = int(fh.read()) + 1
            with open(path, "w") as fh:
                fh.write(str(attempt))
            if attempt < 3:
                raise RuntimeError(f"attempt {attempt} fails")
            return f"ok after {attempt}"

        delays: list[float] = []
        monkeypatch.setattr("vmon.remote.time.sleep", lambda seconds: delays.append(seconds))

        assert eventually.remote(uuid.uuid4().hex) == "ok after 3"
        assert delays == [1.0, 1.0]
        eventually.terminate()


def test_retries_validation_mirrors_modal(monkeypatch, mvm_home) -> None:
    with pytest.raises(ValueError, match="max_retries"):
        Retries(max_retries=-1)
    with pytest.raises(ValueError, match="backoff_coefficient"):
        Retries(max_retries=1, backoff_coefficient=0.5)
    with pytest.raises(ValueError, match="max_delay"):
        Retries(max_retries=1, max_delay=0.1)
    policy = Retries(max_retries=3, backoff_coefficient=2.0, initial_delay=1.0, max_delay=3.0)
    assert [policy.delay_for(n) for n in (1, 2, 3)] == [1.0, 2.0, 3.0]


def test_sandbox_death_between_calls_recreates_transparently(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        @function(image="python:3.14-slim")
        def ping() -> str:
            return "pong"

        assert ping.remote() == "pong"
        # Simulate server-side VM loss: drop the sandbox and kill its execs.
        server.sandboxes["sb-1"]["view"]["status"] = "terminated"
        server._kill_procs(server.sandboxes["sb-1"])
        del server.sandboxes["sb-1"]
        time.sleep(0.1)

        assert ping.remote() == "pong"
        ping.terminate()
        assert len(server.recorded("POST", "/v1/sandboxes")) == 2


def test_json_serializer_rejects_rich_arguments(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        @function(image="python:3.14-slim", serializer="json")
        def echo(value):
            return value

        with pytest.raises(ValueError) as exc_info:
            echo.remote({1, 2})
        assert str(exc_info.value) == "remote function arguments must be JSON-serializable"
        # Tuples stay JSON-legal (encoded as arrays), matching the old behavior.
        assert echo.remote((1, 2)) == [1, 2]
        # Only the tuple call reached the server; the set was rejected locally.
        assert len(server.recorded("POST", "/v1/sandboxes")) == 1
        echo.terminate()


def test_auto_serializer_round_trips_rich_types(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        @function(image="python:3.14-slim")
        def swap(pair):
            left, right = pair
            return (right, left), {1: "int-key"}

        result = swap.remote((1, 2))
        assert result == ((2, 1), {1: "int-key"})
        assert isinstance(result, tuple) and isinstance(result[0], tuple)
        swap.terminate()


def test_invalid_serializer_rejected_eagerly(monkeypatch, mvm_home) -> None:
    with pytest.raises(ValueError, match="serializer"):

        @function(serializer="msgpack")
        def nope() -> None:
            pass


def test_cls_keeps_state_runs_hooks_and_guards_methods(monkeypatch, mvm_home, tmp_path) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)
        sentinel = tmp_path / "exit-sentinel"

        @cls(image="python:3.14-slim")
        class Counter:
            def __init__(self, start: int, sentinel_path: str) -> None:
                self.n = start
                self.sentinel_path = sentinel_path

            @enter
            def warm(self) -> None:
                self.n += 100

            @method
            def bump(self, by: int = 1) -> int:
                self.n += by
                return self.n

            def hidden(self) -> None:
                pass

            @exit
            def farewell(self) -> None:
                with open(self.sentinel_path, "w") as fh:
                    fh.write(str(self.n))

        counter = Counter(5, str(sentinel))
        assert counter.bump.remote() == 106, "constructor + enter hook must run once"
        assert counter.bump.remote(4) == 110, "state must persist across calls"

        with pytest.raises(AttributeError, match="@vmon.method"):
            _ = counter.hidden

        assert counter.bump.local() == 106, "local instance is independent of the guest"

        started = time.monotonic()
        counter.terminate()
        assert time.monotonic() - started < 10, "terminate must not exhaust the grace period"
        assert sentinel.read_text() == "110", "exit hook must run on terminate"
        assert server.recorded("POST", "/v1/sandboxes/sb-1/terminate")


def test_cls_map_initializes_each_worker(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        @cls(image="python:3.14-slim")
        class Scaler:
            def __init__(self, factor: int) -> None:
                self.factor = factor

            @method
            def scale(self, value: int) -> int:
                return value * self.factor

        scaler = Scaler(3)
        assert list(scaler.scale.map([1, 2, 3], concurrency=2)) == [3, 6, 9]
        scaler.terminate()


def test_async_facade_mirrors_sync_surface(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        @function(image="python:3.14-slim")
        def double(value: int) -> int:
            return value * 2

        @function(image="python:3.14-slim")
        def stream(n: int):
            yield from range(n)

        async def go() -> tuple[int, list[int], list[int], list[int]]:
            remote_result = await double.aio.remote(21)
            call = await double.aio.spawn(5)
            gathered = await type(call).gather(call)
            mapped = [item async for item in double.aio.map([1, 2], concurrency=1)]
            streamed = [item async for item in stream.aio.remote_gen(3)]
            return remote_result, gathered, mapped, streamed

        remote_result, gathered, mapped, streamed = asyncio.run(go())
        assert remote_result == 42
        assert gathered == [10]
        assert mapped == [2, 4]
        assert streamed == [0, 1, 2]
        double.terminate()
        stream.terminate()
