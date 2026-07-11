from __future__ import annotations

from types import SimpleNamespace

import pytest

import vmon
from vmon._function_proto import value_from_proto
from vmon.decorators import CurrentCall, _use_current_call
from vmon.v1 import api_pb2 as pb


class Driver:
    def __init__(self, stubs):
        self.stubs = stubs

    def call(self, fn, **_):
        return fn(self.stubs), "fake"


class FunctionStub:
    def __init__(self):
        self.activate = None
        self.created_schedule = None
        self.deleted_schedule = None
        self.rollback = None

    def ActivateApp(self, request):
        self.activate = request
        return pb.AppRevision(
            ref=pb.AppRevisionRef(app=request.app, revision_id="app-r1"),
            functions=request.functions,
            created_at_unix_millis=7,
        )

    def GetApp(self, request):
        app = request.app.pinned.app if request.app.HasField("pinned") else request.app.current
        revision = request.app.pinned.revision_id if request.app.HasField("pinned") else "current-r"
        return pb.AppRevision(ref=pb.AppRevisionRef(app=app, revision_id=revision))

    def RollbackApp(self, request):
        self.rollback = request
        return pb.AppRevision(
            ref=pb.AppRevisionRef(app=request.target.app, revision_id=request.target.revision_id),
            previous=request.expected_current,
        )

    def CreateSchedule(self, request):
        self.created_schedule = request
        return pb.ScheduleRecord(
            ref=pb.ScheduleRef(schedule_id=request.schedule_id or "schedule-1"),
            spec=request.spec,
            created_at_unix_millis=1,
            updated_at_unix_millis=2,
            next_run_unix_millis=3,
        )

    def ListSchedules(self, request):
        return pb.ListSchedulesResponse(
            schedules=[
                self.CreateSchedule(
                    pb.CreateScheduleRequest(
                        spec=pb.ScheduleSpec(
                            name="tick",
                            app=pb.AppRevisionRef(app=request.app, revision_id="app-r1"),
                            target=pb.ScheduleTarget(
                                function=pb.RevisionRef(
                                    function=pb.FunctionRef(namespace="ns", name="worker"),
                                    revision_id="fn-r1",
                                ),
                                input=pb.ValueEnvelope(inline_data=b"null"),
                            ),
                            cron=pb.CronSchedule(expression="* * * * *", time_zone="UTC"),
                        )
                    )
                )
            ],
            next_page_token="next",
        )

    def DeleteSchedule(self, request):
        self.deleted_schedule = request
        return pb.Ok()


class Definition:
    def __init__(self, name):
        self.name = name

    def _resolve(self):
        return pb.FunctionRevision(
            ref=pb.RevisionRef(
                function=pb.FunctionRef(namespace="ns", name=self.name),
                revision_id=f"{self.name}-r1",
            )
        )


def test_app_free_definition_is_lazy_zero_deploy():
    @vmon.function
    def add(one, two):
        return one + two

    assert isinstance(add, vmon.RemoteFunction)
    assert add.local(2, 3) == 5
    assert add._registered is None


def test_app_function_delegates_the_typed_option_surface():
    app = vmon.App("typed", namespace="ns")

    @app.function(
        cpus=2,
        memory=1024,
        min_workers=1,
        max_workers=4,
        buffer_workers=2,
        scale_down_after=10,
        retries=3,
        execution_timeout=30,
        egress=("10.0.0.0/8", "api.example.com"),
    )
    def work(value):
        return value

    assert work.namespace == "ns"
    assert work.options.resources.cpu_millis == 2000
    assert work.options.resources.memory_bytes == 1024
    assert work.options.workers.min_workers == 1
    assert work.options.workers.max_workers == 4
    assert work.options.retry.max_attempts == 4
    assert work.options.timeouts.execution == 30
    assert work.options.resources.network.egress_cidrs == ("10.0.0.0/8",)
    assert work.options.resources.network.egress_domains == ("api.example.com",)

    @app.function(name="blocked", block_network=True)
    def blocked(value):
        return value

    assert blocked.options.resources.network.block_network is True


def test_app_activation_is_complete_atomic_and_lookup_can_be_pinned():
    functions = FunctionStub()
    client = SimpleNamespace(driver=Driver(SimpleNamespace(functions=functions)))
    app = vmon.App("api", namespace="ns", client=client)
    app.add(Definition("one"))
    app.add(Definition("two"))

    revision = app.deploy(request_id="deploy-1")

    assert revision.revision_id == "app-r1"
    assert [item.name for item in functions.activate.functions] == ["one", "two"]
    assert all(item.revision.revision_id.endswith("-r1") for item in functions.activate.functions)
    assert app.get("old-r").revision_id == "old-r"
    rolled_back = app.rollback("old-r", expected_current=revision, request_id="rollback-1")
    assert rolled_back.revision_id == "old-r"
    assert functions.rollback.target.revision_id == "old-r"
    assert functions.rollback.expected_current.revision_id == "app-r1"


def test_app_schedule_uses_pinned_revisions_and_server_owned_rpc():
    functions = FunctionStub()
    client = SimpleNamespace(driver=Driver(SimpleNamespace(functions=functions)))
    app = vmon.App("api", namespace="ns", client=client)
    manifest = vmon.AppManifest("ns", "api", (vmon.AppBinding("job", "ns", "worker", "fn-r1"),))
    revision = vmon.AppRevision(manifest, "app-r1")

    schedule = app.create_schedule(
        "tick",
        "job",
        vmon.Cron("*/5 * * * *"),
        input={"x": 1},
        app_revision=revision,
        schedule_id="stable",
        request_id="request",
    )

    request = functions.created_schedule
    assert schedule.id == "stable"
    assert request.spec.app.revision_id == "app-r1"
    assert request.spec.target.function.revision_id == "fn-r1"
    assert request.spec.target.input.serializer == pb.VALUE_SERIALIZER_JSON
    records, token = app.list_schedules()
    assert records[0].timing == vmon.Cron("* * * * *") and token == "next"
    app.delete_schedule(schedule)
    assert functions.deleted_schedule.schedule_id == "stable"


def test_definition_decorators_validate_compose_and_context_is_scoped():
    @vmon.batched(max_batch_size=8, wait_ms=10)
    @vmon.concurrent(4)
    def work(value):
        return value

    assert work.__vmon_options__.concurrency.max_concurrent_calls == 4
    assert work.__vmon_options__.batching.max_batch_size == 8

    @vmon.function
    @vmon.concurrent(3)
    def deployed(value):
        return value

    assert deployed.options.concurrency.max_concurrent_calls == 3
    with pytest.raises(ValueError):
        vmon.concurrent(0)
    with pytest.raises(RuntimeError):
        vmon.current_call()
    metadata = CurrentCall("call-1", actor_id="actor-1", input_index=2)
    with _use_current_call(metadata):
        assert vmon.current_call() is metadata
    with pytest.raises(RuntimeError):
        vmon.current_call()


class Call:
    def __init__(self, call_id, value=None, error=None):
        self.id, self.value, self.error = call_id, value, error
        self.cancelled = []

    def get(self):
        if self.error:
            raise self.error
        return self.value

    def cancel(self, reason=""):
        self.cancelled.append(reason)


def test_call_group_cancels_siblings_on_error_and_scope_exit():
    one, two = Call("one", 1), Call("two", error=ValueError("bad"))
    with pytest.raises(ValueError):
        vmon.CallGroup([one, two]).gather()
    assert one.cancelled and two.cancelled
    pending = Call("pending")
    with vmon.CallGroup([pending]):
        pass
    assert pending.cancelled


class ActorsStub:
    def __init__(self):
        self.calls = []
        self.next = 1

    def _record(self, actor_id, status=pb.ACTOR_STATUS_READY):
        return pb.ActorRecord(
            ref=pb.ActorRef(actor_id=actor_id),
            function=pb.RevisionRef(
                function=pb.FunctionRef(namespace="ns", name="Counter"), revision_id="r1"
            ),
            status=status,
        )

    def Create(self, request):
        self.calls.append(("create", request))
        return self._record("actor-1")

    def Get(self, request):
        self.calls.append(("get", request))
        return self._record(request.actor_id)

    def Checkpoint(self, request):
        self.calls.append(("checkpoint", request))
        return pb.ActorCheckpoint(
            ref=pb.ActorCheckpointRef(checkpoint_id="checkpoint-1"),
            actor=request.actor,
            sequence=4,
            created_at_unix_millis=5,
        )

    def Restore(self, request):
        self.calls.append(("restore", request))
        return self._record(request.actor.actor_id)

    def Fork(self, request):
        self.calls.append(("fork", request))
        return self._record("actor-2")


class FakeFunction:
    def __init__(self, client):
        self._client = client
        self.targets = []
        self.services = []

    def _resolve(self):
        return pb.FunctionRevision(
            ref=pb.RevisionRef(
                function=pb.FunctionRef(namespace="ns", name="Counter"), revision_id="r1"
            )
        )

    def _spawn_actor(self, actor_id, method, args, kwargs):
        self.targets.append((actor_id, method, args, kwargs))
        return Call("call-1", 9)

    def _spawn_service(
        self, service_key, constructor_args, constructor_kwargs, method, args, kwargs
    ):
        self.services.append(
            (service_key, constructor_args, constructor_kwargs, method, args, kwargs)
        )
        return Call("service-call", 11)

    def with_options(self, options=None):
        return self


@vmon.method
def _dummy(self):
    pass


class Counter:
    def __init__(self, start=0):
        self.value = start
        self.hooks = []

    @vmon.enter(snapshot=True)
    def before_snapshot(self):
        self.hooks.append("snapshot")

    @vmon.enter
    def initialize(self):
        self.hooks.append("initialize")

    @vmon.on_restore
    def restored(self):
        self.hooks.append("restore")

    @vmon.method
    def add(self, value):
        self.value += value
        return self.value


def test_actor_recovery_targets_lifecycle_and_service_distinction():
    actors = ActorsStub()
    client = SimpleNamespace(driver=Driver(SimpleNamespace(actors=actors)))
    function = FakeFunction(client)
    definition = vmon.RemoteClass(Counter, client=client, namespace="ns", _function=function)
    actor = definition(1)
    assert actor.id == "actor-1"
    create_request = actors.calls[0][1]
    assert create_request.WhichOneof("initial_payload") == "initial_arguments"
    assert vmon.decode_value(value_from_proto(create_request.initial_arguments.positional[0])) == 1
    assert definition.metadata.snapshot_enter == ("before_snapshot",)
    assert definition.metadata.enter == ("initialize",)
    assert definition.metadata.enter_order == (
        ("before_snapshot", True),
        ("initialize", False),
    )
    assert definition.metadata.restore == ("restored",)
    assert actor.add.remote(3) == 9
    assert function.targets == [("actor-1", "add", (3,), {})]
    assert actor.add(3) == 4
    assert actor.add.local(1) == 5
    assert actor._local_instance.hooks == ["snapshot", "initialize"]

    recovered = definition.from_id("actor-1")
    with pytest.raises(RuntimeError, match="constructor is intentionally not replayed"):
        recovered.add.local(1)
    checkpoint = recovered.checkpoint()
    recovered.restore(checkpoint)
    fork = recovered.fork(checkpoint)
    assert fork.id == "actor-2"
    assert [name for name, _ in actors.calls].count("create") == 1

    service_function = FakeFunction(client)
    scalable = vmon.RemoteClass(Counter, client=client, service=True, _function=service_function)
    assert scalable.lifecycle == "service"
    first = scalable(4)
    second = scalable(4)
    different = scalable(5)
    with pytest.raises(TypeError):
        _ = first.id
    assert first.add(1) == 5
    assert first._local_instance.hooks == ["snapshot", "initialize"]
    assert first.add.remote(6) == 11
    assert second.add.remote(7) == 11
    assert different.add.remote(8) == 11
    assert service_function.services[0][0] == service_function.services[1][0]
    assert service_function.services[0][0] != service_function.services[2][0]
    assert service_function.services[0][1:] == ((4,), {}, "add", (6,), {})


def test_service_spawn_uses_native_typed_target_and_arguments():
    class Calls:
        request = None

        def Create(self, request):
            self.request = request
            return pb.CallRecord(ref=pb.CallRef(call_id="service-call"))

    calls = Calls()
    client = SimpleNamespace(driver=Driver(SimpleNamespace(calls=calls)))
    function = vmon.RemoteFunction.from_name(
        "Counter", namespace="ns", revision="r1", client=client
    )
    function._registered = pb.FunctionRevision(
        ref=pb.RevisionRef(
            function=pb.FunctionRef(namespace="ns", name="Counter"),
            revision_id="r1",
        )
    )

    handle = function._spawn_service("stable-key", (4,), {"start": True}, "add", (6,), {"step": 2})

    request = calls.request
    assert handle.id == "service-call"
    assert request.type == pb.CALL_TYPE_UNARY
    assert request.target.WhichOneof("receiver") == "service"
    assert request.target.service.service_key == "stable-key"
    assert request.target.service.method == "add"
    assert (
        vmon.decode_value(value_from_proto(request.target.service.constructor.positional[0])) == 4
    )
    assert (
        vmon.decode_value(value_from_proto(request.target.service.constructor.named["start"]))
        is True
    )
    assert vmon.decode_value(value_from_proto(request.inputs[0].arguments.positional[0])) == 6
    assert vmon.decode_value(value_from_proto(request.inputs[0].arguments.named["step"])) == 2

    function._spawn_actor("actor-7", "add", (9,), {"step": 3})
    actor_request = calls.request
    assert actor_request.type == pb.CALL_TYPE_ACTOR
    assert actor_request.target.WhichOneof("receiver") == "actor"
    assert actor_request.target.actor.actor.actor_id == "actor-7"
    assert actor_request.target.actor.method == "add"
    assert vmon.decode_value(value_from_proto(actor_request.inputs[0].arguments.positional[0])) == 9


def test_stopped_actor_remains_recoverable():
    actors = ActorsStub()
    actors.Get = lambda request: actors._record(request.actor_id, pb.ACTOR_STATUS_STOPPED)
    client = SimpleNamespace(driver=Driver(SimpleNamespace(actors=actors)))
    definition = vmon.RemoteClass(Counter, client=client, _function=FakeFunction(client))

    stopped = definition.from_id("sleeping")

    assert stopped.id == "sleeping"


def test_actor_lost_is_explicit_not_constructor_replay():
    actors = ActorsStub()
    actors.Get = lambda request: actors._record(request.actor_id, pb.ACTOR_STATUS_FAILED)
    client = SimpleNamespace(driver=Driver(SimpleNamespace(actors=actors)))
    definition = vmon.RemoteClass(Counter, client=client, _function=FakeFunction(client))
    with pytest.raises(vmon.ActorLostError, match="failed"):
        definition.from_id("gone")
