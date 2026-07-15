"""Durable function, batch, and actor example using the public Python SDK."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import uuid
from collections.abc import AsyncIterator
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from vmon import (
    App,
    BatchCall,
    FunctionCall,
    FunctionOptions,
    Image,
    RemoteClass,
    RemoteFunction,
    SerializerPolicy,
    ValueCodec,
    cls,
    connect,
    function,
    method,
)

BATCH_SIZE = 12
EXECUTION_TIMEOUT_SECONDS = 45.0
GET_TIMEOUT_SECONDS = 75.0
JsonRecord = dict[str, str | int]

PYTHON_IMAGE = Image.python("3.14", slim=True)
JSON_SERIALIZER = SerializerPolicy(
    input_serializer=ValueCodec.JSON,
    result_serializer=ValueCodec.JSON,
)
TRUSTED_PYTHON_SERIALIZER = SerializerPolicy(
    input_serializer=ValueCodec.CLOUDPICKLE,
    result_serializer=ValueCodec.CLOUDPICKLE,
    allow_trusted_python=True,
)


@function(
    name="durable-python-json",
    image=PYTHON_IMAGE,
    serializer=JSON_SERIALIZER,
    execution_timeout=EXECUTION_TIMEOUT_SECONDS,
    result_ttl=3_600.0,
)
def portable_json(payload: JsonRecord) -> JsonRecord:
    """Process JSON values without an injected worker context parameter."""
    # Durable execution is at least once. This example only computes a value. If
    # it wrote to an external system, operation_id would be its idempotency key.
    operation_id = str(payload["operation_id"])
    value = int(payload["value"])
    print(f"processing operation_id={operation_id}")
    return {"operation_id": operation_id, "square": value * value}


@dataclass(frozen=True, slots=True)
class RichInput:
    operation_id: uuid.UUID
    observed_at: datetime
    samples: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class RichResult:
    operation_id: uuid.UUID
    observed_at: datetime
    unique_samples: frozenset[int]


@function(
    name="durable-python-trusted-values",
    image=PYTHON_IMAGE,
    serializer=TRUSTED_PYTHON_SERIALIZER,
    execution_timeout=EXECUTION_TIMEOUT_SECONDS,
    result_ttl=3_600.0,
)
def trusted_rich_values(payload: RichInput) -> RichResult:
    """Round-trip explicitly trusted Python-only values through cloudpickle."""
    return RichResult(payload.operation_id, payload.observed_at, frozenset(payload.samples))


@cls(name="durable-python-checkpointed-ledger")
class CheckpointedLedger:
    """Actor whose mutation remains safe when a method attempt is replayed."""

    def __init__(self) -> None:
        self.total = 0
        self.applied: dict[str, int] = {}

    @method
    def apply_once(self, operation_id: str, amount: int) -> int:
        # Actor method calls are durable and at least once. Recording the operation
        # key with the state makes a replay return the prior result without applying
        # the external intent twice.
        if operation_id not in self.applied:
            self.total += amount
            self.applied[operation_id] = amount
        return self.total

    @method
    def balance(self) -> int:
        return self.total


@dataclass(frozen=True, slots=True)
class Arguments:
    server_url: str | None
    token: str | None
    namespace: str
    app_name: str
    run_name: str
    call_id_file: Path


def _optional_environment(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def parse_args() -> Arguments:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--server-url",
        default=_optional_environment("VMON_SERVER_URL"),
        help="server URL; otherwise connect() uses VMON_DSN, VMON_CONTEXT, or the local socket",
    )
    parser.add_argument(
        "--token",
        default=_optional_environment("VMON_API_TOKEN"),
        help="API token; otherwise the selected context or DSN supplies it",
    )
    parser.add_argument("--namespace", default=os.environ.get("VMON_NAMESPACE", "default"))
    parser.add_argument("--app-name", default=_optional_environment("VMON_APP_NAME"))
    parser.add_argument("--run-name", default=_optional_environment("VMON_RUN_NAME"))
    parser.add_argument(
        "--call-id-file",
        type=Path,
        default=Path("durable-python-call.json"),
        help="file in which to persist the unary and batch call IDs",
    )
    parsed = parser.parse_args()
    suffix = uuid.uuid4().hex
    return Arguments(
        server_url=parsed.server_url,
        token=parsed.token,
        namespace=parsed.namespace,
        app_name=parsed.app_name or f"durable-python-{suffix}",
        run_name=parsed.run_name or f"durable-python-{suffix}",
        call_id_file=parsed.call_id_file,
    )


def save_call_ids(path: Path, *, unary: str, batch: str | None = None) -> None:
    """Persist stable handles so another process can reconstruct the calls."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"unary": unary, "batch": batch}, indent=2) + "\n", encoding="utf-8")


def load_unary_call(path: Path, *, client: Any) -> FunctionCall[JsonRecord]:
    saved = json.loads(path.read_text(encoding="utf-8"))
    return FunctionCall.from_id(str(saved["unary"]), client=client)


def deployed_function(
    revision: Any,
    binding_name: str,
    *,
    client: Any,
    serializer: SerializerPolicy,
) -> RemoteFunction[..., Any]:
    binding = next(item for item in revision.manifest.functions if item.name == binding_name)
    return RemoteFunction.from_name(
        binding.function_name,
        namespace=binding.namespace,
        revision=binding.revision_id,
        client=client,
        options=FunctionOptions(serializer=serializer),
    )


async def batch_inputs(run_name: str) -> AsyncIterator[JsonRecord]:
    # spawn_map consumes this lazily with server backpressure. Keep producers
    # bounded so an example invocation cannot submit an accidental infinite job.
    for index in range(BATCH_SIZE):
        yield {"operation_id": f"{run_name}:batch:{index}", "value": index}
        await asyncio.sleep(0)


async def run_async_calls(
    json_function: RemoteFunction[..., Any],
    *,
    run_name: str,
    client: Any,
    call_id_file: Path,
    unary_id: str,
) -> None:
    immediate = await json_function.aio.remote(
        {"operation_id": f"{run_name}:async-remote", "value": 9}
    )
    print("async remote result", immediate)

    batch = await json_function.aio.spawn_map(batch_inputs(run_name), max_in_flight=4)
    save_call_ids(call_id_file, unary=unary_id, batch=batch.id)
    ordered = [item async for item in batch.results(order_outputs=True)]
    print("ordered batch results", ordered)

    reconstructed = BatchCall.from_id(batch.id, client=client)
    unordered = [item async for item in reconstructed.aio.results(order_outputs=False)]
    print("completion-order batch results", unordered)
    print("batch status", reconstructed.status())


def run_actor(actor_class: RemoteClass, *, run_name: str) -> None:
    actor = actor_class(labels={"example": "durable-functions", "run": run_name})
    try:
        operation_id = f"{run_name}:ledger-credit"
        print("actor balance", actor.apply_once.remote(operation_id, 7))
        checkpoint = actor.checkpoint()
        recovered = actor_class.from_id(actor.id)
        print("checkpoint", asdict(checkpoint))
        print("recovered actor balance", recovered.balance.remote())
    finally:
        actor.delete()
        print("deleted actor", actor.id)


def main() -> None:
    args = parse_args()
    # A direct VMON_SERVER_URL wins when supplied. With no explicit URL or token,
    # connect() retains normal VMON_DSN/VMON_CONTEXT resolution, including a
    # selected context's token.
    with connect(args.server_url, token=args.token) as client:
        app = App(args.app_name, namespace=args.namespace, client=client)
        app.include(portable_json, name="json")
        app.include(trusted_rich_values, name="trusted-python")
        app.include(CheckpointedLedger, name="ledger")
        activated = app.deploy()

        # Resolve the activated app again rather than relying on local definitions.
        deployed = App.from_name(args.app_name, namespace=args.namespace, client=client).get()
        json_function = deployed_function(
            deployed,
            "json",
            client=client,
            serializer=JSON_SERIALIZER,
        )
        rich_function = deployed_function(
            deployed,
            "trusted-python",
            client=client,
            serializer=TRUSTED_PYTHON_SERIALIZER,
        )
        print("activated app revision", activated.revision_id)

        spawned = json_function.spawn(
            {"operation_id": f"{args.run_name}:durable-unary", "value": 12}
        )
        save_call_ids(args.call_id_file, unary=spawned.id)
        reconstructed = load_unary_call(args.call_id_file, client=client)
        # execution_timeout limits one server attempt. get(timeout=...) only limits
        # this local wait, so it is deliberately a separate, larger duration.
        print("reconstructed unary result", reconstructed.get(timeout=GET_TIMEOUT_SECONDS))
        print("unary status", reconstructed.status())
        print("unary stats", reconstructed.stats())
        for line in reconstructed.logs(follow=False):
            print("remote log", line.decode("utf-8", errors="replace").rstrip())

        rich = RichInput(uuid.uuid4(), datetime.now(UTC), (3, 1, 3, 2))
        print("trusted Python-only result", rich_function.spawn(rich).get(GET_TIMEOUT_SECONDS))

        cancellable = json_function.spawn({"operation_id": f"{args.run_name}:cancel", "value": 5})
        cancellable.cancel("durable-function example cancellation")
        print("cancelled call status", cancellable.status())

        asyncio.run(
            run_async_calls(
                json_function,
                run_name=args.run_name,
                client=client,
                call_id_file=args.call_id_file,
                unary_id=spawned.id,
            )
        )
        run_actor(CheckpointedLedger, run_name=args.run_name)


if __name__ == "__main__":
    main()
