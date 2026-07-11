#!/usr/bin/env python3
"""Benchmark durable function worker startup paths against the Rust server."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import inspect
import json
import os
import platform
import shutil
import sys
import tempfile
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))


def percentile(values: Sequence[int | float], percentile_value: float) -> float:
    """Return a linearly interpolated percentile (stdlib-only, deterministic)."""
    if not values:
        raise ValueError("percentile requires at least one value")
    ordered = sorted(float(value) for value in values)
    position = (len(ordered) - 1) * percentile_value / 100
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def summarize(samples: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if not samples:
        raise ValueError("cannot summarize an empty sample set")
    metrics: dict[str, dict[str, float]] = {}
    for key in ("wall_ms", "queue_ms", "startup_ms", "execution_ms"):
        values = [sample[key] for sample in samples]
        metrics[key] = {"p50": percentile(values, 50), "p95": percentile(values, 95)}
    return {"count": len(samples), "samples": list(samples), "summary": metrics}


def _ordinary(value: int) -> int:
    return value + 1


def _source_digest(*objects: object) -> str:
    source = "\n".join(inspect.getsource(obj) for obj in objects)
    return "sha256:" + hashlib.sha256(source.encode()).hexdigest()


def _sample(call: Any, expected: int) -> tuple[str, dict[str, Any]]:
    # Reconstruct by durable ID before waiting/statting: this benchmark therefore
    # exercises the persisted call contract rather than an in-memory handle.
    import vmon
    from vmon.v1 import api_pb2

    rebuilt = vmon.FunctionCall.from_id(call.id, client=call._client)
    result = rebuilt.get(timeout=180)
    if result != expected:
        raise RuntimeError(f"call {call.id} returned {result!r}, expected {expected!r}")
    stats = rebuilt.stats()
    if len(stats.attempts) != 1:
        raise RuntimeError(
            f"call {call.id} has {len(stats.attempts)} attempts; benchmark requires one"
        )
    attempt = stats.attempts[0]
    names = {
        api_pb2.STARTUP_KIND_COLD: "cold",
        api_pb2.STARTUP_KIND_WARM: "warm",
        api_pb2.STARTUP_KIND_SNAPSHOT: "snapshot",
    }
    category = names.get(attempt.startup, f"unknown:{attempt.startup}")
    return category, {
        "call_id": call.id,
        "attempt_id": attempt.attempt_id,
        "wall_ms": stats.wall_millis,
        "queue_ms": attempt.queued_millis,
        "startup_ms": attempt.startup_millis,
        "execution_ms": attempt.execution_millis,
    }


def _collect(
    spawn: Callable[[int], Any], wanted: set[str], iterations: int
) -> dict[str, list[dict[str, Any]]]:
    samples = {category: [] for category in wanted}
    limit = max(12, iterations * len(wanted) * 4)
    observed: dict[str, int] = {}
    for value in range(limit):
        category, sample = _sample(spawn(value), value + 1)
        observed[category] = observed.get(category, 0) + 1
        if category in samples and len(samples[category]) < iterations:
            samples[category].append(sample)
        if all(len(items) == iterations for items in samples.values()):
            return samples
    missing = {
        name: iterations - len(items) for name, items in samples.items() if len(items) < iterations
    }
    raise RuntimeError(
        f"startup categories not observed within {limit} calls; "
        f"missing={missing}, observed={observed}"
    )


def run(iterations: int, output: Path) -> None:
    import e2e
    import vmon

    if iterations < 1:
        raise ValueError("--iterations must be at least 1")
    home: Path | None = None
    server = None
    managed_log: Path | None = None
    url = os.environ.get("VMON_SERVER_URL")
    if url:
        client = vmon.connect(url, discover=False)
        server_mode = "external"
        server_log = os.environ.get("VMON_SERVER_LOG")
    else:
        home = Path(tempfile.mkdtemp(prefix="vmon-bench-"))
        e2e.configure_home(home)
        server, reason = e2e.preflight(home)
        if reason:
            raise RuntimeError(reason)
        client = e2e.sdk()
        server_mode = "managed"
        managed_log = server.log_path
        server_log = str(output.parent / "server.log")

    image = vmon.Image.from_registry(
        os.environ.get("VMON_E2E_PYTHON_IMAGE", "python:3.14-slim")
    ).uv_install("cloudpickle==3.1.2")
    network = vmon.NetworkPolicy(block_network=True)
    ordinary_options = vmon.FunctionOptions(
        image=image,
        resources=vmon.ResourcePolicy(network=network),
        workers=vmon.WorkerPolicy(max_workers=1, max_calls_per_worker=2),
        serializer=vmon.SerializerPolicy(allow_trusted_python=True),
    )

    @vmon.service(
        client=client,
        name="benchmark-snapshot-service",
        snapshot_after_initialize=True,
        snapshot_on_worker_retire=True,
        package_mode="cloudpickle",
        options=vmon.FunctionOptions(
            image=image,
            resources=vmon.ResourcePolicy(network=network),
            workers=vmon.WorkerPolicy(max_workers=1, max_calls_per_worker=1),
            serializer=vmon.SerializerPolicy(allow_trusted_python=True),
        ),
    )
    class SnapshotService:
        def __init__(self) -> None:
            self.ready = False

        @vmon.enter(snapshot=True)
        def initialize(self) -> None:
            self.ready = True

        @vmon.on_restore
        def restored(self) -> None:
            # Presence of this hook makes restoration part of the service
            # lifecycle, while AttemptStats remains the source of classification.
            self.ready = True

        @vmon.method
        def increment(self, value: int) -> int:
            if not self.ready:
                raise RuntimeError("snapshot initialization was not restored")
            return value + 1

    ordinary = vmon.function(
        _ordinary,
        client=client,
        name="benchmark-ordinary",
        options=ordinary_options,
        package_mode="cloudpickle",
    )
    service = SnapshotService()
    try:
        ordinary_samples = _collect(ordinary.spawn, {"cold", "warm"}, iterations)
        snapshot_samples = _collect(service.increment.spawn, {"snapshot"}, iterations)
        revisions = {
            "ordinary": ordinary._resolve().ref.revision_id,
            "snapshot_service": SnapshotService._function._resolve().ref.revision_id,
        }
        payload = {
            "schema_version": 1,
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "environment": {
                "platform": platform.platform(),
                "os": platform.system(),
                "architecture": platform.machine(),
                "python": platform.python_version(),
                "vmon_python": getattr(vmon, "__version__", None),
                "vmon_server": server_mode,
                "server_log": server_log,
            },
            "iterations": iterations,
            "identity": {
                "source_digest": _source_digest(_ordinary, SnapshotService._cls),
                "revisions": revisions,
            },
            "categories": {
                "cold": summarize(ordinary_samples["cold"]),
                "warm": summarize(ordinary_samples["warm"]),
                "snapshot": summarize(snapshot_samples["snapshot"]),
            },
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    finally:
        with contextlib.suppress(Exception):
            client.close()
        if server is not None:
            server.stop()
            if managed_log is not None and managed_log.exists():
                output.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(managed_log, output.parent / "server.log")
        if home is not None:
            shutil.rmtree(home, ignore_errors=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="benchmark cold, warm, and snapshot-restored vmon function workers"
    )
    parser.add_argument(
        "--iterations", type=int, default=10, help="samples required per startup category"
    )
    parser.add_argument("--output", type=Path, required=True, help="JSON results path")
    args = parser.parse_args(argv)
    run(args.iterations, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
