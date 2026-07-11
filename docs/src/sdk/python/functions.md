# Durable Functions

The Python SDK turns an importable Python callable into a durable function owned
and scheduled by the vmon daemon. It is a client library: it does not run a
worker, image runtime, or daemon in the Python process. [Durable calls in Shared
Concepts](../shared-concepts.md#durable-calls) explains the protocol-level model
behind this page: server-owned at-least-once attempts, stable call IDs,
sequence-resumable results and events, and result-retention and observability
boundaries. The Python decorator, source packaging, and serialization details
below are Python-specific.

## Define a function

Use `@vmon.function` on a module-level callable. The decorator preserves the
callable's typed signature and produces a `RemoteFunction`. A synchronous
function, coroutine function, synchronous generator, and async generator get
the corresponding typed remote wrapper.

```python
# myapp/tasks.py
import vmon

@vmon.function
def add(left: int, right: int) -> int:
    return left + right

@vmon.function
def chunks(text: str):
    for word in text.split():
        yield word
```

Call a unary function with `remote()` or create a durable handle first with
`spawn()`. Call a generator with `remote_gen()`.

```python
from myapp.tasks import add, chunks

assert add.remote(20, 22) == 42

call = add.spawn(20, 22)
print(call.id)                 # stable durable call ID
assert call.get(timeout=30) == 42

assert list(chunks.remote_gen("one two")) == ["one", "two"]
```

`FunctionCall` exposes `id`, `get_record()`, `stats()`, `graph()`, `cancel()`,
`events()`, `logs()`, and, for generator calls, `iter_yields()`. A call is
recorded before execution, so a handle can be reconstructed with
`FunctionCall.from_id(call_id, client=client)` and inspected or awaited later.
`get()` raises a remote execution error for a failed call and `TimeoutError`
when its local wait limit expires; the latter does not change server execution.

`remote()` is only for unary functions; `remote_gen()` is only for generator
functions. The SDK validates the local signature before it submits arguments.

## Package Python source

Python function definitions are Python-specific. With the default
`package_mode="package"`, the SDK creates a deterministic ZIP of the callable's
source package and sends a manifest containing the Python ABI, module,
qualified name, and checksums. The daemon imports that module in a worker.
This is not the portable cross-language function ABI.

The callable must come from a normal importable module with filesystem source.
`__main__` callables, nested functions, and closures cannot use package mode.
Choose the package root and additional local packages explicitly when the
default module root is not sufficient:

```python
import vmon

@vmon.function(
    package_root=".",
    include=("myapp/**/*.py",),
    exclude=("**/__pycache__/**",),
    local_packages=("../shared_python",),
)
def summarize(lines: list[str]) -> dict[str, int]:
    return {"lines": len(lines)}
```

`vmon.package_callable()` is useful when you need to examine or construct the
same artifact yourself. `vmon.inspect_manifest()` reads and validates its
embedded `vmon-package.json`; `vmon.build_package()` builds from an explicit
root, module, and qualified name.

```python
from myapp.tasks import add
import vmon

artifact = vmon.package_callable(add)
manifest = vmon.inspect_manifest(artifact.data)
assert manifest.module == "myapp.tasks"
assert manifest.qualname == "add"
```

## Serialization and trust

Function arguments and results use checksummed value envelopes. Select a codec
with `serializer=` (or a `SerializerPolicy`): `json`, `cbor`, or
`cloudpickle`. JSON is the default portable choice; it accepts the SDK's
JSON-compatible profile and rejects non-finite numbers and values outside that
profile. CBOR supports a broader data model but remains a value codec, not
Python code packaging.

`cloudpickle` is intentionally different: it serializes Python objects and
code, is bound to the Python ABI and cloudpickle version, and can execute code
when decoded. It is rejected unless the definition explicitly enables trusted
Python serialization. Use it only when every producer, stored payload, and
consumer is trusted. Do not use it to accept values from an untrusted caller or
as a compatibility mechanism between languages.

```python
import vmon

trusted_python = vmon.SerializerPolicy(
    input_serializer=vmon.ValueCodec.CLOUDPICKLE,
    result_serializer=vmon.ValueCodec.CLOUDPICKLE,
    allow_trusted_python=True,
)

@vmon.function(serializer=trusted_python, package_mode="cloudpickle")
def trusted_only(value):
    return value
```

`package_mode="cloudpickle"` also serializes the callable itself and is
therefore an explicit trusted-Python choice. It can represent closures that
package mode rejects, but it does not make their restoration safe,
cross-language, or ABI-independent. Envelope decoding likewise requires an
explicit trust gate:

```python
envelope = vmon.encode_value({"safe": ["portable"]}, vmon.ValueCodec.JSON)
value = vmon.decode_value(envelope)  # checksum and codec are verified

# Only for a Cloudpickle envelope from a source you trust:
# value = vmon.decode_value(envelope, trusted=True)
```

Decode failures are reported as `EnvelopeIntegrityError`, `CodecMismatchError`,
or `ABIMismatchError` as applicable. See [Errors](errors.md).

## Options, maps, and retries

Pass execution configuration directly to `@vmon.function`, or build a
`FunctionOptions` value. The decorator supports resource, image, network,
volume, timeout, worker, retry, serialization, and packaging arguments. For
the common decorators, `@vmon.concurrent(...)` sets a worker concurrency
policy and `@vmon.batched(max_batch_size=..., max_wait=...)` enables bounded
worker-side batching.

```python
import vmon

@vmon.function(retries=3, execution_timeout=60)
def charge(order_id: str) -> str:
    return f"charged {order_id}"

for outcome in charge.map(["a-1", "a-2"], max_in_flight=2):
    print(outcome)
```

`spawn_map()` creates a detached `BatchCall`; `map()` streams its results;
`starmap()` treats each item as positional arguments; and `for_each()` consumes
results. `max_in_flight` must be a positive integer. `BatchCall.results()` can
return remote exceptions as values with `return_exceptions=True`; otherwise the
first fetched failure is raised.

Retries and scheduling are server-owned. A retry policy provides
at-least-once attempt semantics, not exactly-once execution: a side effect can
occur more than once. Make external effects idempotent and use a durable input,
request, or business key for deduplication. Do not implement a second client
retry loop merely because a call is pending or a local `get()` timed out.

Inside a worker, `vmon.is_remote()` reports whether `VMON_REMOTE=1`, and
`vmon.current_call()` exposes call metadata such as `call_id`, `attempt`,
`input_id`, and `actor_id`. `current_call()` raises `RuntimeError` outside a
vmon-dispatched call.

## Select a worker image

Pass an image reference or compose a Python image with `vmon.Image`. The image
configuration is sent with the function definition; it does not execute or
build a runtime in the client process.

```python
import vmon

image = (
    vmon.Image.python("3.14")
    .uv_install("numpy")
    .add_local_python_source("myapp")
)

@vmon.function(image=image)
def mean(values: list[float]) -> float:
    import numpy
    return float(numpy.mean(values))
```

`Image.python()` validates a Python version and chooses the slim variant by
default. Other sources are `Image.from_registry(...)`,
`Image.from_dockerfile(...)`, and `Image.from_template(...)`; templates require
an immutable SHA-256 revision. Image steps include `uv_sync`, `uv_install`,
`apt_install`, `env`, `run_commands`, `add_local_file`, and
`add_local_python_source`. The callable package and its worker image are
separate artifacts: source package mode determines how Python finds the
callable, while the image determines the worker environment.
