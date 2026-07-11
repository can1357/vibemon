# Errors

The Python SDK exposes stable error categories rather than requiring callers to
parse error text. Catch the narrow category that matches the boundary you are
handling. For daemon-owned retry and durable call behavior, see
[Shared Concepts](../shared-concepts.md) and [Durable Functions](functions.md).

## API and transport failures

`APIError` means the vmon API returned a structured error response. It contains:

* `message` — the server-provided message;
* `code` — the stable structured API code (default `"engine"`);
* `status` — the HTTP status for HTTP API surfaces; gRPC callers should use
  `code`, because their `APIError.status` is not populated.

`TransportError` means the client could not complete I/O with an endpoint. It
contains `message` and the optional `endpoint`. Mesh failover considers
transport failures eligible for another endpoint; it does not fail over a
structured `APIError`. `ProtocolError` means an endpoint returned malformed
protocol data, including HTTP or WebSocket payloads, gRPC `JsonView` values,
and gRPC stream frames.

```python
import vmon

try:
    with vmon.connect("vmon://node-a,node-b") as client:
        client.health()
except vmon.TransportError as error:
    print(f"could not reach {error.endpoint}: {error.message}")
except vmon.APIError as error:
    print(f"server rejected request: {error.code} ({error.status})")
except vmon.ProtocolError:
    print("endpoint returned an invalid protocol payload")
```

Whether a caller retries a transport failure depends on the operation and its
idempotency. A client retry is not a substitute for server-owned durable
function retry policy.

## Durable function failures

`RemoteFunctionError` reports a failed durable function call. It has `message`,
`code` (default `"remote_error"`), `remote_type`, and `traceback`, which is the
remote traceback text when supplied by the server.

```python
import vmon

try:
    result = some_function.remote("input")
except vmon.RemoteFunctionError as error:
    print(error.code, error.remote_type)
    print(error.traceback)
```

For detached calls, `FunctionCall.get()` raises the same category when the
durable result is failed. It may also raise `TimeoutError` when the caller's
local wait expires. A local timeout does not cancel the recorded call; inspect
the handle, wait again, or explicitly call `cancel()` as appropriate.

`BatchCall.results()` raises a fetched remote failure by default. With
`return_exceptions=True`, it yields that failure as an item so a caller can
continue examining other results. This does not convert the failed input into a
success.

## Actor loss

`ActorLostError` is raised only when a durable actor is terminally `failed` or
`deleted`. It exposes `actor_id` and `status`. Stopped actors remain
recoverable; the SDK never silently recreates an actor or replays its
constructor:

```python
import vmon

with vmon.connect() as client:
    # Counter was defined with @vmon.cls(client=client).
    try:
        counter = Counter.from_id(actor_id)
        counter.increment.remote()
    except vmon.ActorLostError as error:
        audit_actor_loss(error.actor_id, error.status)
```

Decide at the application layer whether a new actor, a checkpoint restore, or
an error to the caller is appropriate. See [Stateful Actors and Apps](actors.md).

## Definition, options, and value errors

Configuration mistakes are rejected locally before an RPC whenever possible:

* `OptionsError` reports invalid or conflicting function options, including an
  untrusted Cloudpickle serializer configuration.
* `PackageError` reports invalid Python source packaging, such as a package root
  that is not a directory, a callable without importable module source, an
  unsupported package mode, or a closure in package mode.
* `ValueError` and `TypeError` report invalid SDK arguments such as an empty
  durable ID, an invalid map window, incompatible schedule timing, or wrong
  call mode.

Value-envelope checks have more specific categories:

* `EnvelopeIntegrityError` means a stored payload checksum, compression payload,
  or envelope integrity check failed.
* `CodecMismatchError` means the declared codec/version does not match what the
  SDK can decode.
* `ABIMismatchError` means a Python-specific payload requires a different
  Python ABI.

JSON-profile validation is represented by `JSONProfileError` in
`vmon.values`; it is a `ValueError`. It rejects values that are not supported
by the SDK's portable JSON profile, including non-finite numeric values.

```python
import vmon

try:
    value = vmon.decode_value(envelope)
except vmon.EnvelopeIntegrityError:
    discard_corrupt_payload()
except vmon.CodecMismatchError:
    request_a_supported_codec()
except vmon.ABIMismatchError:
    request_a_compatible_python_payload()
```

Never respond to an ABI or codec mismatch by enabling trusted deserialization
blindly. Cloudpickle decoding requires `trusted=True` and may execute Python
code; use that gate only for data from a source you explicitly trust. See
[Durable Functions](functions.md#serialization-and-trust).
