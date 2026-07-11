# Durable Functions

The TypeScript SDK invokes functions already deployed to the `vmon serve` API. It does not package or deploy TypeScript source, and it does not execute a function locally. Durable calls are daemon-owned: treat execution as **at least once** and make function side effects idempotent.

## Look up a deployed function

`client.functions.fromName(name, options?)` resolves a function and returns a `RemoteFunction`. With no `revisionId`, it resolves the active revision; with one, it pins that revision. The returned handle always retains the resolved immutable revision in `remote.revision`.

```ts
import { connect } from "@vmon/sdk";

const client = connect();
const double = await client.functions.fromName("double", {
  namespace: "production",
  revisionId: "revision-42",
});

console.log(double.revision.revisionId);
```

The default namespace is `default`. Lookup options also establish per-call defaults: `serializer`, `compression`, `inlineLimit`, `typeName`, `labels`, `resultTtlMillis`, and `signal`. `withOptions()` returns another `RemoteFunction` for the same pinned revision with merged defaults; labels are merged.

### Typed application values

By default, inputs and results are `PortableValue`. To use a domain type, pass **both** `input` and `result` `FunctionValueAdapter`s to `fromName`. An adapter converts to and from `PortableValue`; supplying only one is invalid.

```ts
import { connect, type FunctionValueAdapter } from "@vmon/sdk";

interface Counter {
  value: number;
}

const counterAdapter: FunctionValueAdapter<Counter> = {
  toPortable(counter) {
    return { value: counter.value };
  },
  fromPortable(value) {
    if (
      typeof value !== "object" ||
      value === null ||
      value instanceof Uint8Array ||
      Array.isArray(value)
    ) {
      throw new TypeError("expected counter object");
    }
    const field = value.value;
    if (typeof field !== "number") throw new TypeError("expected numeric counter value");
    return { value: field };
  },
};

const increment = await connect().functions.fromName("increment", {
  input: counterAdapter,
  result: counterAdapter,
});
const result = await increment.remote({ value: 1 });
```

## Invoke and inspect calls

`RemoteFunction.remote(input)` creates a durable unary call and waits for its result. Use `spawn(input)` when the call id and lifecycle handle are needed first.

```ts
const call = await double.spawn(21, { labels: { request: "example" } });
console.log(call.id);

const record = await call.status();
const statistics = await call.stats();
const relations = await call.graph();
const answer = await call.get();
```

A `FunctionCall` can be reconstructed elsewhere with `FunctionCall.fromId(client, id)`. It exposes:

- `status()` for the durable call record, `stats()` for accumulated statistics, and `graph()` for call relationships;
- `events({ afterSequence, follow })` to read reconnectable call events after a sequence number;
- `logs({ afterSequence, follow })` for decoded `stdout`, `stderr`, and structured worker logs;
- `result(index)`, `resultEntry(index)`, and `results(afterSequence?, pageSize?)` for indexed results; and
- `cancel(reason?)` to request daemon-side cancellation.

`get({ signal })` waits for the unary result. If its `AbortSignal` is already aborted or fires while waiting, the SDK requests cancellation with the reason `aborted by AbortSignal`; an abort is not merely a local wait timeout. A `signal` in call options is also bound to a newly spawned call. Cancellation remains a daemon operation, so callers should inspect the durable status when coordination matters.

Use `spawnArguments` / `remoteArguments` when the target expects explicit positional and named arguments:

```ts
const total = await double.remoteArguments({
  positional: [20],
  named: { adjustment: 1 },
});
```

## Batch calls

`spawnMap(inputs)` creates a streamed durable batch and returns `BatchCall`; `map(inputs)` yields decoded results in input order. Inputs may be either `Iterable<Input>` or `AsyncIterable<Input>`, and encoding/submission is lazy.

```ts
const batch = await double.spawnMap([1, 2, 3]);

for await (const value of batch) {
  console.log(value);
}
```

`BatchCall` extends `FunctionCall`. Its `result(index)` and `resultEntry(index)` wait for input submission before retrieving a result. Calling `cancel()` first stops local input production, then requests durable cancellation. Errors during input submission cause the SDK to request cancellation before preserving the submission error.

## Applications and bindings

`client.apps.fromName(name, { namespace?, revisionId? })` resolves an application revision. As with functions, omitting `revisionId` selects the current app revision and supplying it pins one. `app.function(bindingName, options?)` returns a portable `RemoteFunction` for a named binding; it throws if the resolved app has no such binding.

```ts
const app = await client.apps.fromName("billing", { namespace: "production" });
const charge = app.function("charge", { serializer: "cbor" });
const receipt = await charge.remote({ invoice: "inv-42" });
```

## Portable values, JSON, CBOR, and artifacts

Function values use the portable JSON/CBOR model:

- `PortableValue` supports `null`, booleans, numbers, `bigint`, strings, `Uint8Array`, arrays, and string-keyed objects recursively.
- JSON is the default serializer and is restricted to JSON-compatible values. CBOR supports `Uint8Array` and `bigint` values only in the accepted 64-bit signed/unsigned integer range, from `-2^64` through `2^64 - 1`.
- `serializer` is `"json"` or `"cbor"`; `compression` is `"none"` or `"gzip"`.
- Values up to `inlineLimit` (default 256 KiB) are inline. Larger values are uploaded as immutable artifacts by the SDK's client-backed artifact store.
- Envelopes carry an uncompressed SHA-256 checksum. Decoding verifies artifact integrity, decompression, size, and checksum before decoding.

The standalone `encodeValue` and `decodeValue` APIs accept an explicit `ValueArtifactStore` when directly handling artifact-backed envelopes. Remote-function handles arrange the store automatically.

```ts
import { decodeValue, encodeValue } from "@vmon/sdk";

const envelope = await encodeValue(
  { bytes: new Uint8Array([0, 1, 255]), wide: 2n ** 53n },
  { serializer: "cbor" },
);
const value = await decodeValue(envelope);
```

The standalone `encodeValue` and `decodeValue` APIs accept a `ValueArtifactStore` when directly handling artifact-backed envelopes. Its `put(data, mediaType)` returns an immutable artifact reference and its `get(ref)` loads the referenced bytes. Remote-function handles arrange that client-backed store automatically. TypeScript rejects the Python-only Cloudpickle serializer for both encoding and decoding.

## Failures

A remote function failure becomes `FunctionExecutionError`, with `code`, `remoteType`, `retryable`, decoded stack `frames`, `details` as `Readonly<Record<string, string>>`, and a nested `cause` when supplied by the service. Handle it separately from API and transport failures as described in [Errors](errors.md). For connection strings and authentication, use [Connection Strings and Contexts](../connection-strings.md).
