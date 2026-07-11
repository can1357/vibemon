# Testing

The TypeScript SDK uses Bun's test runner and TypeScript's compiler for its focused development checks. Run commands from `sdk/ts`.

```sh
bun test
bun run typecheck
```

`bun test` runs the Bun test suite. `bun run typecheck` runs `tsc -p tsconfig.json --noEmit`; it checks types without emitting JavaScript. The package is ESM (`"type": "module"`), so write test imports and module usage accordingly.

## Unit tests

The function tests use `bun:test` and an in-process Connect router transport. They exercise deployed-function lookup, pinned revisions, unary calls, structured arguments, batch streaming, cancellation, durable call reconstruction, remote error decoding, JSON/CBOR values, and artifact-backed values without requiring a running daemon.

Run the complete SDK test suite with `bun test`, or target a test file during development:

```sh
bun test test/functions.test.ts
bun test test/function-values.test.ts
```

Keep tests on the public SDK boundary where possible: create a `Client` backed by a test transport, look up a deployed function, and assert observable call/result behavior. Test custom `FunctionValueAdapter` conversion explicitly, including rejected values, rather than using type assertions to hide a missing conversion.

## API smoke test

`test/smoke.test.ts` is gated. It is skipped unless `VMON_TS_SMOKE=1` is set.

```sh
VMON_TS_SMOKE=1 bun test test/smoke.test.ts
```

When that flag is set, the test chooses one of these source-defined paths:

1. If both `VMON_SERVER_URL` and `VMON_API_TOKEN` are non-empty, it connects to that server.
2. Otherwise, if `target/debug/vmon` exists relative to the repository root, it starts that binary as `vmon serve` on `127.0.0.1` with a temporary home, a free TCP port, and a generated token.
3. Otherwise, it skips the smoke test.

The smoke exercise checks health, creates/lists/deletes a volume, and lists sandboxes. It does not prove deployed-function invocation.

## Remote function smoke test

The portable remote-function smoke is independently gated by `VMON_TS_REMOTE_SMOKE=1`:

```sh
VMON_TS_REMOTE_SMOKE=1 bun test test/smoke.test.ts
```

It requires all of the following environment variables:

- `VMON_SERVER_URL`
- `VMON_API_TOKEN`
- `VMON_REMOTE_NAMESPACE`
- `VMON_REMOTE_JSON_NAME`
- `VMON_REMOTE_JSON_REVISION`
- `VMON_REMOTE_CBOR_NAME`
- `VMON_REMOTE_CBOR_REVISION`

That test connects with discovery disabled, resolves the JSON and CBOR functions, checks their expected revisions, invokes portable values, and reconstructs a detached durable call with `FunctionCall.fromId`. The CBOR case uses `Uint8Array` and a `bigint`, which JSON portable values cannot represent.

## Choosing a check

Use `bun run typecheck` after changing TypeScript types or public examples. Use the function-value tests when changing portable serialization or artifact handling. Enable the API smoke only when a server endpoint/token or the expected local debug binary is available; enable the remote smoke only when the named deployed functions and every required variable are provisioned.

For client configuration rather than test environment variables, see [Connection Strings and Contexts](../connection-strings.md). For call semantics and portable values, see [Durable Functions](functions.md).
