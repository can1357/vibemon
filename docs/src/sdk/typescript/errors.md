# Errors

The TypeScript SDK exposes distinct error classes for API responses, transport failures, malformed protocol data, and durable function execution. Catch errors by class when recovery differs; do not infer behavior from an error message.

For the shared error-code vocabulary and server meanings, see [Error Codes](../../reference/errors.md). For endpoint, credential, and DSN behavior, see [Connection Strings and Contexts](../connection-strings.md).

## API errors

`APIError` represents a rejected vmon API request. It extends `Error` and adds:

- `status`: an HTTP-equivalent numeric status;
- `code`: the vmon error code; and
- `message`: the service or response message.

```ts
import { APIError, connect } from "@vmon/sdk";

const client = connect();

try {
  await client.volumes.delete("missing-volume");
} catch (error) {
  if (error instanceof APIError) {
    console.error(error.status, error.code, error.message);
    throw error;
  }
  throw error;
}
```

HTTP response failures are normalized into `APIError`. Failed gRPC calls are also normalized: the SDK reads the `vmon-code` trailer when present and otherwise maps selected gRPC statuses to vmon codes. The corresponding `status` is an HTTP-equivalent status, not evidence that the failing request used HTTP.

## Transport and protocol failures

`TransportError` represents connection, DNS, or transport-timeout failures eligible for mesh-driver failover. `ProtocolError` represents an invalid wire frame or malformed response payload. Both extend `Error`.

```ts
import { APIError, ProtocolError, TransportError } from "@vmon/sdk";

function reportFailure(error: unknown): void {
  if (error instanceof TransportError) {
    console.error("transport failure", error.message);
  } else if (error instanceof ProtocolError) {
    console.error("protocol failure", error.message);
  } else if (error instanceof APIError) {
    console.error("API failure", error.code);
  } else {
    console.error("unexpected failure", error);
  }
}
```


## Function execution failures

A durable call that fails with a service-supplied call error causes `FunctionCall.get()`, result retrieval, or streaming results to throw `FunctionExecutionError`. It includes the remote error's `code`, `remoteType`, `retryable` flag, `frames`, and `details` as `Readonly<Record<string, string>>`; nested remote causes are exposed through the standard `Error.cause` chain.

```ts
import { FunctionExecutionError } from "@vmon/sdk";

try {
  await call.get();
} catch (error) {
  if (error instanceof FunctionExecutionError) {
    console.error(error.code, error.remoteType, error.retryable);
    for (const frame of error.frames) {
      console.error(`${frame.file}:${frame.line} in ${frame.functionName}`);
    }
    throw error;
  }
  throw error;
}
```

`retryable` reports what the daemon included in that failure; this SDK does not document an automatic retry guarantee. Because function calls are durable and daemon-owned, design externally visible effects to tolerate at-least-once execution. See [Durable Functions](functions.md) for cancellation, result retrieval, and call handles.
