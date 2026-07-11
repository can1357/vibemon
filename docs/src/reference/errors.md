# Error Codes

The public control-plane API is gRPC. An RPC failure has a gRPC status and, for
server-classified failures, a `vmon-code` response-metadata value. Clients
should branch on `vmon-code`, not on the human-readable status message or an
HTTP-like status value exposed by an SDK.

| Stable server code | gRPC status | Meaning | Typical caller action |
| --- | --- | --- | --- |
| `not_found` | `NOT_FOUND` | The named resource does not exist. | Do not retry unchanged; refresh the resource reference. |
| `invalid` | `INVALID_ARGUMENT` | A request or its configuration is invalid. | Correct the request. |
| `unauthorized` | `UNAUTHENTICATED` | Authentication was not accepted. | Supply or refresh credentials. |
| `not_running` | `FAILED_PRECONDITION` | The requested operation requires a running or suitable sandbox state. | Change state, then retry if appropriate. |
| `busy` | `ABORTED` | A required resource is locked or contended. | Retry only when the operation is safe to repeat. |
| `unsupported` | `UNIMPLEMENTED` | The server or deployment cannot perform the request. | Choose a supported configuration or capability. |
| `engine` | `UNAVAILABLE` | An unclassified engine or allocation failure occurred. | Treat as potentially transient; inspect the message and retry only when safe. |

`EngineError` is the server implementation type, not an SDK contract. Its
legacy JSON-envelope spelling for its final variant is `engine_error`; the v1
gRPC contract and SDK fallback mapping use `engine`. Do not use
`engine_error` as a substitute for the `vmon-code` value on a gRPC RPC.

The SDKs expose a structured server-error category (called `APIError` in the
current Python, Go, and TypeScript SDKs) carrying the server code and message.
They also provide an HTTP-equivalent status for compatibility: `400` invalid,
`401` unauthorized, `404` not found, `409` not running/busy, `501`
unsupported, and `503` engine. That number is not an additional control-plane
transport or an API status contract.

## Failure categories

Use the category before deciding whether to retry:

| Failure | What it means | Retry rule |
| --- | --- | --- |
| Structured server error | The RPC reached vmon and produced a gRPC failure, usually with `vmon-code`. | Apply the code-specific rule above. |
| Transport failure | Connection, DNS, I/O, or transport-timeout failure before a reliable server result. | Only replay operations known to be safe. A mesh context may fail over on connection-establishment failure. |
| Protocol failure | The client could not decode a wire frame or response payload. | Do not blindly replay; report the endpoint and client/server versions. |
| Function-execution failure | User function code ran and failed; its durable call result carries failure details. | Handle as application failure, not as a failed control-plane request. |

The Python SDK currently names these categories `APIError`, `TransportError`,
`ProtocolError`, and `RemoteFunctionError`. Go and TypeScript expose structured
API and protocol/transport errors according to their own language conventions;
callers must not depend on identical type names or fields across languages.
For function behavior, see the language-specific durable-function guides rather
than treating a user exception as an RPC protocol error.

For retry and gateway behavior, see [Connection Strings and Contexts](../sdk/connection-strings.md) and [Mesh and High Availability](../platform/mesh.md).
