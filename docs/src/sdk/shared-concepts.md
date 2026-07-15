# Shared Concepts

This page describes behavior shared by the Python, Go, and TypeScript clients.
The API-facing resource model is common even where method names, synchronous
or asynchronous style, and type names differ. Begin with [SDK Overview](overview.md)
and use [Connection Strings and Contexts](connection-strings.md) to select the
daemon.

## Resource handles and namespaces

The root client is a remote-control handle for one `vmon serve` deployment or
mesh roster. Its namespaces organize daemon resources rather than duplicating
them locally:

| Namespace | Main operations | Returned handle behavior |
| --- | --- | --- |
| Sandboxes | Create, list, get, stop, remove, snapshot | A sandbox is bound to its client and can expose process, file, and port handles |
| Snapshots | List, restore, fork | Restore and fork return sandbox handles |
| Volumes | List, create, delete | A volume names daemon-managed persistent storage |
| Pools | List, set, delete, clear | A pool represents daemon-managed warm capacity |
| Mesh | Read status and nodes | Mesh data describes daemon nodes and advertised endpoints |

Sandbox process, file, and port objects are scoped to a sandbox ID. A process
executes through the guest agent; files name paths in the guest filesystem;
ports use the daemon's guest port proxy. They are not host process, host-file,
or direct guest-network handles. The [Networking](../platform/networking.md)
and [Storage and Volumes](../platform/storage.md) pages describe the platform
semantics that those handles operate on.

Create, get, restore, fork, and list operations return handles identified by a
stable resource ID. The SDK privately remembers which mesh endpoint served each
handle so follow-on operations avoid a cluster-wide broadcast; applications do
not select or retain worker-node addresses.

An ID-only sandbox reference starts without a route. On its first operation,
or after ownership moves, the SDK resolves the current owner and retries the
sandbox-scoped request once. Callers continue using the same stable ID.

## Durable calls

A durable call is a server-side function invocation. The Call service records a
call before it becomes eligible to execute, and the server assigns its stable
call ID. Keep that ID when work must outlive a client process: each SDK has its
own call-handle and reconstruction API, but the ID identifies the same durable
call record.

The daemon owns the execution-attempt and retry lifecycle. A retry policy has
at-least-once semantics, not exactly-once semantics: after a worker or
infrastructure failure, an attempt can run again and an external side effect
can therefore happen more than once. Make side effects idempotent and use a
durable request or business key for deduplication. Reconnecting a client or
waiting again for a result does not create a client-owned retry policy.

Results and events are resumable by durable sequence, rather than by a
connection-local position:

* Each committed result or generator yield has a strictly increasing result
  sequence. The protocol can return bounded result pages after the last
  consumed sequence.
* Each call event has a strictly increasing per-call sequence. An event watch
  starts after a supplied sequence and can replay committed history before
  following new events.

Persist the last fully processed sequence before resuming. A cursor excludes
that sequence and earlier ones, so it is suitable for reconnecting consumers
without treating a newly opened stream as new work. The concrete APIs differ:
Python exposes `FunctionCall.from_id()` and sequence arguments on `events()`;
Go exposes `FunctionCallFromID` and `Watch`; TypeScript exposes
`FunctionCall.fromId()` and `events()`. Those names do not imply shared
decorators, source packaging, serializers, or actor APIs.

The durable call record is the operational observation boundary: it reports
the latest lifecycle status, committed input and result counts, timestamps,
optional graph and labels, terminal error, and accumulated execution
statistics. Its event log records lifecycle transitions, worker logs, result
and yield commits, attempt transitions, errors, input closure, and cancellation
requests. A function's result TTL controls durable **result** retention after
terminal completion, and a call can override that retention when created. It
does not promise permanent result storage or define a general archival policy
for every call event or log.

For language-specific function definitions, deployment, value encoding, and
worker behavior, use the corresponding SDK pages rather than assuming a
durable-call handle makes those features portable.

## RPCs, HTTP exceptions, and errors

Resource operations are `vmon.v1` gRPC methods. A failed RPC has a gRPC
status, and daemon error metadata may include a stable `vmon-code`. Important
mappings include:

| Daemon error code | gRPC status | Typical meaning |
| --- | --- | --- |
| `not_found` | `NOT_FOUND` | The resource does not exist |
| `invalid` | `INVALID_ARGUMENT` | Request data is malformed or invalid |
| `unauthorized` | `UNAUTHENTICATED` | Authentication is missing or rejected |
| `not_running` | `FAILED_PRECONDITION` | A sandbox or guest agent is not in the required state |
| `busy` | `ABORTED` | A required resource is locked |
| `unsupported` | `UNIMPLEMENTED` | The daemon cannot provide the operation |
| `engine` | `UNAVAILABLE` | An engine or allocation failure occurred |

Malformed protocol envelopes are a client-side protocol error. Connection,
DNS, and transport-timeout failures are transport errors and are distinct from
daemon statuses. Handle these categories separately: a structured daemon
error means the request reached a daemon, while a transport error can mean
that it did not.

Health, Prometheus metrics, and sandbox port proxying are the intentional HTTP
exceptions. The root health operation uses `/healthz`; metrics use `/metrics`;
port handles use the guest port proxy. HTTP failures from these surfaces are
HTTP/API errors, not gRPC status responses. Do not derive a general resource
API from those paths.

## Lazy discovery and restricted failover

The DSN endpoints are the initial **seed roster**. With discovery enabled,
the driver does not contact a mesh while constructing the client. After a
successful request it can obtain advertised peers from mesh status and merge
them into its roster. Discovery is refreshed lazily and periodically; disabling
it with `discover=off` keeps routing to the configured endpoints.

Endpoint selection follows a sticky preference. A successful response makes
that endpoint preferred, while an endpoint that has a transport failure is
temporarily cooled down. Failover is intentionally restricted:

* The driver tries another roster endpoint only for a connection-level failure
  that did not reach a daemon.
* A gRPC status and an HTTP response—successful or failing—are results from a
  daemon and are not replayed on another endpoint.
* Streaming calls are not safely replayed once started. A caller must decide
  whether reconnecting, resuming, or repeating work is safe for its workload.

This policy is conservative because create, execute, file-write, lifecycle,
and other state-changing calls may have side effects. A timeout or broken
connection does not prove that a daemon did not receive the request. Supply
an application-level idempotency or reconciliation strategy before manually
retrying a mutation.

## Timeouts, cancellation, and streams

The connection timeout comes from the DSN or language-specific connection
options and defaults to 60 seconds. It bounds client communication; it is not
a request to change a sandbox's lifetime or the daemon's VMM timeout. Pass a
separate sandbox timeout when creating or managing workload lifetime.

Logs, events, console attachment, and interactive execution are streams.
Interactive execution begins with required setup frames, and stream lifecycle
is observable: consume it until completion, cancel it through the language
API when no longer needed, and treat a disconnect as an uncertain operation
boundary. In particular, do not automatically resend interactive input or a
mutation after a lost stream without knowing whether the daemon processed it.

## Security boundary

The token authenticates requests to the daemon; it does not make a client a
local VMM controller. Use TLS (`vmons` or `https`) for remote endpoints,
limit access to Unix sockets, and do not expose host control sockets, host
shares, or port-forwarding surfaces across trust boundaries. See
[Security](../platform/security.md) for platform controls and
[Connection Strings and Contexts](connection-strings.md) for credential and
context selection.
