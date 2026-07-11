# Glossary

| Term | Meaning |
| --- | --- |
| **actor** | A durable, stateful function target. Calls directed to an actor use its durable lifecycle; terminal actor loss is not silently repaired by replaying its constructor. |
| **availability zone** | A failure-domain label used by the `ZONE` high-availability placement policy. It is distinct from a host. |
| **call** | A durable record of a function invocation, with lifecycle state, ordered events, logs, and execution attempts. |
| **checkpoint** | A non-destructive capture of eligible guest state used by asynchronous replica restore. It does not preserve host-side TCP flows. |
| **context** | A named client configuration containing an ordered mesh gateway roster and bearer-token credentials. A local connection uses the local daemon socket instead. |
| **durability tier** | The per-sandbox choice `off`, `async`, `rerun`, or `async+rerun`. `async` restores from a periodic checkpoint; `rerun` permits at-least-once re-execution from a durable create record; the combined tier prefers restore then re-run. |
| **epoch** | A monotonic ownership generation. Competing owner claims resolve by highest `(epoch, node_id)`. An epoch helps converge after failure; it is not a distributed lock. |
| **fencing** | Stopping or preventing a superseded owner from writing after a higher ownership claim. Mesh epoch fencing is best-effort and bounded by gossip convergence; writable volumes additionally require quorum-fenced leases. |
| **gateway** | The reachable `vmon serve` control-plane endpoint. It accepts client RPCs and proxies operations to the sandbox owner when necessary. |
| **HA policy** | The `ResourceSpec` worker-placement policy: `NONE` makes no cross-host redundancy guarantee; `HOST` spreads workers across physical hosts; `ZONE` spreads workers across availability zones. This is distinct from the sandbox durability tier. |
| **idempotency key** | A client-supplied identity for a repeatable create/restore operation. The mesh uses it in the durable create record and for deterministic coordination. It does not make arbitrary interactive operations safe to replay. |
| **mesh** | A leaderless collection of `vmon serve` nodes that gossip membership and route clients to sandbox owners. It has no consensus store and does not claim consensus-grade guarantees. |
| **owner node** | The one node for a sandbox that runs its VMM process and is the only node allowed to mutate its state. |
| **quorum restore** | Automatic orphan restore only after a strict majority of expected membership confirms the former owner is unreachable. Expected membership is a high-water mark and is not reduced by reaping. |
| **replica** | A checkpoint copy pushed to a rendezvous-selected peer for `async` durability. Replica fan-out defaults to one; it is not synchronous memory replication. |
| **RPO / RTO** | Recovery point objective / recovery time objective. For `async`, RPO is checkpoint cadence; RTO includes failure detection and restore time. Work after the most recent checkpoint can be lost. |
| **sandbox** | A managed microVM instance. It has exactly one owner node at a time, though gateways may proxy requests to that owner. |
| **template** | An immutable saved VM state used to create or restore compatible sandboxes. Reuse is gated by backend and architecture compatibility. |
| **transport failure** | A connection, DNS, I/O, or timeout failure without a reliable server result. It differs from a structured server error and must be retried only when the operation is replay-safe. |
| **vmon-code** | Response metadata on a failed gRPC RPC carrying the stable server error code. See [Error Codes](errors.md). |
| **vmond** | The Rust control-plane daemon used by `vmon serve`; it owns the v1 gRPC surface. It is not a raw TCP VM-control protocol. |
| **worker** | The runtime allocation that executes function work. Its `ResourceSpec` declares CPU, memory, disk, architecture, placement, volume, and network requirements. |
| **writable-volume lease** | A strict-majority, epoch-fenced lease that supplies cluster-wide single-writer protection. It requires at least three nodes; host-local file locking alone is not a mesh mechanism. |

For operational details, see [Mesh and High Availability](../platform/mesh.md), [Storage and Volumes](../platform/storage.md), and [Shared Concepts](../sdk/shared-concepts.md).
