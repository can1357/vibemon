# Glossary

| Term | Meaning |
| --- | --- |
| **actor** | A durable, stateful function target. Calls directed to an actor use its durable lifecycle; terminal actor loss is not silently repaired by replaying its constructor. |
| **availability zone** | A failure-domain label used by the `ZONE` high-availability placement policy. It is distinct from a host. |
| **call** | A durable record of a function invocation, with lifecycle state, ordered events, logs, and execution attempts. |
| **checkpoint** | A VM execution-state capture used by `async` recovery, durable suspend, and `checkpoint` history points. It preserves guest processes but not host-side TCP flows. |
| **context** | A named client configuration containing an ordered mesh gateway roster and bearer-token credentials. A local connection uses the local daemon socket instead. |
| **durability tier** | The per-sandbox choice `off`, `async`, `rerun`, or `async+rerun`. `async` restores from a periodic checkpoint; `rerun` permits at-least-once re-execution from a durable create record; the combined tier prefers restore then re-run. |
| **epoch** | A monotonic ownership generation. PostgreSQL allocates and fences production epochs; development meshes converge persisted claims through gossip. |
| **fencing** | Preventing a stale owner from serving or writing. Production uses PostgreSQL owner/volume leases plus local expiry watchdogs; development epoch fencing is best-effort and bounded by gossip convergence. |
| **gateway** | The reachable `vmon serve` control-plane endpoint. It accepts client RPCs and proxies operations to the sandbox owner when necessary. |
| **HA policy** | The `ResourceSpec` worker-placement policy: `NONE` makes no cross-host redundancy guarantee; `HOST` spreads workers across physical hosts; `ZONE` spreads workers across availability zones. This is distinct from the sandbox durability tier. |
| **idempotency key** | A client-supplied identity for a repeatable create/restore operation. The mesh uses it in the durable create record and for deterministic coordination. It does not make arbitrary interactive operations safe to replay. |
| **lifecycle generation** | The monotonic `state_generation` attached to an accepted desired-state transition. Stale completers cannot overwrite a newer generation. |
| **mesh** | A collection of `vmon serve` gateways that gossip liveness/capacity and route clients to sandbox owners. Production delegates ownership and lifecycle authority to PostgreSQL; development mode has only its stated gossip guarantees. |
| **owner node** | The one node for a sandbox that runs its VMM process and is the only node allowed to mutate its state. |
| **quorum restore** | Automatic orphan restore only after a strict majority of expected membership confirms the former owner is unreachable. Expected membership is a high-water mark and is not reduced by reaping. |
| **recovery point** | An immutable retained `disk` or `checkpoint` capture. Disk points cold-boot; checkpoint points restore VM execution state. |
| **replica** | A periodic checkpoint used by `async` durability. Production publishes encrypted replicas to S3; development meshes push them to rendezvous-selected peers. |
| **RPO / RTO** | Recovery point objective / recovery time objective. For `async`, RPO is checkpoint cadence; RTO includes failure detection and restore time. Work after the most recent checkpoint can be lost. |
| **sandbox** | A managed microVM instance. It has exactly one owner node at a time, though gateways may proxy requests to that owner. |
| **suspend** | A durable lifecycle transition that commits a checkpoint and suspended marker before releasing the live VM. `Resume` restores the exact marker while preserving the sandbox ID. |
| **template** | An immutable saved VM state used to create or restore compatible sandboxes. Reuse is gated by backend and architecture compatibility. |
| **transport failure** | A connection, DNS, I/O, or timeout failure without a reliable server result. It differs from a structured server error and must be retried only when the operation is replay-safe. |
| **vmon-code** | Response metadata on a failed gRPC RPC carrying the stable server error code. See [Error Codes](errors.md). |
| **vmond** | The Rust control-plane daemon used by `vmon serve`; it owns the v1 gRPC surface. It is not a raw TCP VM-control protocol. |
| **worker** | The runtime allocation that executes function work. Its `ResourceSpec` declares CPU, memory, disk, architecture, placement, volume, and network requirements. |
| **writable-volume lease** | An epoch-fenced lease that supplies cluster-wide single-writer protection. PostgreSQL grants it in production; development meshes require strict-majority peer votes. Writable mesh volumes require at least three expected members. |

For operational details, see [Mesh and High Availability](../platform/mesh.md), [Storage and Volumes](../platform/storage.md), and [Shared Concepts](../sdk/shared-concepts.md).
