# Mesh and High Availability

A mesh is a set of `vmon serve` gateways. Each gateway is the Rust `vmond` server; it owns its local sandbox registry and starts local VMM processes. Clients use a named context: an ordered roster of gateway URLs and a bearer token. A gateway accepts a request and proxies it to the sandbox owner when needed. The gRPC API is the authoritative control plane; HTTP remains for health, metrics, and port proxying.

This is a leaderless gossip mesh, not a consensus system. The safety properties in this page are intentionally tiered. See [Security](security.md) before putting a gateway on a network.

## Bootstrap a reachable cluster

All nodes need the same full operator token (`--token` or `VMON_API_TOKEN`). Start the server on every node with that token, then initialize the first node and join the others with the blob printed by setup:

```sh
# seed node: vmon serve is already running with token T
VMON_API_TOKEN=T vmon mesh setup --advertise http://<seed-ip>:8000

# each additional node: start vmon serve with the same token first
VMON_API_TOKEN=T vmon mesh join <blob>
```

`--advertise` is not cosmetic. The advertised URL must be reachable from every other member **and** every client that will use the context. There is no built-in NAT traversal, relay, or overlay. For hosts behind NAT, put the nodes and clients on a reachable network such as WireGuard or Tailscale, then advertise the overlay address:

```sh
VMON_API_TOKEN=T vmon mesh setup --advertise http://<overlay-ip>:8000
```

Use HTTPS when TLS is configured, and advertise the exact HTTPS URL. Inter-node exec proxying then uses WSS:

```sh
vmon serve --tls-cert /path/to/cert.pem --tls-key /path/to/key.pem
VMON_API_TOKEN=T vmon mesh setup --advertise https://<node-name>:8000
```

## Contexts and client failover

Create a context from any reachable gateway. The CLI fetches and saves the gateway roster. It does not save a token unless `--save-token` is supplied; saved credentials are written under `$VMON_HOME/credentials/`.

```sh
export VMON_API_TOKEN=T
vmon context add prod --server https://<any-node>:8000 --save-token
vmon context use prod
```

`vmon context use local` restores local-daemon routing. An explicitly named missing context is an error; it does not fall back to local operation.

Failover has a delivery boundary:

- Reads and replay-safe writes can walk the saved roster only when connection establishment fails. Detached create or restore must carry an idempotency key to be replay-safe.
- Attached or interactive `run`, `exec`, shell, snapshot, fork, and extend probe `/healthz` to choose one live gateway, then run exactly once. A request that may have been delivered is not duplicated.
- A context can tolerate a gateway loss only while one saved endpoint remains reachable. There is no client-side answer when every saved gateway is down.

## Placement

Placement is request-scoped, never determined by the client or ingress gateway architecture. `arch` is optional on create, run, restore, and fork. When omitted, the coordinator derives compatible architectures from the image manifest and live node capabilities; a Mac client may therefore target an x86 Linux pool without an explicit client-side switch.

```sh
vmon run --arch aarch64 alpine -- uname -m
vmon restore tpl --arch x86_64 --name restored
vmon fork tpl --arch aarch64 --count 2
```

Architecture and agent/kernel capability are hard eligibility filters. Snapshot/template restore, fork, migration, and replica restore also require compatible backend and CPU-baseline state. Scores favor warm pools and templates, free capacity, locality and region, and penalize in-flight work, but scoring never overrides an eligibility gate. A mixed-architecture mesh whose image architecture cannot be derived returns `arch_required`; no eligible live pool returns `unplaceable`.

Mesh creates reject host-local `fs_dir` shares rather than placing them with weaker semantics. Use a named volume instead.

## Ownership and fencing

A sandbox has one owner node. That node runs the VMM and is the only node that mutates sandbox state. Ownership is claimed with a monotonic epoch; competing claims resolve by highest `(epoch, node_id)`. Nodes persist local epochs and stop a local sandbox that is superseded by a higher authoritative epoch.

Epoch fencing is **best effort**, not distributed mutual exclusion. Gossip convergence bounds a split brain to the network-partition window and converges after rejoin, but it does not provide a consensus-grade single-writer guarantee. Do not rely on epochs alone for writable shared state.

For planned work, drain a node before taking it down:

```sh
VMON_API_TOKEN=T vmon mesh leave --drain
```

## Durability tiers

Each sandbox selects `ha=off`, `async`, `rerun`, or `async+rerun`. Mesh contexts default to `async`; local daemon creates default to `off`.

| Tier | What it provides | Limit |
| --- | --- | --- |
| Metadata commit | The create record—sandbox ID, spec, owner, epoch, and idempotency key—is replicated before a create acknowledgement. | With three or more expected members, it needs a strict majority. In a two-node mesh every live peer must acknowledge, but a lone survivor can accept locally; that is weaker than majority quorum. |
| `async` | Periodic non-destructive checkpoints go to rendezvous-ranked peers. `mesh status` shows checkpoint age. | RPO is the checkpoint cadence; work since the last checkpoint is lost on owner crash. RTO is failure detection plus restore time. It is not zero-loss replication. |
| `rerun` | A survivor re-executes the durable create record at a higher epoch when there is no usable checkpoint. | It is at-least-once compute. Work must tolerate re-execution. |
| `async+rerun` | Restores a checkpoint when available, otherwise re-runs. | Retains the limits of both mechanisms. |

The default mesh replication cadence is auto-derived as 60 seconds and the default fan-out is one replica. `VMON_REPLICATE_SEC=0` disables checkpoint replication; this changes the available guest-state protection, not the durable create record behavior.

Inspect actual membership, health, capacity, sandbox tier, checkpoint age, replica count, warnings, and per-node replication/restore/fence counters with:

```sh
VMON_API_TOKEN=T vmon mesh status
```

## Quorum restore and writable volumes

At three or more expected members, automatic orphan restore is quorum-gated by default. The elected survivor must obtain strict-majority confirmation of the former owner being unreachable. The expected-member high-water mark is not reduced merely because a member is reaped. A restore is deferred and retried when quorum is short, the old owner remains reachable, the elected node is wrong, a replica or required secret is missing, or it otherwise cannot complete safely.

A two-node mesh cannot form a post-failure majority. Quorum restore is off by default there and the status payload warns about it. `VMON_RESTORE_QUORUM=0` forces quorum restore off; doing so removes that automatic-restore guard.

Writable mesh volumes use a different mechanism: quorum-granted, epoch-fenced leases. A holder renews by half the TTL and self-fences writers if renewal misses that deadline. No successor can receive a vote until the full TTL from the prior grant has elapsed. This is the single-writer mechanism for writable volume data.

Writable volumes require at least three nodes in a mesh context and are rejected at create on smaller meshes; they never silently degrade to a host lock. Read-only volumes are unrestricted. On a local daemon, the host-local `flock` protects only same-host concurrency.
