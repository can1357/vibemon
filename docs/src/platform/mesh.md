# Mesh and High Availability

A mesh is a set of Rust `vmon serve` gateways. Each gateway owns local VMM
processes and can proxy a request to the current sandbox owner. Clients use a
named context containing an ordered gateway roster and bearer token. The gRPC
API is authoritative; HTTP remains for health, metrics, port proxying, and
mesh-internal coordination.

`cluster_mode=single-node` is the default and retains the development
gossip/peer-replica authority model. `cluster_mode=production` makes
PostgreSQL authoritative for ownership and lifecycle and S3 authoritative for
portable object bytes. Production has no elected vmon leader, but it is not a
gossip-only safety model. See [Security](security.md) before putting a gateway
on a network.

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

### Production prerequisites

Every production node must use the same PostgreSQL database, S3 bucket/prefix,
and non-default portable-history encryption key. Configure
`cluster_mode = "production"`, `postgres_url`, the required `s3_*` values, and
`portable_history_key_id`; provision identical key material under each node's
`$VMON_HOME/security/keys/`. The daemon rejects incomplete production
configuration and
never falls back to local authority. See [Configuration](configuration.md) for
the exact fields and [Server Operation](server.md#production-cluster-storage)
for failure behavior.

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

A sandbox has one owner node, identified by a monotonic epoch. That owner alone
may serve or mutate its VMM. The two cluster modes enforce this differently:

- In `production`, PostgreSQL allocates epochs, commits ownership changes, and
  grants a short owner lease. The node renews the lease and arms a local
  watchdog from the database's remaining TTL. Failed or late renewal
  self-fences the VM and local route. A successor activates only after its
  lease and ownership transaction commit.
- In `single-node` development meshes, persisted epochs and gossip converge on
  the highest claim. This is best-effort fencing bounded by gossip convergence,
  not distributed mutual exclusion.

Production migration uses a durable source intent and one migration token. The
target stages and validates recovery, PostgreSQL commits the new owner, and the
source is fenced before the target serves. Retrying either side converges on
the committed owner instead of rolling ownership backward.

For planned work, drain a node before taking it down:

```sh
VMON_API_TOKEN=T vmon mesh leave --drain
```

## Durability tiers

Each sandbox selects `ha=off`, `async`, `rerun`, or `async+rerun`. Mesh
contexts default to `async`; local daemon creates default to `off`.

| Tier | Production mode | Single-node/development mesh | Limit |
| --- | --- | --- | --- |
| Metadata commit | PostgreSQL transactionally stores the create record, owner, epoch, lifecycle state, and lease before acknowledgement. | The create record is synchronously copied to the required live peers before acknowledgement. | This preserves control-plane identity, not uncheckpointed guest memory. |
| `async` | Encrypted checkpoints and manifests are published to S3 and committed in PostgreSQL. | Non-destructive checkpoints are pushed to rendezvous-ranked peers. | RPO is the checkpoint cadence; RTO is detection plus restore time. It is not zero-loss replication. |
| `rerun` | A fenced successor re-executes the durable create record when no usable checkpoint exists. | Same at-least-once rerun contract after epoch convergence. | Work must tolerate re-execution. |
| `async+rerun` | Restores a checkpoint when available, otherwise re-runs. | Same preference using peer replicas. | Retains the limits of both mechanisms. |

The default mesh replication cadence is 60 seconds and fan-out is one.
`VMON_REPLICATE_SEC=0` disables asynchronous replica capture; it does not
remove the durable create record or production ownership lease.

### Durable lifecycle and portable recovery

Pause is node-local and retains the VM. Suspend is durable: the server captures
a checkpoint, commits the suspended marker, then tears down the source.
`Resume` restores that exact marker on the current or newly fenced owner.
Publication or commit failure leaves the source running and no false suspended
marker.

History has `disk` and `checkpoint` tiers. Disk capture quiesces only the block
worker and rollback cold-boots; checkpoint capture preserves VM execution
state. In production, encrypted S3 objects remain invisible until their
PostgreSQL manifest commits. Rollback pins the selected point, stages a
replacement without destroying the current VM, and switches only after the
replacement is ready. Removal uses a tombstone so interrupted object cleanup
can resume without resurrecting ownership.

Inspect actual membership, health, capacity, sandbox tier, checkpoint age, replica count, warnings, and per-node replication/restore/fence counters with:

```sh
VMON_API_TOKEN=T vmon mesh status
```

## Restore safety and writable volumes

At three or more expected members, automatic orphan restore is quorum-gated by
default. The elected successor must obtain strict-majority confirmation that
the former owner is unreachable. The expected-member high-water mark is not
reduced merely because a member is reaped. Restore defers when quorum is short,
the old owner remains reachable, the electee is wrong, or a required replica,
secret, key, or host dependency is missing.

A two-node mesh cannot form a post-failure majority. Quorum restore is off by
default there and the status payload warns about it.
`VMON_RESTORE_QUORUM=0` disables that automatic-restore guard.

Writable volumes use epoch-fenced leases. Production grants them through
PostgreSQL; development meshes use strict-majority peer votes. Holders renew
before expiry and self-fence writers on missed renewal; no successor can
activate until the previous lease is no longer valid. Writable volumes require
at least three expected members and are rejected on smaller mesh contexts.
Read-only volumes are unrestricted. A local daemon's host `flock` protects only
same-host concurrency.

## Kubernetes mesh

The Helm chart deploys `vmon serve` as a StatefulSet on KVM-labelled hosts.
Every pod is both an API endpoint and a VM owner; the public Service can send a
request to any healthy pod. Pod zero initializes membership and other stable
pod identities join before readiness. PostgreSQL stores ownership, fencing,
lifecycle, and portable manifests. S3 stores encrypted portable history,
replicas, and function artifacts. Gossip carries liveness and capacity but is
not production ownership authority. One host-networked pod runs per Kubernetes
node so API ports and TAP devices do not collide.
