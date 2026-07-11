# Mesh

A Python `Client` represents one logical connection to a vmon mesh. Create it with `connect()` as usual; the driver starts from the DSN endpoints and, unless disabled, discovers mesh-advertised endpoints lazily.

```python
from vmon import connect

with connect("vmon://node-a.example,node-b.example?discover=on") as client:
    status = client.mesh.status()
    print(status.self_node.node_id)
    for peer in status.peers:
        print(peer.node_id, peer.advertise, peer.region)
```

For DSN host lists, discovery parameters, contexts, and authentication, see [Connection Strings and Contexts](../connection-strings.md). `connect(..., discover=False)` keeps discovery disabled; it does not turn a single endpoint into a mesh router.

## Inspecting membership

`client.mesh.status()` returns `MeshStatus`, whose stable fields are:

- `self_node`, a `MeshNode` for the reporting node;
- `peers`, a list of advertised peer `MeshNode` values; and
- `replicas_held`, the reported replica count.

A `MeshNode` has `node_id`, `advertise`, and `region`. `client.mesh.nodes()` returns `[status.self_node, *status.peers]`.

```python
nodes = client.mesh.nodes()
for node in nodes:
    print(f"{node.node_id}: {node.advertise} ({node.region})")
```

At the transport layer, `client.driver.endpoints()` returns a detached roster of `EndpointInfo(url, healthy, source)` values. This is an advanced diagnostic surface, not an instruction to manually select endpoints for ordinary resource calls.

## Routing and resource affinity

The mesh driver chooses a reachable candidate endpoint for client-wide requests and maintains a roster of seed and discovered endpoints. A sandbox returned by `create()`, `get()`, `restore()`, `fork()`, or a list operation is then pinned to the endpoint that returned it. Its `endpoint` property exposes that current base URL, and `node` exposes a server-supplied node id when available.

Resource handles remain owner-pinned: do not treat a `Sandbox` as a portable request that can be freely routed to every mesh member. On a sandbox-scoped call that reports `not_found` (or an HTTP 404 for a port proxy) and when more than one endpoint exists, the SDK resolves which roster endpoint currently hosts that sandbox and re-pins the handle. `sandbox.migrate(target)` asks the server to migrate it, then resolves and re-pins the same handle to its new owner.

```python
sandbox = client.sandboxes.create(image="alpine")
try:
    print("before:", sandbox.endpoint)
    sandbox.migrate("node-b")
    print("after:", sandbox.endpoint)
finally:
    sandbox.terminate()
```

Use a target node identifier accepted by the daemon. Migration is a server operation; the returned `SandboxInfo` is the updated server view.

## Failover boundaries

The driver retries/fails over only **transport failures**: failures that did not successfully reach an HTTP endpoint, or gRPC errors translated to `TransportError`. It does not retry API errors, HTTP status failures, validation failures, or a command that has already started. For streaming gRPC calls, it waits until the server accepts the stream so connection failures can fail over before handing the stream to your code; failures after a stream is established belong to that stream.

This policy avoids claiming an at-least-once or exactly-once execution guarantee. Design mutations with the API's explicit controls, such as `idempotency_key` when creating a sandbox, rather than assuming mesh retry semantics.

`SandboxAPI.list()` has mesh-aware behavior: it queries the live roster endpoints, merges sandbox rows by id, and can filter with `tags=` and `node=`. A successful empty or partial response can still be useful when another endpoint has a transport failure; if every attempted endpoint has a transport failure, it raises the last `TransportError`.

```python
workers = client.sandboxes.list(tags={"role": "worker"}, node="node-b")
for worker in workers:
    print(worker.id, worker.node, worker.endpoint)
```

Close the `Client` when finished (`with connect(...) as client:` is recommended). Closing releases every endpoint transport owned by its driver. For the common lifecycle and error model, see [Shared concepts](../shared-concepts.md) and [Errors](errors.md).
