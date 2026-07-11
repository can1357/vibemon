# TypeScript SDK

`@vmon/sdk` is an ESM client for a running `vmon serve` daemon. It is not a
VMM, daemon, or local sandbox runtime: it sends HTTP requests and RPCs to a
reachable server. Use it from a Bun program or a browser application when the
application can reach the daemon's network endpoint.

The root package exports the connection and resource API:

```ts
import { connect } from "@vmon/sdk";

const client = connect("https://vmon.example");
const health = await client.health();
console.log(health.ok);
await client.close();
```

The package is ESM-only (`"type": "module"`). Its public package entry points
are:

- `@vmon/sdk` for `connect`, `Client`, `MeshDriver`, resource classes, models,
  driver and DSN types, and errors.
- `@vmon/sdk/functions` for the durable-function API.
- `@vmon/sdk/function-values` for function value encoding and decoding.

Start with [Install](install.md), then [Connect](connect.md). The root object
and its service namespaces are described in [Client](client.md); resource
operations are documented in the pages that follow it.

## Transport boundary

The SDK uses ordinary HTTP(S) for health, metrics, and HTTP-backed resource
operations, and the daemon's `/grpc` WebSocket bridge for RPCs. Network DSNs
therefore work in Bun and browsers, subject to the deployment's TLS, CORS, and
WebSocket policy. The TypeScript SDK does **not** provide native HTTP/2 gRPC or
Unix-domain-socket transport; `vmon+unix://` is unsupported.

For the accepted DSN forms, credentials, discovery, and endpoint-affinity
rules, see [Connection Strings and Contexts](../connection-strings.md) and
[Shared Concepts](../shared-concepts.md). For server setup and exposure, see
[Server Operation](../../platform/server.md).
