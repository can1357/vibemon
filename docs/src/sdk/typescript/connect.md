# Connect from TypeScript

Create a root client with `connect`. Construction parses the DSN and creates a
`MeshDriver`; it does not contact the daemon until an operation is made.

```ts
import { connect } from "@vmon/sdk";

const client = connect("vmons://vmon-a.example,vmon-b.example?timeout=20");
try {
  const info = await client.info();
  console.log(info.version);
} finally {
  await client.close();
}
```

`connect(dsn?, options?)` accepts the same explicit overrides as the driver:

```ts
const client = connect("https://vmon.example", {
  token: process.env.VMON_API_TOKEN,
  timeout: 30,
  discover: false,
});
```

`timeout` is in seconds. `token`, `timeout`, and `discover` override values
parsed from the DSN or environment. `fetch`, `websocket`, and `transport` are
also driver injection points when an application needs to supply compatible
transport implementations; ordinary Bun and browser applications normally use
their globals.

## Defaults by runtime

An omitted or blank DSN is resolved in this order:

1. `VMON_DSN`.
2. `VMON_CONTEXT`, as `vmon+context://<name>`.
3. In a browser with an HTTP(S) page, the page origin—for example,
   `https://console.example/sandboxes` selects `https://console.example`.
4. Outside a browser, including Bun, `http://127.0.0.1:8000`.

This makes a browser app naturally address the server that served its page,
while a Bun program defaults to local TCP. It does not mean either environment
starts a local daemon.

A named context requires filesystem access: the SDK reads `contexts.json` and
an optional credential from `VMON_HOME` (or the user's home directory). A
Bun runtime can use that form; a browser cannot resolve it because it has no
local filesystem access. Use a network DSN in browser code.

## Supported transports

Use `vmon://`, `vmons://`, `http://`, or `https://` DSNs in TypeScript.
`vmon`/`vmons` expand to HTTP(S) endpoints and RPCs use the daemon's WebSocket
bridge. The SDK has no Unix-domain-socket transport, so
`vmon+unix:///path/to/vmond.sock` is rejected even in Bun.

The browser must be allowed to make the required HTTP and WebSocket connections
to the selected origin. In particular, a page and a different daemon origin
need the server's appropriate browser-origin policy; the SDK does not proxy
that connection through the page origin.

For the exact DSN grammar, query parameters, context file layout, token
precedence, and validation rules, see [Connection Strings and
Contexts](../connection-strings.md). For mesh discovery, affinity, and
transport-failure behavior after connecting, see [Shared
Concepts](../shared-concepts.md).

## Constructing a client with a driver

`connect` is the normal factory. When an application owns driver construction,
pass a `MeshDriver` (or another implementation of the exported `Driver`
interface) directly to `Client`:

```ts
import { Client, MeshDriver } from "@vmon/sdk";

const driver = new MeshDriver("https://vmon.example", { discover: false });
const client = new Client(driver);
try {
  await client.health();
} finally {
  await client.close();
}
```

This is useful when the application needs driver-level configuration. Resource
APIs still use the same root client; see [Client](client.md).
