# Files and Ports

`Sandbox.files` and `Sandbox.ports` are remote guest handles bound to a
sandbox ID. They do not expose the JavaScript host filesystem or raw guest
network sockets. Start with [Sandboxes](sandboxes.md) to obtain a handle.

## Guest files

`files.read(path)` returns the complete guest file as `Promise<Uint8Array>`;
`readText(path)` decodes that result as UTF-8. Write raw content with
`write(path, content)` or UTF-8 text with `writeText(path, content)`.
`content` uses the standard `BodyInit` input family, such as a string,
`Uint8Array`, `Blob`, `FormData`, or `ReadableStream`.

```ts
import { connect } from "@vmon/sdk";

const sandbox = await connect().sandboxes.get("sbx_123");
await sandbox.files.writeText("/tmp/message.txt", "hello\n");

const text = await sandbox.files.readText("/tmp/message.txt");
const bytes = await sandbox.files.read("/tmp/message.txt");
console.log(text, bytes.byteLength);
```

`list(path = ".")` returns `FileInfo[]`, `stat(path)` returns a `FileInfo`,
`delete(path, recursive = false)` removes a guest path, and `mkdir(path)`
creates a directory and its parents. `mkdir` is implemented as a remote
`mkdir -p -- <path>` command; its failure surfaces as an SDK error based on
that command's stderr.

```ts
await sandbox.files.mkdir("/tmp/output");
await sandbox.files.write("/tmp/output/data.bin", new Uint8Array([1, 2, 3]));

const entries = await sandbox.files.list("/tmp/output");
const metadata = await sandbox.files.stat("/tmp/output/data.bin");
console.log(entries, metadata.size);

await sandbox.files.delete("/tmp/output", true);
```

`FileInfo` is intentionally tolerant of daemon metadata. Its common optional
fields are `ok`, `name`, `type`, `size`, `mode`, and `mtime`; validate the
fields your application requires.

### Convenience, not server-side file streaming

`open(path)` returns a `ReadableStream<Uint8Array>`, but it first downloads the
entire file via `read()` and then enqueues one in-memory chunk. Likewise,
`write()` first materializes its `BodyInit` into bytes before sending one file
write request. These APIs are convenient for small or bounded files; they are
not server-side streaming transfer APIs and do not provide backpressure from
the guest file service. Avoid using `open()` or a streaming `BodyInit` for
large files when bounded memory matters.

## HTTP port proxy

`ports.http(port, method, path = "", options?)` makes one HTTP request through
the daemon's guest port proxy and returns the resulting `Response`. `port` must
be an integer from `0` through `65535`. The path is relative to that port; a
leading slash is accepted and normalized.

```ts
const response = await sandbox.ports.http(8080, "GET", "/health", {
  headers: { accept: "application/json" },
  params: { verbose: true },
});

if (!response.ok) {
  throw new Error(`health request returned ${response.status}`);
}
console.log(await response.json());
```

The `options` are request options accepted by the SDK driver (except its
internal endpoint selection), including a request body, headers, and query
`params`. This method deliberately returns HTTP responses, including
non-2xx responses, so inspect `response.ok` and `response.status` yourself.
It is not a general daemon-resource HTTP API.

Before proxying, the SDK obtains the sandbox tunnel token through `tunnels()`
and adds it as `connect_token`. Caller-supplied query keys `connect_token`,
`token`, and `access_token` are removed, so they cannot override or inject a
proxy credential. The token is an implementation detail; do not copy it into
URLs or logs.

## WebSocket port proxy

`ports.websocket(port, path = "", query = {})` opens a `WebSocket` to a
proxied guest port. It uses the same valid port range and path normalization as
HTTP, and applies the same tunnel-token handling to its query parameters.

```ts
const socket = await sandbox.ports.websocket(8080, "/socket", {
  room: "build-42",
});

socket.addEventListener("message", (event) => {
  console.log(event.data);
});
socket.send("hello");
```

The returned object is the platform `WebSocket`; connection state, message
ordering, close handling, reconnection, and application protocol semantics
remain the caller's responsibility. The SDK only constructs the authenticated
proxy connection. It does not promise a particular guest WebSocket protocol or
reconnect a disconnected socket.

The SDK's browser-compatible gRPC bridge itself uses an HTTP(S) node endpoint
and a WebSocket upgrade to `/grpc`. It does not provide a Unix-domain-socket
transport for browser code. Use a network-reachable daemon endpoint appropriate
to the browser's security model and deployment policy. See [Connection Strings
and Contexts](../connection-strings.md) and [Networking](../../platform/networking.md).
