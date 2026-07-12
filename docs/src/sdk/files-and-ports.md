# Files and Ports

Every sandbox exposes remote file and port helpers. They operate through the running `vmon serve` daemon and are bound to that sandbox; they do not expose the client machine's filesystem, create a local listener, or provide raw guest sockets. Obtain a server-backed handle as described in [Sandboxes](sandboxes.md).

For connection and shared error behavior, see [Connect](connect.md), [Connection Strings and Contexts](connection-strings.md), and [Error Codes](../reference/errors.md).

## Guest files

The file helper uploads, downloads, inspects, and removes guest paths. Reads and writes transfer a complete file in one operation; the convenience stream/open APIs described below are backed by an in-memory whole-file transfer, not server-side streaming.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`Sandbox.files` provides byte and text variants. `write_bytes()` and `write_text()` replace a file; `read_bytes()` and `read_text()` download the complete file. Text methods default to UTF-8, and writes default to mode `0o644`.

```python
sandbox.files.mkdir("/work/output")
sandbox.files.write_text("/work/output/result.txt", "done\n")

entry = sandbox.files.stat("/work/output/result.txt")
assert entry.size == len(b"done\n")
assert sandbox.files.read_text("/work/output/result.txt") == "done\n"

for entry in sandbox.files.list("/work/output"):
    print(entry.name, entry.type, entry.size)
```

The supported operations are:

- `read_bytes(path)` and `read_text(path, encoding="utf-8")`;
- `write_bytes(path, data, mode=0o644)` and `write_text(path, text, encoding="utf-8", mode=0o644)`;
- `open(path)`, returning a closeable, in-memory `FileStream`;
- `list(path=".")` and `stat(path)`;
- `delete(path, recursive=False)`; and
- `mkdir(path, parents=True)`.

```python
with sandbox.files.open("/work/output/result.txt") as file:
    first = file.read(4)
    rest = file.read()
assert first + rest == b"done\n"
```

`open()` downloads the file before returning. Its stream is not remote, seekable, or writable: `read()` consumes a local buffer, iteration yields the remaining bytes once, and `close()` releases the buffer. Thread-backed async equivalents are available through `sandbox.files.aio`, such as `await sandbox.files.aio.read_text(path)`.

</div>
<div data-sdk-language="go">

`Sandbox.Files` uses byte-oriented transfers. `Write` uploads the supplied bytes, while `Read` downloads the complete file.

```go
files := sandbox.Files

if err := files.Mkdir(ctx, "/workspace/output"); err != nil {
    return err
}
if err := files.Write(ctx, "/workspace/output/message.txt", []byte("hello\n")); err != nil {
    return err
}

data, err := files.Read(ctx, "/workspace/output/message.txt")
if err != nil {
    return err
}
fmt.Print(string(data))
```

The supported operations are:

- `Read(ctx, path)`, returning the entire file as `[]byte`;
- `Open(ctx, path)`, returning an `io.ReadCloser` over an already-completed read;
- `Write(ctx, path, data)`;
- `List(ctx, path)` and `Stat(ctx, path)`;
- `Delete(ctx, path, options...)`, with `vmon.DeleteOptions{Recursive: true}` for recursive removal; and
- `Mkdir(ctx, path)`.

`Read`, `Write`, `Stat`, `Delete`, and `Mkdir` reject an empty path. `List` treats an empty path as `"."`. `Mkdir` runs `mkdir -p -- <path>` in the guest and reports nonzero command output as an error.

```go
entries, err := files.List(ctx, "/workspace/output")
if err != nil {
    return err
}
for _, entry := range entries {
    fmt.Printf("%s %d bytes\n", entry.Name, entry.Size)
}

if err := files.Delete(ctx, "/workspace/output", vmon.DeleteOptions{Recursive: true}); err != nil {
    return err
}
```

`Open` is not a network-backed incremental stream. It uses `Read`, so both operations buffer the complete file and enforce the client's `WithMaxResponseBytes` limit. An oversized result returns `*vmon.ResponseTooLargeError`.

```go
client, err := vmon.Connect(dsn, vmon.WithMaxResponseBytes(8<<20))
if err != nil {
    return err
}
defer client.Close()
```

The configured maximum guards file data returned by `Read` and bounded proxy-relocation inspection; it does not make remote operations incremental or truncate their results. Use context deadlines to bound wait time.

</div>
<div data-sdk-language="typescript">

`Sandbox.files` provides byte and UTF-8 text variants. `write()` uploads any standard `BodyInit`, such as a string, `Uint8Array`, `Blob`, `FormData`, or `ReadableStream`; `read()` downloads the complete file as a `Uint8Array`.

```ts
await sandbox.files.mkdir("/tmp/output");
await sandbox.files.writeText("/tmp/output/message.txt", "hello\n");

const text = await sandbox.files.readText("/tmp/output/message.txt");
const bytes = await sandbox.files.read("/tmp/output/message.txt");
console.log(text, bytes.byteLength);
```

The supported operations are:

- `read(path)` and `readText(path)`;
- `write(path, content)` and `writeText(path, content)`;
- `open(path)`, returning a `ReadableStream<Uint8Array>`;
- `list(path = ".")` and `stat(path)`;
- `delete(path, recursive = false)`; and
- `mkdir(path)`.

```ts
await sandbox.files.write("/tmp/output/data.bin", new Uint8Array([1, 2, 3]));

const entries = await sandbox.files.list("/tmp/output");
const metadata = await sandbox.files.stat("/tmp/output/data.bin");
console.log(entries, metadata.size);

await sandbox.files.delete("/tmp/output", true);
```

`open()` first downloads the entire file and then enqueues one in-memory chunk. Likewise, `write()` materializes its `BodyInit` into bytes before sending one write request. Neither API provides guest-side streaming or backpressure, so avoid them for large files when bounded memory matters. `mkdir` runs remote `mkdir -p -- <path>` and surfaces command failures as SDK errors.

</div>
</div>

File metadata is daemon-supplied. Its common fields describe success, name, type, size, mode, and modification time.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`FileInfo` has `ok`, `name`, `type`, `size`, `mode`, and `mtime` fields.

</div>
<div data-sdk-language="go">

`vmon.FileInfo` has `OK`, `Name`, `Type`, `Size`, `Mode`, and `ModTime` fields. A successful `List` response must contain its entries; malformed success data produces a protocol error rather than an empty list.

</div>
<div data-sdk-language="typescript">

`FileInfo` intentionally tolerates daemon metadata. Its common fields—`ok`, `name`, `type`, `size`, `mode`, and `mtime`—are optional, so validate the fields the application requires.

</div>
</div>

## Expose ports and discover tunnels

Declare guest ports when creating a sandbox. Tunnel discovery returns daemon-side TCP targets and, when supplied by the server, a short-lived connection token. Discovery does not make an HTTP request, open a WebSocket, or create a local port forward.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
sandbox = client.sandboxes.create(
    image="alpine",
    ports=[8080],
    command=["sh", "-lc", "python3 -m http.server 8080"],
)

tunnels = sandbox.tunnels()
target = tunnels.get(8080)
if target is not None:
    print(f"guest 8080 is available at {target.host}:{target.port}")
```

`Sandbox.tunnels()` returns a mapping-like `TunnelSet`; guest port numbers map to `TunnelTarget(host, port)`. Use a tunnel target for direct TCP connectivity when the client can reach it. `TunnelSet.connect_token` is daemon authentication metadata, not a public URL.

</div>
<div data-sdk-language="go">

Expose ports through `SandboxCreateRequest.Ports`. `sandbox.Tunnels(ctx)` returns `vmon.TunnelSet`, mapping guest ports to daemon-side `TunnelTarget` values and including a short-lived connect token.

Treat the result as metadata rather than a public URL or replacement for the proxy APIs. The SDK does not implement a general TCP forwarder or reconnect protocol.

</div>
<div data-sdk-language="typescript">

Expose ports in the sandbox creation request. The proxy helpers call `tunnels()` internally to obtain the target metadata and current connect token; applications normally do not need to handle that token.

The SDK does not create local listeners or expose raw TCP sockets. Its browser-compatible gRPC bridge also requires an HTTP(S) daemon endpoint and a WebSocket upgrade to `/grpc`; it does not support Unix-domain sockets in browser code. See [Networking](../platform/networking.md).

</div>
</div>

## HTTP port proxy

The HTTP helper sends one request through the daemon to an exposed guest port. The SDK obtains the sandbox's current connect token and removes caller-provided `connect_token`, `token`, and `access_token` query parameters before inserting it. Do not add, copy into URLs, or log those credentials.

Guest 4xx and 5xx statuses are returned as ordinary HTTP responses rather than SDK errors; inspect the response according to the language's HTTP API.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`Sandbox.ports.http(port, method, path="", *, params=None, headers=None, content=None, json=None)` returns an `httpx.Response`. The method must be a supported HTTP verb and the port must be in the TCP port range. Pass `content=` for bytes or `json=` for a JSON body.

```python
response = sandbox.ports.http(
    8080,
    "GET",
    "/health",
    params={"verbose": "1"},
    headers={"Accept": "application/json"},
)
response.raise_for_status()
print(response.json())
```

The proxy uses the sandbox's currently pinned mesh endpoint. It is useful when the client can reach the daemon but cannot reach a tunnel target directly; it is not a general-purpose local port forwarder.

</div>
<div data-sdk-language="go">

`sandbox.Ports.HTTP` accepts a `vmon.ProxyRequest` containing `Method`, guest-relative `Path`, `Query`, `Header`, and `Body`, and returns `*http.Response`. The SDK completely reads and closes the request body while building the forwarded request. Use a body safe to consume once; the caller must close the returned response body.

```go
response, err := sandbox.Ports.HTTP(ctx, 8080, vmon.ProxyRequest{
    Method: http.MethodPost,
    Path:   "api/items",
    Query:  url.Values{"verbose": []string{"1"}},
    Header: http.Header{"Content-Type": []string{"application/json"}},
    Body:   strings.NewReader(`{"name":"widget"}`),
})
if err != nil {
    return err
}
defer response.Body.Close()

body, err := io.ReadAll(response.Body)
if err != nil {
    return err
}
fmt.Printf("%s: %s\n", response.Status, body)
```

The SDK preserves downstream status, headers, and body. With multiple configured endpoints, it relocates and retries only after an HTTP 404 whose bounded JSON body contains `{"code":"not_found"}`, indicating that the daemon no longer hosts the sandbox. It does not replay an ordinary application 404. This endpoint-affinity behavior is not a general retry guarantee; design side-effecting handlers with normal HTTP retry safety.

</div>
<div data-sdk-language="typescript">

`ports.http(port, method, path = "", options?)` returns a platform `Response`. The port must be an integer from `0` through `65535`; a leading slash in the path is accepted and normalized. Options include a body, headers, and query `params`, but not the SDK's internal endpoint selection.

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

This is a sandbox guest-port proxy, not a general daemon-resource HTTP API. Inspect `response.ok` or `response.status` for application failures.

</div>
</div>

## WebSocket port proxy

The WebSocket helper opens an authenticated connection through the same guest-port proxy. It applies the same port/path rules and removes token-like caller query parameters before adding the daemon-issued token. The SDK establishes transport only; the application owns its protocol, connection lifecycle, and cleanup.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`Sandbox.ports.websocket(port, path="", *, params=None)` returns the SDK transport's `WebSocketConnection`, not an `httpx.Response`.

```python
with sandbox.ports.websocket(8080, "/events") as websocket:
    websocket.send_text("subscribe")
    message = websocket.recv_message()
    print(message)
```

The Python helper is scoped to the sandbox's currently pinned mesh endpoint. It does not provide a general-purpose port forwarder.

</div>
<div data-sdk-language="go">

`Ports.WebSocket` accepts a port, guest-relative path, and `url.Values`, and returns a message-oriented `WebSocketConn`.

```go
socket, err := sandbox.Ports.WebSocket(ctx, 8080, "chat", url.Values{
    "room": []string{"general"},
})
if err != nil {
    return err
}
defer socket.Close()

if err := socket.Write(ctx, vmon.WebSocketTextMessage, []byte("hello")); err != nil {
    return err
}
messageType, data, err := socket.Read(ctx)
if err != nil {
    return err
}
if messageType != vmon.WebSocketTextMessage {
    return fmt.Errorf("unexpected message type %d", messageType)
}
fmt.Println(string(data))
```

`Write` accepts only complete text or binary messages. `Read` returns one text or binary message; normal and going-away remote closes become `io.EOF`. Both methods honor context cancellation. `Close` performs a normal close and is the required cleanup path. The SDK does not provide raw TCP forwarding, custom framing, or reconnection.

</div>
<div data-sdk-language="typescript">

`ports.websocket(port, path = "", query = {})` returns the platform `WebSocket`.

```ts
const socket = await sandbox.ports.websocket(8080, "/socket", {
  room: "build-42",
});

socket.addEventListener("message", (event) => {
  console.log(event.data);
});
socket.send("hello");
```

Connection state, message ordering, close handling, reconnection, and application protocol semantics remain the caller's responsibility. The SDK constructs the authenticated proxy connection but does not reconnect a disconnected socket or promise a guest protocol.

</div>
</div>

## Network policy

Network policy is separate from tunnel discovery and port proxying. Manage it through the sandbox network APIs; proxy calls neither configure nor bypass it.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Use the sandbox network-policy API described by the Python SDK. Tunnel and proxy helpers do not mutate sandbox network policy.

</div>
<div data-sdk-language="go">

Use `sandbox.Network` to retrieve `vmon.NetworkState` and `sandbox.SetNetwork` with `vmon.NetworkPolicy` to update it. Pointer fields distinguish an omitted setting from a replacement; a non-`nil` allowlist pointer replaces the list even when the slice is empty.

```go
blocked := true
state, err := sandbox.SetNetwork(ctx, vmon.NetworkPolicy{
    BlockNetwork: &blocked,
})
if err != nil {
    return err
}
fmt.Println(*state.BlockNetwork)
```

`NetworkState` includes server-reported blocking, CIDR and domain allowlists, persisted egress allowlists, and inbound CIDR allowlist data.

</div>
<div data-sdk-language="typescript">

Use the sandbox network-policy API rather than the port helpers to control guest networking. Tunnel and proxy calls do not establish a browser security exception or alter deployment policy.

</div>
</div>
