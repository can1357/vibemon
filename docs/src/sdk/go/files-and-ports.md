# Files and Ports

`Sandbox.Files` and `Sandbox.Ports` are remote client services attached to a sandbox handle. They operate through the `vmon serve` API; they do not expose local files or create a local network listener. Obtain a server-backed handle with `Create`, `Get`, `List`, or `Ref` as described in [Sandboxes](sandboxes.md).

For connection and shared context/error behavior, see [Connect](connect.md), [Connection Strings and Contexts](../connection-strings.md), and [Error Codes](../../reference/errors.md).

## Guest files

Use `sandbox.Files` for guest paths. `Read`, `Write`, `Stat`, `Delete`, and `Mkdir` reject an empty path. `List` treats an empty path as `"."`.

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

| Method | Result |
| --- | --- |
| `Read(ctx, path)` | Entire file as `[]byte`. |
| `Open(ctx, path)` | An `io.ReadCloser` over the file data. Close it when done. It is backed by the SDK's complete read, not a network-backed incremental file stream. |
| `Write(ctx, path, data)` | Writes the supplied bytes. |
| `List(ctx, path)` | `[]vmon.FileInfo` for a directory path; an empty path lists `.`. |
| `Stat(ctx, path)` | One `vmon.FileInfo`. |
| `Delete(ctx, path, options...)` | Removes a file or directory. Pass `vmon.DeleteOptions{Recursive: true}` for recursive deletion. |
| `Mkdir(ctx, path)` | Creates the directory and necessary parents. It runs `mkdir -p -- <path>` in the guest and reports nonzero command output as an error. |

`FileInfo` has `OK`, `Name`, `Type`, `Size`, `Mode`, and `ModTime` fields. A `List` response must include its entries; the SDK reports a protocol error for a malformed successful response rather than silently returning an empty list.

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

### Response bounds

`Read` enforces the client's configured maximum response size: when received file data is larger than `WithMaxResponseBytes` permits, it returns `*vmon.ResponseTooLargeError`. Set that bound when connecting if the application needs to limit in-memory file results. `Open` uses `Read`, so it has the same bound and complete-buffer behavior.

```go
client, err := vmon.Connect(dsn, vmon.WithMaxResponseBytes(8<<20))
if err != nil {
    return err
}
defer client.Close()
```

The configured maximum is not a promise that every remote operation is incrementally streamed or truncated; it specifically guards the file data returned by `Read` and the bounded inspection used for proxy relocation. Keep a context deadline around file operations when the caller needs an upper wait time.

## HTTP port proxy

Expose the desired guest port in `SandboxCreateRequest.Ports`, then proxy one HTTP request with `sandbox.Ports.HTTP`. The request is described by `vmon.ProxyRequest`: `Method`, guest-relative `Path`, `Query`, `Header`, and `Body`.

The SDK reads the supplied body completely and closes it when building the forwarded request. Supply a body that is safe to consume once, such as `http.NoBody`, `strings.NewReader`, or an application-owned `io.ReadCloser` that the SDK may close. The returned `*http.Response` is the downstream response; the caller must close `response.Body`.

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

The SDK preserves the downstream response, including non-2xx status codes, headers, and body. A guest application's 404 or other non-2xx status is therefore a response to inspect, not automatically a Go error. It removes `connect_token`, `token`, and `access_token` from caller-provided query values and inserts the sandbox's current connect token. Do not add those credentials yourself.

The proxy tracks endpoint affinity. With multiple configured endpoints, it retries a request only after an HTTP 404 response whose bounded JSON body has `{"code":"not_found"}`, indicating that the daemon no longer hosts the sandbox. It does **not** replay an ordinary application 404. Because an HTTP request body is read before forwarding, callers should still design side-effecting handlers with normal HTTP retry safety; the SDK's relocation behavior is not a general retry guarantee.

## WebSocket port proxy

`Ports.WebSocket` opens a connect-token-authenticated guest-port WebSocket. It accepts a port, guest-relative path, and `url.Values`; token-like query parameters are sanitized in the same way as HTTP proxy requests.

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

`WebSocketConn.Write` sends one complete text or binary message. Use only `vmon.WebSocketTextMessage` or `vmon.WebSocketBinaryMessage`; other values fail locally. `Read` waits for one text or binary message and returns its type and bytes. Both methods are context-aware: a canceled context returns its context error. A normal or going-away remote close is normalized to `io.EOF` by `Read`.

`Close` performs a normal WebSocket close and releases the connection. It is safe to defer after a successful open; it is the required cleanup path when ending proxy communication. The SDK offers message-oriented WebSocket transport only—do not assume an unimplemented TCP forwarding, framing protocol, or reconnect behavior.

## Network policy and tunnel metadata

Sandbox network policy is managed through `sandbox.Network` and `sandbox.SetNetwork`, not through the port-proxy calls. `Network` retrieves `vmon.NetworkState`; `SetNetwork` takes `vmon.NetworkPolicy` with pointer fields so that omitted fields remain unchanged. A non-`nil` allowlist pointer replaces that list, even if the slice is empty.

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

`NetworkState` includes server-reported `BlockNetwork`, CIDR and domain allowlists, persisted egress allowlists, and inbound CIDR allowlist data. `sandbox.Tunnels(ctx)` returns `vmon.TunnelSet`, mapping each exposed guest port to a daemon-side `TunnelTarget` and including a short-lived connect token. The proxy methods obtain and refresh that token from the sandbox handle as needed; treat `Tunnels` as metadata, not as a public URL or a replacement for `Ports.HTTP` and `Ports.WebSocket`.