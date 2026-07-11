# Files and Ports

Every `Sandbox` has `files` and `ports` helpers. They operate on that sandbox; they are not host filesystem or host network APIs.

## Guest files

`Sandbox.files` supplies byte- and text-oriented guest filesystem operations:

- `read_bytes(path)` and `read_text(path, encoding="utf-8")` read one complete file;
- `write_bytes(path, data, mode=0o644)` and `write_text(path, text, encoding="utf-8", mode=0o644)` replace a file;
- `list(path=".")` returns `list[FileInfo]`;
- `stat(path)` returns a `FileInfo`;
- `delete(path, recursive=False)` removes a path; and
- `mkdir(path, parents=True)` creates a directory through the guest agent.

`FileInfo` has `ok`, `name`, `type`, `size`, `mode`, and `mtime` fields. Paths are guest paths.

```python
sandbox.files.mkdir("/work/output")
sandbox.files.write_text("/work/output/result.txt", "done\n")

entry = sandbox.files.stat("/work/output/result.txt")
assert entry.size == len(b"done\n")
assert sandbox.files.read_text("/work/output/result.txt") == "done\n"

for entry in sandbox.files.list("/work/output"):
    print(entry.name, entry.type, entry.size)
```

Use `open(path)` when a file-like, closeable in-memory reader is more convenient:

```python
with sandbox.files.open("/work/output/result.txt") as file:
    first = file.read(4)
    rest = file.read()
assert first + rest == b"done\n"
```

`open()` fetches the guest file before returning the `FileStream`; it is not a remote, seekable, or writable file descriptor. `FileStream.read()` consumes its local buffer, iteration yields the remaining bytes once, and `close()` releases that buffer. Use `read_bytes()` or `write_bytes()` for explicit whole-file transfer.

The thread-backed async equivalents are available through `sandbox.files.aio`, for example `await sandbox.files.aio.read_text("/work/output/result.txt")`. See [Async API](async.md) for the execution model.

## Exposed ports and tunnels

Expose guest ports when creating a sandbox:

```python
sandbox = client.sandboxes.create(
    image="alpine",
    ports=[8080],
    command=["sh", "-lc", "python3 -m http.server 8080"],
)
```

`Sandbox.tunnels()` lists the daemon-side TCP targets for the sandbox's exposed guest ports. It returns a mapping-like `TunnelSet`: guest port numbers map to `TunnelTarget(host, port)`. The returned object also carries a `connect_token` when the server provides one.

```python
tunnels = sandbox.tunnels()
target = tunnels.get(8080)
if target is not None:
    print(f"guest 8080 is available at {target.host}:{target.port}")
```

Tunnel discovery and proxying are different operations. `tunnels()` gives typed target information; it does **not** make an HTTP request or open a WebSocket. For requests through the daemon's sandbox port proxy, use `Sandbox.ports`.

## HTTP and WebSocket proxying

`Sandbox.ports.http(port, method, path="", *, params=None, headers=None, content=None, json=None)` sends one HTTP request to an exposed guest port and returns an `httpx.Response`. The method must be a supported HTTP verb and the port must be in the TCP port range. The SDK obtains and adds the daemon-issued connection token; do not add `connect_token`, `token`, or `access_token` yourself.

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

Pass `content=` for bytes or `json=` for a JSON request body. `http()` returns guest 4xx and 5xx responses unchecked; call `response.raise_for_status()` when those statuses should raise. This proxy API is useful when the client can reach the daemon but not a tunnel target directly.

For an upgraded connection, call `Sandbox.ports.websocket(port, path="", *, params=None)`. It returns a `WebSocketConnection` supplied by the SDK transport, not an `httpx.Response`.

```python
with sandbox.ports.websocket(8080, "/events") as websocket:
    websocket.send_text("subscribe")
    message = websocket.recv_message()
    print(message)
```

The proxy calls are scoped to the sandbox object and use its currently pinned mesh endpoint. They are not a general-purpose port-forwarder; use the tunnel target from `tunnels()` for direct TCP connectivity when that is the connection model you need.
