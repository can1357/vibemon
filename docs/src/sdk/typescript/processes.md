# Processes

Process APIs execute in a remote sandbox through `vmon serve`; they do not
start a process on the JavaScript host. For connection and stream failure
semantics, see [Shared Concepts](../shared-concepts.md#timeouts-cancellation-and-streams)
and [Errors](errors.md).

## Captured commands

`Sandbox.run(cmd, options?)` executes a command and waits for a captured
result. `cmd` is an argument array, not a shell command line. The result has
the numeric `exit` code and Base64-encoded `stdout_b64` and `stderr_b64`.
Decode bytes explicitly before treating them as text.

```ts
import { connect } from "@vmon/sdk";

const sandbox = await connect().sandboxes.get("sbx_123");
const result = await sandbox.run(["sh", "-lc", "printf '%s' hello"]);

if (result.exit !== 0) {
  throw new Error(`command failed with exit ${result.exit}`);
}
const bytes = Uint8Array.from(
  atob(result.stdout_b64),
  (character) => character.charCodeAt(0),
);
const stdout = new TextDecoder().decode(bytes);
console.log(stdout);
```

`RunOptions` accepts `env`, `workdir`, `timeout`, `tty`, and `secrets`.
`timeout` is sent as the remote exec timeout; it is distinct from client
communication timeouts. `secrets` are merged into the command environment;
see [Volumes and Secrets](volumes-and-secrets.md#secrets).

## Interactive execution

`Sandbox.exec(cmd, options?)` opens a bidirectional `Process` and returns as
soon as the local stream wrapper is created. It does **not** wait for a ready
frame. Use it when input, streaming output, a TTY, or terminal resizing is
needed.

```ts
const process = await sandbox.exec(["sh"], { tty: true });
process.resize(24, 80);
process.stdin.write("printf 'hello\\n'\n");
process.stdin.close();

const decoder = new TextDecoder();
for await (const event of process) {
  if (event.stream === "stdout" || event.stream === "stderr") {
    console.log(decoder.decode(event.data, { stream: true }));
  }
}

const exit = await process.wait();
console.log(exit.code, exit.signal);
```

`Process` is an `AsyncIterable<ExecEvent>`. Each event has a `stream` of
`"stdout"`, `"stderr"`, or `"console"` and raw `Uint8Array` `data`. The
`onStdout` and `onStderr` options receive the same stdout and stderr chunks as
they arrive; they do not replace async iteration.

`stdin.write()` accepts a string (encoded as UTF-8) or a `Uint8Array`.
`stdin.close()` sends the process EOF frame: it closes **standard input**, not
the process handle. Continue consuming output and call `wait()` to observe the
remote exit. `resize(rows, cols)` sends a terminal resize frame and is useful
only for a remote TTY program; the SDK does not validate dimensions or create
a terminal by itself.

`wait()` resolves to `{ code, signal }`, where `signal` is a number or `null`.
If the RPC fails, ends without an exit frame, or a process is closed before
completion, `wait()` rejects and async iteration fails. `close()` cancels the
underlying exec RPC and causes the server to terminate the guest process. After
an exit or `close()`, further writes, EOF, and resize attempts throw a
`ProtocolError` because the process is no longer open.

## Console attachment and logs

`attach()` yields a read-only `ConsoleStream` for console bytes. It has no
stdin or resize API. Break from or cancel an attachment deliberately with
`close()` when it is no longer needed.

```ts
const consoleStream = await sandbox.attach();
const decoder = new TextDecoder();

for await (const bytes of consoleStream) {
  console.log(decoder.decode(bytes, { stream: true }));
}
```

`logs()` consumes the current non-following log RPC and returns its decoded
text. `followLogs()` returns a `LogStream`, an async iterable of decoded text
chunks. Chunks are transport chunks rather than guaranteed lines; preserve
partial data if line-oriented processing matters. Call `close()` on the
returned `LogStream` to cancel the follow RPC.

```ts
const initial = await sandbox.logs();
console.log(initial);

const logs = await sandbox.followLogs();
for await (const chunk of logs) {
  console.log(chunk);
}
```

## Client shell

`client.shell(request?)` starts the daemon's interactive shell RPC and waits
for its ready frame before resolving with a `Process`. Its optional request is
a JSON object passed to the daemon; it is not a shell command and is not bound
to a `Sandbox` handle.

```ts
const shell = await sandbox.client.shell({ image: "alpine" });
shell.stdin.write("exit\n");
const exit = await shell.wait();
console.log(exit.code);
```

If the shell exits before its ready frame, `client.shell()` rejects with a
`ProtocolError`. Treat stream disconnection as an uncertain remote-operation
boundary and do not automatically replay input; the shared stream guidance
explains why.
