# Processes

Process APIs execute commands in a remote sandbox through a running `vmon serve` daemon; they do not start processes on the SDK host. Use captured execution for a bounded result after a command exits, and streaming execution when you need incremental output, stdin, terminal resizing, or explicit lifetime control.

Commands are argument vectors. The daemon does not implicitly parse a shell command line, so invoke a shell explicitly when redirection, pipelines, expansion, or other shell syntax is required. Command-specific working directories, environment values, terminal flags, and execution timeouts are sent to the daemon.

For sandbox lifecycle and configuration, see [Sandboxes](sandboxes.md). For connection setup and shared failures, see [Connect](connect.md), [Connection Strings and Contexts](connection-strings.md), and [Error Codes](../reference/errors.md).

## Captured execution

Captured execution waits for the command to finish and returns its exit code, stdout, and stderr together. A nonzero command exit is data in the result rather than an SDK transport error; applications decide which exit codes are acceptable. Output is binary at the protocol boundary, so decode it only when the command's encoding is known.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`Sandbox.run(*cmd, workdir=None, env=None, timeout=None, tty=False)` accepts separate command arguments or one iterable. It returns an `ExecResult` with `returncode`, `stdout`, and `stderr`; both output fields are `bytes`. Command `env` values augment the sandbox's bound environment and override values with the same names.

```python
from vmon import connect

with connect() as client:
    sandbox = client.sandboxes.create(image="alpine")
    try:
        result = sandbox.run(
            "sh", "-lc", "printf '%s\\n' \"$GREETING\"",
            env={"GREETING": "hello"},
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"command failed ({result.returncode}): "
                f"{result.stderr.decode('utf-8', errors='replace')}"
            )
        print(result.stdout.decode("utf-8"), end="")
    finally:
        sandbox.terminate()
```

</div>
<div data-sdk-language="go">

Pass a non-empty argument vector in `vmon.ExecRequest.Command`. `Workdir`, `Env`, `Timeout`, and `TTY` are passed to the server; `Timeout` is a pointer to a timeout value in seconds. `Sandbox.Run` returns an `ExecResult` containing `ExitCode`, `Stdout`, and `Stderr`.

```go
result, err := sandbox.Run(ctx, vmon.ExecRequest{
    Command: []string{"sh", "-c", "printf '%s\\n' \"$GREETING\""},
    Env:     map[string]string{"GREETING": "hello"},
})
if err != nil {
    return err
}
if result.ExitCode != 0 {
    return fmt.Errorf("command failed (%d): %s", result.ExitCode, result.Stderr)
}
fmt.Print(string(result.Stdout))
```

Context cancellation ends the in-flight unary request.

</div>
<div data-sdk-language="typescript">

`Sandbox.run(cmd, options?)` takes an argument array. `RunOptions` accepts `env`, `workdir`, `timeout`, `tty`, and `secrets`; secrets are merged into the command environment as described in [Volumes and Secrets](volumes-and-secrets.md#secrets). The result has numeric `exit` and Base64-encoded `stdout_b64` and `stderr_b64` fields, which must be decoded explicitly.

```ts
const result = await sandbox.run(
  ["sh", "-lc", "printf '%s\\n' \"$GREETING\""],
  { env: { GREETING: "hello" } },
);

const decode = (value: string) => new TextDecoder().decode(
  Uint8Array.from(atob(value), (character) => character.charCodeAt(0)),
);
if (result.exit !== 0) {
  throw new Error(`command failed (${result.exit}): ${decode(result.stderr_b64)}`);
}
console.log(decode(result.stdout_b64));
```

</div>
</div>

Use captured execution for bounded command results and readiness checks. It is not incremental: no output is available until the command completes.

## Streaming execution

Streaming execution opens a bidirectional process session. Output arrives as framed stdout or stderr chunks, and the final frame reports the exit code and optional terminating signal. Transport chunks are not guaranteed to align with text characters or lines; use an incremental decoder or preserve partial data when boundaries matter. Keep a single consumer for each stream or event source, and always provide a way to cancel and close a session.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`Sandbox.exec(*cmd, workdir=None, env=None, timeout=None, pty=False, tty=False)` returns a `Process` immediately. `pty=True` and `tty=True` are equivalent terminal requests. The process exposes separate `stdout` and `stderr` `ByteStream` objects, `stdin`, `wait(timeout=None)`, `resize(rows, cols)`, and `close()`.

```python
process = sandbox.exec("sh", "-lc", "echo out; echo err >&2")
try:
    status = process.wait(timeout=10)
    stdout = process.stdout.read().decode("utf-8")
    stderr = process.stderr.read().decode("utf-8")
finally:
    process.close()

print(stdout, end="")
print(stderr, end="")
if status.code != 0:
    raise RuntimeError(f"process exited with {status.code}")
```

A `ByteStream` is iterable and also provides `read_chunk(timeout=None)` and `read(size=-1)`. `read_chunk()` returns `None` at end of stream and raises `TimeoutError` if no chunk arrives in time. A positive `read(size)` may wait for that many bytes or end-of-stream. Do not run multiple consumers against the same stream because each consumed chunk is removed.

`Process` is a context manager, so deterministic cleanup can be expressed with `with sandbox.exec(...) as process:`.

</div>
<div data-sdk-language="go">

`Sandbox.Exec` returns a `*vmon.Process`. Call `Receive(ctx)` until an event has a non-`nil` `Exit`. Output events identify `vmon.StreamStdout` or `vmon.StreamStderr` in `Stream` and carry bytes in `Data`; the exit contains its code and optional signal.

```go
process, err := sandbox.Exec(ctx, vmon.ExecRequest{
    Command: []string{"sh", "-c", "echo out; echo err >&2"},
})
if err != nil {
    return err
}
defer process.Close()

for {
    event, err := process.Receive(ctx)
    if err != nil {
        return err
    }
    if event.Exit != nil {
        if event.Exit.Code != 0 {
            return fmt.Errorf("process exited with %d", event.Exit.Code)
        }
        break
    }
    switch event.Stream {
    case vmon.StreamStdout:
        _, err = os.Stdout.Write(event.Data)
    case vmon.StreamStderr:
        _, err = os.Stderr.Write(event.Data)
    default:
        return fmt.Errorf("unexpected process stream: %v", event.Stream)
    }
    if err != nil {
        return err
    }
}
```

`Receive(ctx)` blocks for one frame. Canceling that context closes the session and returns the context error; `io.EOF` means the stream ended without another event. Treat unknown frames and streams as protocol errors.

For common output forwarding, `Copy(ctx, stdout, stderr)` receives until exit and writes each stream to its corresponding `io.Writer`. `Wait(ctx)` does the same while discarding output.

```go
exit, err := process.Copy(ctx, os.Stdout, os.Stderr)
if err != nil {
    return err
}
if exit.Code != 0 {
    return fmt.Errorf("command exited with %d", exit.Code)
}
```

</div>
<div data-sdk-language="typescript">

`Sandbox.exec(cmd, options?)` returns a `Process` as soon as the local stream wrapper is created; it does not wait for a ready frame. The process is an `AsyncIterable<ExecEvent>`. Events have `stream` set to `"stdout"`, `"stderr"`, or `"console"` and raw `Uint8Array` data. Optional `onStdout` and `onStderr` callbacks receive the same output chunks and do not replace iteration.

```ts
const process = await sandbox.exec(
  ["sh", "-lc", "echo out; echo err >&2"],
);
const decoder = new TextDecoder();

for await (const event of process) {
  if (event.stream === "stdout") {
    console.log(decoder.decode(event.data, { stream: true }));
  } else if (event.stream === "stderr") {
    console.error(decoder.decode(event.data, { stream: true }));
  }
}

const exit = await process.wait();
if (exit.code !== 0) {
  throw new Error(`process exited with ${exit.code}`);
}
```

`wait()` resolves to `{ code, signal }`, where `signal` is a number or `null`. If the RPC fails, ends without an exit frame, or is closed before completion, `wait()` rejects and async iteration fails.

</div>
</div>

## Standard input and terminals

Sending EOF closes remote standard input, not the process session. Continue consuming output and wait for the final exit. A terminal request asks the guest to allocate terminal behavior; the SDK does not emulate a terminal locally. Resize operations are meaningful only for programs running with a terminal.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`Process.stdin.write()` accepts `bytes` or `str`; `stdin.close()` sends EOF. Closing stdin twice is harmless, while a later write raises a transport error. For a non-terminal process, `wait()` closes still-open stdin before waiting so a command blocked on EOF can finish. It does not do this for terminal processes. If `wait(timeout=...)` times out, it terminates the process before raising `TimeoutError`.

```python
with sandbox.exec("sh", pty=True, timeout=30) as process:
    process.resize(40, 120)
    process.stdin.write("printf 'terminal\\n'; exit\n")
    process.stdin.close()
    status = process.wait(timeout=30)
    output = process.stdout.read().decode("utf-8")

assert status.code == 0
print(output, end="")
```

`close()` terminates an unfinished command and closes both output streams.

</div>
<div data-sdk-language="go">

`WriteStdin` sends one byte chunk, `CloseStdin` sends EOF, and `Resize` sends terminal rows and columns. After `CloseStdin` succeeds, later writes fail locally.

```go
process, err := sandbox.Exec(ctx, vmon.ExecRequest{
    Command: []string{"sh"},
    TTY:     true,
})
if err != nil {
    return err
}
defer process.Close()

if err := process.Resize(ctx, 40, 120); err != nil {
    return err
}
if err := process.WriteStdin(ctx, []byte("echo ready\\nexit\\n")); err != nil {
    return err
}
if err := process.CloseStdin(ctx); err != nil {
    return err
}
exit, err := process.Wait(ctx)
if err != nil {
    return err
}
if exit.Code != 0 {
    return fmt.Errorf("shell exited with %d", exit.Code)
}
```

`Process.Close` cancels the session and releases its resources; it is safe to defer after a successful `Exec` or `Shell`. It is not a graceful stdin EOF. Cancellation during `WriteStdin`, `CloseStdin`, or `Resize` aborts that frame operation and closes the session.

</div>
<div data-sdk-language="typescript">

`stdin.write()` accepts a string encoded as UTF-8 or a `Uint8Array`; `stdin.close()` sends EOF. `resize(rows, cols)` sends terminal dimensions but does not validate them or create a terminal.

```ts
const process = await sandbox.exec(["sh"], { tty: true });
process.resize(40, 120);
process.stdin.write("echo ready\nexit\n");
process.stdin.close();

const exit = await process.wait();
if (exit.code !== 0) {
  throw new Error(`shell exited with ${exit.code}`);
}
```

`close()` cancels the exec RPC and causes the server to terminate the guest process. After exit or `close()`, writes, EOF, and resize attempts throw `ProtocolError` because the process is no longer open.

</div>
</div>

## Shell sessions

A client shell uses the same process input, output, resize, wait, and close operations as streaming execution. Unlike sandbox `exec`, shell setup consumes a server ready frame before returning the process. The ready frame identifies the sandbox; consumers begin with process output and must not try to receive that frame themselves.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`Client.shell(*cmd, ref=None, image=None, cpus=1, memory=512, disk_mb=1024, timeout=300)` always requests a terminal. With no command it starts `/bin/sh`. A live sandbox `ref` runs there and is not ephemeral; otherwise the daemon creates an ephemeral fallback sandbox from `ref`, `image`, or its default shell image and removes it when the shell closes. The returned process exposes its announced `sandbox_id`.

```python
from vmon import connect

with connect() as client:
    shell = client.shell("sh", "-lc", "printf ready", image="alpine")
    try:
        output = shell.stdout.read().decode("utf-8")
        status = shell.wait(timeout=60)
    finally:
        shell.close()

assert status.code == 0
assert output == "ready"
```

Use `SandboxAPI.create()` when mounts or other sandbox configuration must exist before the shell starts.

</div>
<div data-sdk-language="go">

`Client.Shell` accepts a `vmon.ShellRequest`. `Reference` can name an existing sandbox, template, or image; `Image` explicitly chooses an ephemeral image. The request can also specify `Command`, CPU, memory, disk, and timeout values. The returned process has `SandboxID` set from the ready frame.

```go
shell, err := client.Shell(ctx, vmon.ShellRequest{
    Reference: "sandbox-id",
    Command:   []string{"sh"},
})
if err != nil {
    return err
}
defer shell.Close()

if err := shell.WriteStdin(ctx, []byte("uname -s\\nexit\\n")); err != nil {
    return err
}
if err := shell.CloseStdin(ctx); err != nil {
    return err
}
exit, err := shell.Copy(ctx, os.Stdout, os.Stderr)
if err != nil {
    return err
}
fmt.Println(exit.Code)
```

</div>
<div data-sdk-language="typescript">

`client.shell(request?)` accepts an optional JSON request passed to the daemon; it is not a shell command and is not bound to a `Sandbox` handle.

```ts
const shell = await sandbox.client.shell({ image: "alpine" });
shell.stdin.write("exit\n");
const exit = await shell.wait();
console.log(exit.code);
```

If the shell exits before its ready frame, `client.shell()` rejects with `ProtocolError`.

</div>
</div>

## Console attachment and logs

Console attachment is a read-only stream of live serial-console bytes. It is not a process and has no stdin or resize operation. Log retrieval is separate: a non-following call gathers currently available logs, while following returns a closeable stream. Log chunks are transport chunks rather than guaranteed lines. Close attachments and follow streams deliberately so they cannot remain blocked without a cancellation path.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`Sandbox.attach()` returns a `ConsoleStream`. Iterate it or call `read(size=-1)`; `wait(timeout=None)` waits for the attachment to end and `close()` stops it. `Sandbox.logs()` returns buffered text, while `Sandbox.logs(follow=True)` returns a context-manageable `LogStream` of UTF-8-decoded chunks with invalid bytes replaced.

```python
with sandbox.attach() as console:
    for chunk in console:
        print(chunk.decode("utf-8", errors="replace"), end="")

print(sandbox.logs())
with sandbox.logs(follow=True) as logs:
    for chunk in logs:
        print(chunk, end="")
```

</div>
<div data-sdk-language="go">

`Sandbox.Attach` returns a `*vmon.ConsoleStream`. `Receive(ctx)` returns `StreamEvent` values on `vmon.StreamConsole`; `io.EOF` marks the natural end.

```go
console, err := sandbox.Attach(ctx)
if err != nil {
    return err
}
defer console.Close()
for {
    event, err := console.Receive(ctx)
    if errors.Is(err, io.EOF) {
        break
    }
    if err != nil {
        return err
    }
    if event.Stream != vmon.StreamConsole {
        return fmt.Errorf("unexpected console stream: %v", event.Stream)
    }
    if _, err := os.Stdout.Write(event.Data); err != nil {
        return err
    }
}
```

`Logs(ctx)` gathers available chunks into one string. `FollowLogs(ctx)` returns a `*vmon.LogStream`; call `Next()` until `io.EOF`, and defer `Close()` to cancel the underlying stream when stopping early.

```go
logs, err := sandbox.FollowLogs(ctx)
if err != nil {
    return err
}
defer logs.Close()
for {
    chunk, err := logs.Next()
    if errors.Is(err, io.EOF) {
        break
    }
    if err != nil {
        return err
    }
    if _, err := os.Stdout.Write(chunk); err != nil {
        return err
    }
}
```

The context passed to `FollowLogs` remains associated with the stream.

</div>
<div data-sdk-language="typescript">

`attach()` returns an async-iterable `ConsoleStream` of byte chunks. `logs()` returns decoded buffered text; `followLogs()` returns an async-iterable `LogStream` of decoded text chunks. Call `close()` to stop either live stream early.

```ts
const consoleStream = await sandbox.attach();
const decoder = new TextDecoder();
try {
  for await (const bytes of consoleStream) {
    console.log(decoder.decode(bytes, { stream: true }));
  }
} finally {
  consoleStream.close();
}

console.log(await sandbox.logs());
const logs = await sandbox.followLogs();
try {
  for await (const chunk of logs) {
    console.log(chunk);
  }
} finally {
  logs.close();
}
```

</div>
</div>

## Exit and error behavior

A completed command can report a nonzero exit code without any RPC failure. Check the captured result or streaming exit explicitly. An optional signal distinguishes signal termination where the SDK exposes it.

Failures before a valid result or exit frame—connection errors, protocol violations, cancellation, premature stream closure, and communication timeouts—are SDK errors rather than command exits. Streaming input is not safe to replay automatically after a disconnect because the remote side may already have processed it. Close every process or live stream when finished, and design cancellation for any background consumer.