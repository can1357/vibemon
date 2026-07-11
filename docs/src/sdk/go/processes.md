# Processes

The Go SDK offers two execution modes against an existing daemon sandbox:

- `sandbox.Run` is **captured execution**: one unary request returns after the command has finished with all captured stdout, stderr, and the exit code.
- `sandbox.Exec` is **streaming execution**: it opens a live bidirectional process session. Read `ExecEvent` values while the process runs, write stdin, optionally resize a TTY, and close the session when finished.

The SDK also provides `client.Shell` for a shell session, `sandbox.Attach` for a read-only serial console, and sandbox log retrieval. These are remote API clients; they do not start a local runtime. For client setup and shared failure behavior, see [Connect](connect.md), [Connection Strings and Contexts](../connection-strings.md), and [Error Codes](../../reference/errors.md).

## Captured commands

Pass a non-empty argument vector in `vmon.ExecRequest.Command`; the SDK does not invoke a shell for you. `Workdir`, `Env`, `Timeout`, and `TTY` are passed to the server. `Timeout` is a pointer to a server-side timeout value in seconds.

`Run` returns only after execution completes. Its `ExecResult` contains `ExitCode`, `Stdout`, and `Stderr`; a nonzero exit code is represented in `ExitCode`, not automatically converted into a Go error.

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

Use captured execution for a bounded command result or a readiness check. It is not the streaming API: output is returned in the final `ExecResult`, so it is not available incrementally. Context cancellation ends the in-flight request.

## Streaming commands

`Exec` opens a live `*vmon.Process` in the selected sandbox. Call `Receive` repeatedly until it returns an event whose `Exit` is non-`nil`. Output events have `Stream` set to `vmon.StreamStdout` or `vmon.StreamStderr` and bytes in `Data`; the final event has the `ExecExit` in `Exit`, including its code and an optional terminating signal.

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
    }
    if err != nil {
        return err
    }
}
```

`Receive(ctx)` blocks for one frame. Its supplied context controls that wait: canceling it closes the session and returns the context error. It returns `io.EOF` if the stream ends without another event. Treat an unrecognized frame or stream as a protocol error rather than assuming it is output.

For the common case of copying output, `Copy` loops over `Receive` and sends stdout and stderr to separate `io.Writer` values until exit. `Wait` is equivalent to copying both streams to `io.Discard`.

```go
process, err := sandbox.Exec(ctx, vmon.ExecRequest{
    Command: []string{"make", "test"},
})
if err != nil {
    return err
}
defer process.Close()

exit, err := process.Copy(ctx, os.Stdout, os.Stderr)
if err != nil {
    return err
}
if exit.Code != 0 {
    return fmt.Errorf("make test exited with %d", exit.Code)
}
```

### Stdin and TTYs

`WriteStdin` sends one byte chunk to the remote process. `CloseStdin` sends EOF; after it succeeds, later stdin writes fail locally. When `TTY` is true, `Resize` sends rows and columns for that terminal.

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
_, err = process.Wait(ctx)
return err
```

`Process.Close` cancels the process session and releases its resources. It is safe to defer immediately after a successful `Exec` or `Shell`, including after the process has reported its exit. Closing is not an additional graceful stdin EOF: call `CloseStdin` when the program needs EOF, and use `Close` to end the client session. A context canceled during `WriteStdin`, `CloseStdin`, or `Resize` aborts that frame operation and closes the process session.

## Shell sessions

`client.Shell` opens a live shell session. A `vmon.ShellRequest` can refer to an existing sandbox, template, or image with `Reference`, or choose an ephemeral image explicitly with `Image`; it also accepts `Command`, CPU, memory, disk, and timeout values. The client consumes the server's ready frame before returning and sets `process.SandboxID` to the ready sandbox identifier.

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

A shell uses the same `Process` receive, stdin, resize, copy, wait, and close APIs as `Exec`. Its setup call consumes the ready frame; consumers should begin with process output, not try to receive a ready event themselves.

## Console and logs

`Attach` opens a read-only `*vmon.ConsoleStream` for the live sandbox serial console. `Receive(ctx)` returns `StreamEvent` values whose stream is `vmon.StreamConsole`; it honors the context passed to each receive. Always close the stream when stopping consumption.

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
    if _, err := os.Stdout.Write(event.Data); err != nil {
        return err
    }
}
```

`Logs(ctx)` gathers the currently available log chunks into a single string. `FollowLogs(ctx)` instead returns a `*vmon.LogStream`; call `Next` to receive each chunk. `Next` returns `io.EOF` at the natural end. `LogStream.Close` cancels the underlying stream, so defer it once the stream is open.

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

The context passed to `FollowLogs` remains associated with the follow stream; cancel it or call `Close` to stop following. A log, console, or process stream is a resource: do not leave it blocked in a background goroutine without a cancellation path.