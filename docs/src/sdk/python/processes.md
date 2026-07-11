# Processes

Use `Sandbox.run()` when the command has no interactive input and you want all of its output after it exits. Use `Sandbox.exec()` when input, incremental output, terminal resizing, or explicit process lifetime control matters. Both take either separate command arguments or one iterable of arguments.

```python
from vmon import connect

with connect() as client:
    sandbox = client.sandboxes.create(image="alpine")
    try:
        result = sandbox.run("sh", "-lc", "printf '%s' hello")
        assert result.returncode == 0
        assert result.stdout == b"hello"
        assert result.stderr == b""
    finally:
        sandbox.terminate()
```

`run(*cmd, workdir=None, env=None, timeout=None, tty=False)` returns an `ExecResult` with `returncode`, `stdout`, and `stderr`. Its output fields are bytes; decode them only when the command's encoding is known. Command-specific `env` values are added to the sandbox's bound environment, overriding same-named values.

For the sandbox lifecycle, defaults, timeouts, and errors, see [Sandboxes](sandboxes.md) and [Errors](errors.md).

## Streaming execution

`exec(*cmd, workdir=None, env=None, timeout=None, pty=False, tty=False)` returns a `Process` immediately. `pty=True` and `tty=True` are equivalent ways to request a terminal. A `Process` exposes:

- `stdin`, a `ProcessStdin`, with `write(bytes | str)` and `close()`;
- `stdout` and `stderr`, each a `ByteStream` of byte chunks;
- `wait(timeout=None)`, returning `ExecExit` with `code` and optional `signal`;
- `resize(rows, cols)` for a terminal session; and
- `close()`, which terminates an unfinished command and closes both output streams.

Send all input, then close standard input to send EOF. Closing it twice is harmless. A write after it has been closed raises a transport error.

```python
process = sandbox.exec("sh", "-lc", "read line; printf 'got:%s' \"$line\"")
process.stdin.write("message\n")
process.stdin.close()                 # EOF permits `read` and the command to finish
exit_status = process.wait(timeout=10)
output = process.stdout.read().decode("utf-8")
assert exit_status.code == 0
assert output == "got:message"
```

A `ByteStream` is iterable, or can be consumed with `read_chunk(timeout=None)` and `read(size=-1)`. `read_chunk()` returns `None` at end of stream and raises `TimeoutError` when no chunk arrives before its timeout. `read()` returns bytes; with a positive size it may wait until that many bytes are available or end-of-stream. Do not have multiple consumers read the same stream: chunks are removed as one consumer receives them.

`Process.wait()` waits for the exit frame. For a non-terminal process, it closes still-open stdin before waiting so a command waiting for EOF can finish. A terminal process does not get that automatic stdin close; write the terminal protocol you need and close stdin or otherwise end the program. If the wait times out, it terminates the current process before raising `TimeoutError`.

Output streams are closed by the SDK after the process stream ends. `Process.close()` also closes them, and `Process` is a context manager, so a `with` block has deterministic cleanup:

```python
with sandbox.exec("sh", "-lc", "printf ready") as process:
    status = process.wait(timeout=10)
    text = process.stdout.read().decode("utf-8")
assert status.code == 0
assert text == "ready"
```

## Terminals and shells

A plain `exec()` is not a terminal. Request one for programs that test for a TTY or need terminal dimensions:

```python
process = sandbox.exec("sh", pty=True, timeout=30)
try:
    process.resize(40, 120)
    process.stdin.write("printf 'terminal\\n'; exit\n")
    assert process.wait(timeout=30).code == 0
    print(process.stdout.read().decode("utf-8"), end="")
finally:
    process.close()
```

The SDK sends the terminal flag and lets the guest provide terminal behavior; it does not emulate a terminal locally. In particular, treat `stdout` and `stderr` as the two process streams exposed by the API rather than assuming an undocumented local terminal transformation.

`Client.shell(*cmd, ref=None, image=None, cpus=1, memory=512, disk_mb=1024, timeout=300)` creates a streaming shell in an ephemeral sandbox and always requests a TTY. With no command it starts `/bin/sh`. Its returned `Process` has the same input, stream, resize, wait, and close operations; `sandbox_id` identifies the ephemeral sandbox when the server announces it.

```python
with connect() as client:
    shell = client.shell(timeout=60)
    try:
        shell.stdin.write("echo hello; exit\n")
        assert shell.wait(timeout=60).code == 0
        print(shell.stdout.read().decode("utf-8"), end="")
    finally:
        shell.close()
```

## Console and logs

`Sandbox.attach()` returns a read-only `ConsoleStream`. Iterate it or call `read(size=-1)` to receive console bytes; `wait(timeout=None)` waits for the attachment to end, and `close()` stops it. It is not a `Process`: it has no stdin or resize operation.

`Sandbox.logs()` returns the buffered log text. `Sandbox.logs(follow=True)` returns a closeable `LogStream` of UTF-8-decoded text chunks (with invalid bytes replaced) for live following. Close live streams when finished, preferably with a context manager. For daemon-wide event streaming, use `Client.events()`; it is also closeable. These are streams, not captured command results.
