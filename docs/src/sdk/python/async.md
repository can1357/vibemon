# Async API

The Python SDK keeps its synchronous object hierarchy as the primary API. A
bound `Sandbox` and its `files` and `ports` helpers expose thread-backed `.aio`
facades for use from an event loop. Those facades run the corresponding
synchronous operation in a worker thread, so the operation's arguments,
result, and error behavior remain those of the synchronous method.

The root `Client` and its `SandboxAPI` (`client.sandboxes`) do not have `.aio`
facades. Run their blocking operations with `asyncio.to_thread`:

```python
import asyncio
import vmon

async def main() -> None:
    client = vmon.connect()
    try:
        sandbox = await asyncio.to_thread(
            client.sandboxes.create, image="alpine:latest"
        )
        try:
            exit_result = await sandbox.aio.run("sh", "-lc", "printf hello")
            assert exit_result.returncode == 0
        finally:
            await sandbox.aio.terminate()
    finally:
        await client.aclose()

asyncio.run(main())
```

Do not call a blocking root operation directly from an async request handler:
it blocks the event-loop thread. The sandbox `.aio` facades do not add
cancellation, transaction, or retry guarantees beyond the daemon operation
they delegate to.

## Durable function facades

Durable functions expose an asynchronous `.aio` facade:

```python
import vmon

@vmon.function
async def double(value: int) -> int:
    return value * 2

async def use_function() -> None:
    call = await double.aio.spawn(21)
    assert await call.get(timeout=30) == 42
```

The async call handle returned by `await function.aio.spawn(...)` provides
`await get(timeout=None)` and `await cancel(reason="cancelled by client")`.

The callable's own shape still matters:

| Local callable | Synchronous API | `.aio` API |
| --- | --- | --- |
| ordinary function | `remote()` | `await remote()` |
| coroutine function | `remote()` | `await remote()` |
| generator function | `remote_gen()` | `async for value in remote_gen()` |
| async generator function | `remote_gen()` | `async for value in remote_gen()` |

Using `remote()` for a generator or `remote_gen()` for a unary function raises
`ValueError` before a wrong call kind is submitted.
