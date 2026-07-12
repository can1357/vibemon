# Stateful Actors and Apps

`@vmon.cls` defines a Python durable actor: a server-backed class definition
whose actor instances have durable IDs and exported method calls. This is a
Python source-packaged feature, not a portable class interface. Read
[Durable Functions](../functions.md) for Python packaging and trusted
serialization, and [Shared Concepts](../shared-concepts.md) for daemon-owned
durability and execution semantics.

## Define an actor

Decorate the class with `@vmon.cls` and each remotely callable instance method
with `@vmon.method`. At least one method must be exported. Constructing the
remote class creates an actor and returns a `RemoteObject`; calling an exported
method with `remote()` creates and waits for its durable method call.

```python
import vmon

with vmon.connect() as client:
    @vmon.cls(client=client)
    class Counter:
        def __init__(self, initial: int = 0):
            self.value = initial

        @vmon.method
        def increment(self, amount: int = 1) -> int:
            self.value += amount
            return self.value

        @vmon.method
        def read(self) -> int:
            return self.value

    counter = Counter(10)
    assert counter.increment.remote(2) == 12
    assert counter.read.remote() == 12
    print(counter.id)

    # RemoteObject.from_id reconnects to an existing durable actor.
    same_counter = Counter.from_id(counter.id)
    assert same_counter.read.remote() == 12
```

The SDK does not make actor recovery transparent. Only `failed` and `deleted`
actors are terminal and raise `ActorLostError`; it does not replay the
constructor. A stopped actor remains recoverable, for example by restoring an
existing checkpoint. Persist business state deliberately and decide whether
creating or restoring an actor is correct for the application.

## Lifecycle hooks and snapshots

Lifecycle hooks are declared on the class and dispatched in definition order:

* `@vmon.enter` runs initialization. `@vmon.enter(snapshot=True)` is marked as
  a snapshot-enter hook.
* `@vmon.exit` runs graceful shutdown handling.
* `@vmon.on_restore` runs after a snapshot restore.

```python
import vmon

with vmon.connect() as client:
    @vmon.cls(client=client, snapshot_after_initialize=True)
    class Cache:
        def __init__(self, key: str):
            self.key = key
            self.ready = False

        @vmon.enter
        def open(self) -> None:
            self.ready = True

        @vmon.on_restore
        def reconnect_after_restore(self) -> None:
            # Recreate connections or other process-local resources here.
            self.ready = True

        @vmon.exit
        def close(self) -> None:
            self.ready = False

        @vmon.method
        def status(self) -> bool:
            return self.ready
```

Hooks communicate lifecycle intent to the daemon; they do not guarantee that a
Python object, network connection, or external side effect can be restored
safely. In particular, a post-restore hook should rebuild process-local
resources rather than assuming a serialized connection remains valid.

An actor handle supports `checkpoint()`, `restore(checkpoint)`, and
`fork(checkpoint=None, labels=None, request_id=None)`. A checkpoint is an
immutable `ActorCheckpoint` with its own ID, actor ID, sequence, and creation
time. `restore()` atomically restores the same actor ID; `fork()` creates a
separate actor identity from a checkpoint. `delete()` permanently deletes the
actor. Treat all of these as remote state transitions, not as Python object
copying.

## Services versus actors

`@vmon.service` uses the same exported-method style for horizontally scalable,
non-durable instances. A service proxy has no durable actor ID and cannot use
`from_id`, checkpoints, restores, forks, or deletes. Its constructor
information is carried with service method calls. Choose `@vmon.cls` when the
server must retain a durable actor identity; choose `@vmon.service` only for
stateless/scalable service behavior.

## Group definitions with `App`

An `App` is optional. Functions and remote classes work without one. Adding
definitions creates unique application-local bindings, and `deploy()` registers
all definitions then atomically activates the complete manifest.

```python
import vmon

with vmon.connect() as client:
    app = vmon.App("billing", client=client)

    @app.function
    def invoice_total(cents: int, tax_cents: int) -> int:
        return cents + tax_cents

    @app.include(name="counter")
    @vmon.cls(client=client)
    class InvoiceCounter:
        def __init__(self) -> None:
            self.count = 0

        @vmon.method
        def increment(self) -> int:
            self.count += 1
            return self.count

    revision = app.deploy()
    print(revision.revision_id)
```

`App.add(definition, name=...)` also adds an already decorated function or
class; duplicate binding names raise `ValueError`. `manifest()` registers the
definitions and returns the complete pinned `AppManifest`. `get()` resolves the
current revision or a supplied revision ID. `rollback(target, ...)` atomically
selects a prior immutable app revision and accepts an optional
`expected_current` guard.

## Server-owned schedules

`App.create_schedule()` creates or replaces a daemon-owned schedule pinned to
an app revision and the exact function revision in that manifest. Pass an
app-local function name or `AppBinding`, a `Cron` (five-field expression plus
IANA time zone), or a positive `Period`.

```python
from datetime import timedelta
import vmon

with vmon.connect() as client:
    app = vmon.App.from_name("billing", client=client)
    current = app.get()
    schedule = app.create_schedule(
        "hourly-invoices",
        "invoice_total",
        vmon.Period(timedelta(hours=1), anchor_unix_millis=0),
        input={"cents": 100, "tax_cents": 10},
        app_revision=current,
    )
```

Use `get_schedule()`, `list_schedules(page_size=..., page_token=...)`, and
`delete_schedule()` to manage schedules. The schedule's durable identity and
revision pinning belong to the server; a client process need not remain alive
for it to run.
