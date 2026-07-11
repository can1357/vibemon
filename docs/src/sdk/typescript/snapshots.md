# Snapshots

Snapshots belong to the daemon reached by the TypeScript client. The SDK is a client for a running `vmon serve` API; it does not create a local VMM or run a server.

## Capture a sandbox

A `Sandbox` can produce a full VM snapshot. The returned string is the snapshot name to use with the collection API. Pass `true` as the second argument only when the daemon should stop the sandbox as part of capture.

```ts
import { connect } from "@vmon/sdk";

const client = connect();
const sandbox = await client.sandboxes.create({ image: "alpine:latest" });
const snapshotName = await sandbox.snapshot("build-ready");
```

For a filesystem-only capture, call `snapshotFilesystem`. It returns the daemon's image reference rather than a full VM snapshot name:

```ts
const image = await sandbox.snapshotFilesystem("source-tree");
```

Use the [platform snapshot guide](../../platform/snapshots.md) for the server-side lifecycle and snapshot semantics.

## List and restore

`client.snapshots.list()` returns snapshot names. `restore(name, request?)` creates and returns a new `Sandbox` from a full snapshot.

```ts
const names = await client.snapshots.list();
const restored = await client.snapshots.restore("build-ready", {
  name: "worker-1",
  env: { JOB: "compile" },
  timeout_secs: 300,
});

console.log(names, restored.id);
```

The optional restore body is `RestoreRequest`: every `SandboxCreateRequest` field is optional, plus `agent?: boolean | null`. This lets a restore supply supported creation settings such as `name`, `env`, `cpus`, `memory`, `volumes`, `tags`, or `timeout_secs`. It is sent to the daemon as JSON; validation and the resulting sandbox state are daemon-owned.

### S3-backed snapshots

If the captured sandbox was created with `s3_mounts`, the daemon records the
mount configuration with the snapshot (and with a filesystem image created
from that snapshot). On `restore`, it reconstructs those recorded mounts; do
not use the inherited `s3_mounts` field in `RestoreRequest` as a replacement or
override. The stored configuration includes the URI, endpoint, resolved
region, read-only setting, and credential mode, never inline credential
values.

An S3 mount that originally used inline credentials or the daemon's AWS
credential environment requires the restoring daemon to provide
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (and, when applicable,
`AWS_SESSION_TOKEN`). Anonymous mounts need no credentials. A snapshot records
remote mount configuration, not S3 data or secrets; volatile guest-overlay
writes are not persisted to S3. `fork` does not reconstruct S3 mounts from the
snapshot, so do not rely on its inherited `s3_mounts` request field.

## Fork many sandboxes

`client.snapshots.fork(name, request)` restores several independent sandboxes from one snapshot and returns `Promise<Sandbox[]>`. Its `ForkRequest` requires `count` and accepts the same optional sandbox creation fields as restore.

```ts
const workers = await client.snapshots.fork("build-ready", {
  count: 4,
  tags: { role: "builder" },
  timeout_secs: 300,
});

for (const worker of workers) {
  console.log(worker.id);
}
```

A template name may be supplied through the `template` field when creating, restoring, or forking because it is part of `SandboxCreateRequest`; it is not a separate TypeScript template runtime. See [Sandboxes](sandboxes.md) for sandbox creation and [Snapshots, Restore, and Fork](../../platform/snapshots.md) for the daemon feature.
