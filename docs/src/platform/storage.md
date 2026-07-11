# Storage and Volumes

Vibemon has two distinct storage mechanisms:

- A template's generated ext4 root filesystem is the microVM boot disk. It is
  an immutable base for the verified template.
- A named volume is a persistent host directory exported to a guest with
  virtio-fs. It is not copied into a VMM snapshot.

This distinction matters for durability, sharing, and restore planning.

## Root filesystem

For image-backed workloads, Vibemon unpacks the OCI filesystem, injects the
agent, and formats an ext4 rootfs. The boot-verified template retains that
rootfs as an immutable base. The VMM can also boot an operator-provided
virtio-blk root disk directly:

```sh
# On Linux, a direct root VMM must drop to an unprivileged sandbox identity.
sudo ./target/release/vmon vmm \
  --kernel <kernel-image> \
  --rootfs <disk.img> \
  --sandbox-uid <unprivileged-uid> \
  --sandbox-gid <unprivileged-gid> \
  --cmdline "console=ttyS0 root=/dev/vda rw"
```

See [OCI Images](images.md) for the generated-rootfs pipeline. A guest's
writable runtime changes are VMM state; they are not the same thing as a named
volume's host-directory data.

## virtio-fs host shares

The in-VMM virtio-fs device exports one host directory under a tag. The Linux
guest mounts it with the normal virtio-fs client:

```sh
# The host directory is deliberately selected by the operator.
# On Linux, a direct root VMM must drop to an unprivileged sandbox identity.
sudo ./target/release/vmon vmm \
  --kernel <kernel-image> \
  --initrd <initramfs.cpio.gz> \
  --fs-tag shared --fs-dir /path/to/share \
  --sandbox-uid <unprivileged-uid> \
  --sandbox-gid <unprivileged-gid> \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"

# In the Linux guest, after creating /mnt/shared:
mount -t virtiofs shared /mnt/shared
```

The `--fs-tag`/`--fs-dir` device is always read-only: guest writes are
rejected, but the host directory can still be changed by host processes.
Use `--volume` for a writable export, or append `:ro` to a volume specification
for a read-only export.
The VMM confines FUSE path resolution to the exported root, including checks
on canonicalized parent paths. That confinement is not a trust boundary for
untrusted guest code: a writable share lets a guest modify host-owned data
within the exported directory, and the guest can read data the host deliberately
exports. Share only a dedicated, minimally privileged directory; never mount
host secrets, source trees containing credentials, or security-sensitive host
paths into an untrusted guest.

Virtio-fs needs guest kernel support. The documented default aarch64 kernel
includes it; x86_64 Firecracker kernels and custom kernels without virtio-fs
cannot mount these shares.

## Managed S3 mounts

`vmon serve` can expose an S3 bucket or prefix at an absolute guest mountpoint
through the `s3_mounts` sandbox-create field. This is daemon orchestration, not
the direct `vmon vmm --remote-fs` interface: `vmond` validates access, starts a
per-VM host-side Unix-socket S3 proxy, and supplies that socket to the VMM as
an internal read-only remote filesystem. The guest never receives S3
credentials or a host socket path.

```json
{
  "s3_mounts": {
    "/mnt/assets": {
      "uri": "s3://example-assets/public",
      "endpoint": "https://s3.example.invalid",
      "region": "us-east-1",
      "read_only": true
    }
  }
}
```

Mountpoints must be absolute. A mount source is `s3://bucket[/prefix]`; an
endpoint and region are optional. Before the sandbox starts, the daemon probes
the bucket with a one-key ListObjectsV2 request, so invalid URI, configuration,
credentials, endpoint, or bucket access fails creation instead of surfacing as
a delayed guest mount failure. A sandbox accepts at most **8** S3 mounts.

With `read_only: true`, the agent mounts the proxied virtio-fs layer directly
and guest writes fail. With the default `read_only: false`, it mounts a
guest-side overlayfs whose lower layer is the same read-only S3 filesystem.
Guest changes remain guest-local: they are never uploaded or synchronized back
to S3. The overlay's `upper` and `work` files live under the guest rootfs, so
they can persist through a VMM snapshot or fork when that guest disk is
captured; they are not durable S3 storage.

For an authenticated source, provide both `access_key` and `secret_key` in the
create request (with optional `session_token`), or configure the daemon with
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (and optionally
`AWS_SESSION_TOKEN`). Supplying only part of an inline key pair is rejected;
without either complete source the request is anonymous. Inline credentials
are request-bound and are redacted from serialization and debug output. Do not
put credentials in examples, sandbox metadata, or ordinary environment values
visible to a guest.

## Named volumes

A named volume is a persistent host directory created under
`$VMON_HOME/volumes/<name>` and attached with virtio-fs. Names are 1–64
characters and must match:

```text
^[a-z0-9_][a-z0-9_.-]{0,63}$
```

Vibemon creates the volume root and directory with mode `0700`; it rejects
symlinks and non-directory volume paths. The gRPC `VolumeService` lists,
creates, and deletes volume names. A delete is refused while its volume is
attached. Although the protobuf service description uses the broad term
"persistent storage," the current implementation backing a named volume is a
host directory, not a portable block-volume format.

The low-level VMM syntax demonstrates writable and read-only volume exports:

```sh
sudo ./target/release/vmon vmm \
  --kernel <kernel-image> \
  --initrd <initramfs.cpio.gz> \
  --volume data:/var/lib/vmon-volumes/data \
  --volume cache:/srv/cache:ro \
  --sandbox-uid <unprivileged-uid> \
  --sandbox-gid <unprivileged-gid> \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"
```

The VMM volume tag accepts `[a-z0-9_]{1,32}`. The right-hand directory is a
host path; make it a private directory whose ownership and permissions are
appropriate for the VMM process and its guest workload.

### Writer safety

When `vmon serve` grants a writable named-volume mount, its `vmond` process
takes a non-blocking, host-local exclusive file lock and releases it when the
holder is dropped. This prevents concurrent writable mounts coordinated by that
host's `vmon serve` process; it is not a distributed filesystem protocol, a
backup, or a replacement for application-level consistency. A direct
`vmon vmm --volume` launch does not take this lock, so its operator must enforce
writer exclusivity. Plan one writer, use read-only mounts for consumers, and
stop/quiesce an application before taking an application-consistent copy of its
data.

## Snapshots and volume data

A VMM snapshot saves CPU, RAM, and device state. It records the virtio-fs
attachment information and mode, but it does **not** copy named-volume
contents. On restore or fork through the SDK/control plane, named volumes are
re-attached by name to their local host path. Consequently:

- a snapshot is not a backup of a volume;
- restoring a snapshot can observe the volume's current contents, not its
  contents at snapshot time;
- a fork whose volume is writable needs the same single-writer discipline; and
- moving or restoring a snapshot elsewhere does not imply the named-volume
  data is present or compatible there.

`SnapshotFs` is a separate gRPC operation: it requests a filesystem freeze and
creates a filesystem-level snapshot on disk. Do not confuse it with
`Snapshot`, which captures VMM memory, registers, and device state. For
lifecycle operations and VMM snapshot compatibility, see
[Snapshots, Restore, and Fork](snapshots.md).
