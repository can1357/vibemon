# Snapshots, Restore, and Fork

Vibemon exposes two different snapshot operations. Select the one that matches
the state you need; neither is a substitute for the other.

| Operation | gRPC operation | Captures | Does not promise |
| --- | --- | --- | --- |
| VMM snapshot | `SandboxService.Snapshot` | Paused microVM memory, registers, machine state, and serialized device state. | A copy of external host data, such as named-volume contents or live host network connections. |
| Filesystem snapshot | `SandboxService.SnapshotFs` | Requests a filesystem freeze and creates a filesystem-level snapshot on disk. | CPU/RAM/device state or a resumable microVM image. |

Both operations require a running sandbox. `Snapshot` accepts an optional name
and a `stop` flag; setting `stop` stops the microVM immediately after capture.
`SnapshotFs` also accepts an optional name. The gRPC API is authoritative;
these operations replace older HTTP lifecycle routes.

## VMM snapshot contents

On disk, a VMM snapshot directory has a `current-generation` manifest that
selects matching state and RAM files:

- `vmstate.<generation>.bin` is postcard-encoded machine state; and
- `memory.<generation>.bin` is raw guest RAM in memory-slot order.

The state envelope contains the snapshot format version, architecture,
hypervisor backend, memory size and regions, vCPU and machine state, serial
state, boot configuration, and virtio device state. It includes sufficient
backend hints to reopen block, TAP, user-mode networking, and virtio-fs
backends. That is metadata and device state, not a transfer of arbitrary host
resources.

A snapshot may be full or delta-based. A delta stores changed 4 KiB RAM pages
relative to a sibling base snapshot and has a validated base-name reference.
Restore reconstructs the chain, with a maximum chain depth of 64. Keep every
base snapshot directory: deleting or changing a base breaks its dependent
deltas. Remote lazy page-in accepts full snapshots only; it does not accept
snapshot deltas.

### Low-level example

The direct VMM control socket can pause, snapshot, and resume a VM:

```sh
# Linux default-sandboxed root launch. Run this from a non-root account that
# can read the guest assets and read/write the snapshot root; the substitutions
# make the VMM drop to that account after sudo starts it.
sudo ./target/release/vmon vmm \
  --sandbox-uid "$(id -u)" \
  --sandbox-gid "$(id -g)" \
  --kernel <kernel-image> \
  --initrd <initramfs.cpio.gz> \
  --api-sock /tmp/vmon/control.sock \
  --snapshot-root /tmp/vmon-snapshots \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"

# After the VMM has emitted its JSON banner on the socket:
printf '%s\n' \
  '{"id":1,"method":"pause","params":{}}' \
  '{"id":2,"method":"snapshot","params":{"name":"demo"}}' \
  '{"id":3,"method":"resume","params":{}}' \
  | socat - UNIX-CONNECT:/tmp/vmon/control.sock

sudo ./target/release/vmon vmm --sandbox-uid "$(id -u)" --sandbox-gid "$(id -g)" --restore /tmp/vmon-snapshots/demo
sudo ./target/release/vmon vmm --sandbox-uid "$(id -u)" --sandbox-gid "$(id -g)" --fork-from /tmp/vmon-snapshots/demo --count 4
```

This direct example uses an operator-managed snapshot root and `socat`. The
server and SDKs instead use `SnapshotService`: `List` enumerates registered
snapshots, `Restore` restores a named snapshot with a JSON configuration body,
and `Fork` creates one or more copy-on-write instances with a JSON
configuration body.

## Restore and fork

A restore creates a microVM instance from an existing snapshot. A fork creates
one or more instances from the same snapshot and uses copy-on-write sharing for
high-density startup. Both reject an unknown snapshot; fork also validates its
requested count and target configuration.

Before restoring, identify external dependencies that the snapshot refers to:

| Dependency | Restore behavior | Operational action |
| --- | --- | --- |
| Named virtio-fs volume | The mount is re-attached by name to its local host directory; its data was not saved in the VMM snapshot. | Ensure the required local volume exists and control writable access. |
| Ordinary virtio-fs host share | The snapshot retains the share path, tag, and read-only mode; its host data is external. | Ensure the path is present and safe on the restore host. Do not assume a file-consistent point-in-time copy. |
| Linux TAP | The snapshot carries the TAP backend hint and MAC; live connectivity still depends on the named host TAP and its topology. | Recreate/maintain the operator-managed TAP environment. |
| macOS or Windows user-mode NAT | Guest-visible libslirp state is serialized. | Expect in-flight host TCP connections to reset; new guest outbound connections can be opened after restore. |
| Secret environment | A full snapshot preserves guest RAM, including secret bytes that remain after a process exits. Live mesh migration also forwards the runtime secret environment over the authenticated peer channel. | Treat snapshots and replicas as secret-bearing data. Local snapshot metadata does not separately persist secret values; supply explicit restore or fork secrets when future exec calls need the environment binding. |
| Managed S3 mount | The daemon writes credential-free S3 mount metadata (`uri`, endpoint, region, read-only setting, generated tag, and credential provenance) into `s3-mounts.json` beside the snapshot. On restore or fork, it recreates a per-VM Unix socket on Linux/macOS or named-pipe endpoint on Windows using the restoring daemon's credentials; the live endpoint is not snapshotted. Writable guest overlays remain part of the captured guest disk. | Make the source available again. The snapshot never contains access keys, secret keys, or session tokens. Restore re-probes access and fails if credentials or access are unavailable. Guest overlay writes remain guest-local and are never synchronized to S3. |

## Compatibility rules

The current VMM snapshot format version is **3**. Version 3 added serialized
libslirp user-network state. Older snapshots are rejected and must be
recaptured; newer versions are also rejected. The restore implementation also
requires all of the following:

1. the snapshot architecture equals the restoring binary's architecture
   (`x86_64` or `aarch64`);
2. the snapshot hypervisor backend equals the current backend (KVM snapshots
   restore on KVM builds; HVF snapshots restore on HVF builds);
3. snapshot metadata, RAM-region layout, serialized device/backend pairing,
   queues, and delta metadata validate; and
4. for a delta snapshot, every layer has the same RAM layout and a valid base
   chain no deeper than 64 layers.

Cross-hypervisor and cross-architecture restore are unsupported. A migration
RPC exists (`SandboxService.Migrate` takes a sandbox ID and destination), but
it is not a guarantee that a snapshot can be restored across architectures,
hypervisor backends, or differing host resources. Treat migration as
control-plane state transfer subject to the same snapshot compatibility and
external-dependency constraints; do not rely on cross-backend restore.

## Templates

An image template is a boot-verified VMM snapshot plus its immutable ext4 base
rootfs. Template identity incorporates image and agent content plus the
memory, CPU, virtio-fs-slot, host-share, NIC, and TAP-slot configuration used
at template creation. This prevents a template prepared for one device shape
from silently standing in for another.

Templates are indexed by a SHA-256 digest of their bootable content. The index
is a local pointer to a live template directory; it is not a portable image
registry. Rebuild or recapture a template when its kernel, agent, image, or
machine/device shape changes. For the OCI-to-template process, see
[OCI Images](images.md); for persistent data outside template and VMM state,
see [Storage and Volumes](storage.md).
