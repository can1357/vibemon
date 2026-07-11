# Low-Level VMM

`vmon vmm` starts one bare virtual-machine monitor directly. It is the escape hatch below the managed [`vmon` CLI](cli.md) and [`vmon serve`](server.md): it does not create a server sandbox record and it does not expose the `vmon.v1` gRPC API. The monitor uses KVM on Linux and Apple Hypervisor.framework on Apple-silicon macOS; the backend is selected at build time.

This command needs a compatible host, a kernel and guest image built for the host architecture, and sufficient privilege for the requested host facilities. On Linux, a root launch with the default sandbox must also supply `--sandbox-uid` and `--sandbox-gid` (both greater than zero); the examples use the `nobody` identity `65534:65534`, which must be able to read the supplied files. On macOS, `--net user` is the entitlement-free NAT choice; host TAP/vmnet networking requires the appropriate host support and entitlement. Run [Installation](installation.md) and [Troubleshooting](troubleshooting.md) guidance before using it in production.

## Direct boot

A direct boot requires `--kernel`; an initramfs or a root filesystem supplies the guest userspace. The default transport is `mmio`.

```sh
# initramfs guest
sudo vmon vmm \
  --kernel /path/to/kernel \
  --initrd /path/to/initramfs.cpio.gz \
  --sandbox-uid 65534 --sandbox-gid 65534 \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"

# virtio-blk root filesystem, visible in the guest as /dev/vda
sudo vmon vmm \
  --kernel /path/to/kernel \
  --rootfs /path/to/disk.img \
  --sandbox-uid 65534 --sandbox-gid 65534 \
  --cmdline "console=ttyS0 root=/dev/vda rw"
```

Add `--rootfs-ro` to open the disk read-only. `--mem MIB` (default 256) selects guest RAM and `--cpus N` selects vCPUs. `--timeout-secs N` makes the monitor end after 1 through 86,400 seconds. `--transport pci` is available only on x86_64; use it when the guest needs PCI virtio devices:

```sh
sudo vmon vmm \
  --kernel /path/to/kernel --initrd /path/to/initramfs.cpio.gz \
  --transport pci \
  --sandbox-uid 65534 --sandbox-gid 65534 \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"
```

## Networking and guest devices

On Linux/KVM, attach an operator-created TAP interface with `--tap`:

```sh
sudo vmon vmm \
  --kernel /path/to/kernel --initrd /path/to/initramfs.cpio.gz \
  --tap tap0 \
  --sandbox-uid 65534 --sandbox-gid 65534 \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"
```

On macOS/HVF, use user-mode NAT without the VM-networking entitlement:

```sh
vmon vmm \
  --kernel /path/to/kernel --initrd /path/to/initramfs.cpio.gz \
  --net user \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"
```

`--net` accepts only `user`. Other available direct attachments are `--mac ADDR`, read-only `--fs-tag TAG --fs-dir DIR`, repeatable `--volume TAG:HOST_DIR` (append `:ro` for a read-only volume), `--rng`, and `--console-agent`. `--agent-sock PATH` bridges a host Unix socket to the virtio-console agent; `--agent-exec CMD` runs the command through that agent after boot. A cold boot using `--agent-exec` requires `--agent-sock`.

### Proxy-backed remote filesystems

`--remote-fs <tag>:<absolute-socket>` is a repeatable, direct VMM attachment for
a read-only virtio-fs filesystem served by an operator-managed Unix-socket
proxy. It is **not** an S3 client and does not create a proxy. The tag is what
the guest mounts; it must match `[a-z0-9_]{1,32}` and share the same namespace
as `--volume` tags, so every volume and remote-filesystem tag in one VM must be
unique. The socket path must be absolute.

```sh
sudo vmon vmm \
  --kernel /path/to/kernel --initrd /path/to/initramfs.cpio.gz \
  --remote-fs assets:/run/object-proxy/assets.sock \
  --sandbox-uid 65534 --sandbox-gid 65534 \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"

# In the guest:
mount -t virtiofs assets /mnt/assets
```

The device supports only lookup, listing, metadata, and reads. Opens with
write intent and filesystem-changing operations fail with `EROFS`; it never
writes through the socket proxy. The VMM connects lazily when the guest issues
a request. It retries a failed socket connection or proxy exchange once, then
reports `EIO` to the guest; restoring a snapshot likewise reconnects on the
next request rather than preserving a live socket connection. Start and keep
the proxy available for the full period in which the guest needs the mount.

Under `--jail`, the VMM binds the configured socket into the jail. Some kernels
reject a Unix-socket inode bind mount, in which case the jailer binds the
socket's parent directory instead. That fallback exposes the parent directory
inside the jail, so use a dedicated, private socket directory with no other
sensitive entries. Landlock and ordinary filesystem permissions must permit
the VMM to traverse the socket parent and connect to the socket; choose the
path and ownership accordingly.

## UEFI boot

UEFI mode replaces a direct kernel with operator-provided firmware and a bootable disk. Use firmware built for the host architecture. The following x86_64 example uses PCI transport:

```sh
sudo vmon vmm \
  --boot-mode uefi \
  --firmware /path/to/OVMF_CODE.fd \
  --rootfs /path/to/uefi-bootable-disk.img \
  --sandbox-uid 65534 --sandbox-gid 65534 \
  --transport pci
```

On aarch64, provide an aarch64 firmware image (for example, a `QEMU_EFI.fd` build) and keep the default `mmio` transport. Snapshot restore and fork are backend- and architecture-specific; do not use an image captured by one backend or architecture on another.

## Control socket

`--api-sock PATH` enables a Unix-domain-socket lifecycle protocol. Add `--snapshot-root DIR` when named snapshots should be available. The socket serves newline-delimited UTF-8 JSON: the server writes a banner first, then each request must contain an unsigned integer `id`, a `method`, and a `params` object. Each response is a JSON line with that `id`, `ok`, and either `result` or an `error` object.

```sh
sudo vmon vmm \
  --kernel /path/to/kernel --initrd /path/to/initramfs.cpio.gz \
  --api-sock /tmp/vmon/control.sock \
  --snapshot-root /tmp/vmon-snapshots \
  --sandbox-uid 65534 --sandbox-gid 65534 \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"

printf '%s\n' \
  '{"id":1,"method":"pause","params":{}}' \
  '{"id":2,"method":"snapshot","params":{"name":"before-change"}}' \
  '{"id":3,"method":"resume","params":{}}' \
  '{"id":4,"method":"quit","params":{}}' \
  | socat - UNIX-CONNECT:/tmp/vmon/control.sock
```

The supported verbs are:

| Method | Required parameters | Effect |
| --- | --- | --- |
| `ping` | none | Returns a liveness reply. |
| `info` | none | Returns monitor information. |
| `pause` | none | Pauses the VM. |
| `resume` | none | Resumes the VM. |
| `snapshot` | `name` string | Creates a named snapshot; optional `base` string and `track` boolean are accepted. Requires `--snapshot-root` and a paused VM. |
| `quit` | none | Stops the monitor. |
| `metrics` | none | Returns monitor metrics. |
| `extend` | `secs` unsigned number | Extends the lifecycle deadline. |

A control connection begins with a banner such as `{"vmm":"VERSION","api":1}`. Requests longer than 65,536 bytes and non-UTF-8 request lines are rejected. Keep the socket inaccessible to untrusted local users.

## Restore and fork

A restore or fork starts from a snapshot directory, so it does not take `--kernel`:

```sh
sudo vmon vmm --restore /tmp/vmon-snapshots/before-change --sandbox-uid 65534 --sandbox-gid 65534
sudo vmon vmm --fork-from /tmp/vmon-snapshots/before-change --count 4 --sandbox-uid 65534 --sandbox-gid 65534
```

`--fork-from` creates copy-on-write children; `--count` defaults to 1. `--disk-overlay-of BASE --rootfs NEW` creates a new no-overwrite copy-on-write disk overlay for a direct boot. For server-managed snapshots, restores, and forks, use the control-plane commands documented in [Snapshots, Restore, and Fork](snapshots.md) instead.
