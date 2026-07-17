# Low-Level VMM

`vmon vmm` starts one bare virtual-machine monitor directly. It is the escape hatch below the managed [`vmon` CLI](cli.md) and [`vmon serve`](server.md): it does not create a server sandbox record and it does not expose the `vmon.v1` gRPC API. The monitor uses KVM on Linux, Hypervisor.framework on Apple Silicon macOS, and WHP on x86_64 Windows; the backend is selected at build time.

This command needs a compatible host, guest assets for the host architecture, and sufficient privilege for requested host facilities. Linux root launches with the default sandbox must supply `--sandbox-uid` and `--sandbox-gid`. macOS and Windows can use `--net user` for outbound NAT without an operator-created TAP device.

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

On macOS/HVF or Windows/WHP, use user-mode NAT:

```sh
vmon vmm \
  --kernel /path/to/kernel --initrd /path/to/initramfs.cpio.gz \
  --net user \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"
```

`--net` accepts only `user`. Other attachments include `--mac ADDR`, `--fs-tag TAG --fs-dir DIR`, repeatable `--volume TAG:HOST_DIR`, `--rng`, and `--console-agent`. `--agent-sock PATH` bridges the host to the virtio-console agent through a Unix socket on Linux/macOS or a local named pipe on Windows.

### NVIDIA SR-IOV vGPU passthrough

On x86_64 Linux/KVM, `--vfio-gpu` assigns a pre-created NVIDIA SR-IOV virtual function through NVIDIA's vendor-specific VFIO cdev interface. The host must run NVIDIA vGPU Manager, enable IOMMU and SR-IOV, create the virtual function, select a nonzero `current_vgpu_type`, and expose its `vfio-dev` cdev. Whole physical GPUs, legacy `vfio-pci` group devices, and mdev devices are not accepted.

```sh
sudo vmon vmm \
  --kernel /path/to/kernel \
  --rootfs /path/to/disk.img \
  --vfio-gpu /sys/bus/pci/devices/0000:3d:00.4 \
  --vm-uuid 12345678-9abc-def0-1122-334455667788 \
  --sandbox-uid 65534 --sandbox-gid 65534 \
  --cmdline "console=ttyS0 root=/dev/vda rw"
```

The launcher must be able to open `/dev/iommu` and the function's VFIO cdev before dropping privileges. Repeat `--vfio-gpu` to assign up to eight functions. `--vm-uuid` is required, must be non-nil, and should remain stable for NVIDIA guest licensing. The option is independent of `--transport`, which controls only virtio devices.

GPU assignment supports fresh boots only. Restore, fork, lazy memory paging, pause, and snapshot are rejected because VFIO device state is not serializable. Install an NVIDIA guest driver compatible with the host vGPU Manager release.

### Proxy-backed remote filesystems

`--remote-fs <tag>:<endpoint>` is a repeatable, direct VMM attachment for a read-only virtio-fs filesystem served by an operator-managed proxy. The endpoint is a Unix socket on Linux/macOS and a local named-pipe-derived path on Windows. The tag must match `[a-z0-9_]{1,32}` and share the same namespace as `--volume` tags. The endpoint path must be absolute.

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
