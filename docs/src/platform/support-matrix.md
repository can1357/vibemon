# Support Matrix

Vibemon runs Linux guests on either Linux/KVM or Apple Silicon macOS/HVF. The table distinguishes the host platform that runs `vmon` from the guest that the VMM boots.

| Area | Supported configuration | Operational constraint |
| --- | --- | --- |
| Linux host | Linux with a present, readable, and writable `/dev/kvm` | KVM is the local hypervisor. `vmon doctor` reports a hard failure when `/dev/kvm` is absent or inaccessible. The diagnostic recommends enabling KVM and adding the operator to the `kvm` group, then logging in again. |
| macOS host | macOS 15 or later on Apple Silicon with Hypervisor.framework support | HVF is used natively. The executable must be ad-hoc signed with the `com.apple.security.hypervisor` entitlement before it can run. |
| Host CPU architecture | `x86_64` and `aarch64` | Linux guest architecture follows the host hypervisor architecture. |
| macOS/HVF guest architecture | `aarch64` Linux only | macOS/HVF is not an x86_64 guest path. |
| Guest operating system | Linux | Direct-kernel boot and operator-supplied UEFI firmware are supported. Non-Linux guests are not a target. |
| x86_64 direct kernel | Uncompressed ELF `vmlinux` or `bzImage` | `vmon` loads these formats directly. |
| aarch64 direct kernel | Uncompressed `Image` | The demo can extract `Image` from a host `vmlinuz` on arm64 Linux. |
| UEFI | QEMU/EDK2 firmware supplied by the operator | Use `--boot-mode uefi --firmware <path>`; Vibemon does not ship firmware blobs. |
| Linux networking | TAP with `--tap` | Linux/KVM uses TAP networking. `just run` invokes the binary through `sudo` on Linux because the project recipes treat `/dev/kvm` and TAP networking as root-requiring. |
| macOS networking | `--net user` virtio-net backed by libslirp | This is entitlement-free user-mode NAT, but the local build needs native `libslirp` and `pkg-config`. `--tap` does not work on the ad-hoc-signed macOS binary because vmnet-style networking needs unavailable entitlements. |
| Devices | Serial console, virtio-blk, virtio-net, virtio-console agent, virtio-rng, and writable or read-only virtio-fs | The guest agent is Linux-only. A default aarch64 kernel includes virtio-fs; x86_64/firecracker and custom kernels without virtio-fs do not. |
| Snapshot restore | Same backend and architecture as capture | KVM snapshots restore on KVM builds, HVF snapshots on macOS/HVF builds, and arm64 images on arm64. Cross-hypervisor and cross-architecture migration are out of scope. |

## Apple Silicon macOS specifics

The macOS hypervisor check queries `kern.hv_support`; it must be `1`. `just build` and `just release` apply the required ad-hoc signature automatically. The entitlement file grants only `com.apple.security.hypervisor`; restricted entitlements such as `com.apple.vm.networking` cannot be carried by this ad-hoc signature and cause launch refusal.

HVF itself does not require root. If vmnet networking is available in an environment that supports it, the project run recipe requires running the whole command under `sudo`; the usual ad-hoc-signed local path is `--net user` instead. See [Build from Source](build-from-source.md) for signing and libslirp requirements.

## Linux and Lima

On Linux, the normal path is KVM directly. From macOS, the `lima-*` recipes can drive a separate Linux guest with nested KVM using `limactl shell`. That guest must have its own Vibemon checkout: the macOS checkout is not mounted there. The recipe defaults are `VMON_LIMA_VM=kvm` and `LIMA_REPO=~/vmon` and can be overridden.

## Diagnose the active host

Run the diagnostic from a checkout or with `VMON_BIN` pointing at an executable binary:

```sh
vmon doctor
```

It checks the executable, the macOS signing entitlement when applicable, the HVF/KVM availability, image tools, `mkfs.ext4`, guest kernel, bundled guest agent, daemon socket, and host environment. It exits non-zero for hard failures. The check is useful before first microVM use, but a server- or panel-only host need not satisfy every VM-launch prerequisite. [Installation](installation.md) separates those two deployment cases.
