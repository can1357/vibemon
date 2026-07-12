# Support Matrix

Vibemon runs Linux guests on Linux/KVM, Apple Silicon macOS/HVF, and x86_64 Windows/WHP. The table distinguishes the host platform from the guest that the VMM boots.

| Area | Supported configuration | Operational constraint |
| --- | --- | --- |
| Linux host | Linux with a present, readable, and writable `/dev/kvm` | KVM is the local hypervisor. `vmon doctor` reports a hard failure when `/dev/kvm` is absent or inaccessible. |
| macOS host | macOS 15 or later on Apple Silicon with Hypervisor.framework support | HVF is used natively. The executable must be ad-hoc signed with `com.apple.security.hypervisor`. |
| Windows host | x86_64 Windows with Windows Hypervisor Platform available | WHP is used natively. Enable the Windows Hypervisor Platform/Hyper-V components and hardware virtualization before launching guests. |
| Host CPU architecture | `x86_64` and `aarch64` | Linux follows the host architecture. macOS/HVF is `aarch64` only; Windows/WHP is `x86_64` only. |
| Guest operating system | Linux | Direct-kernel boot and operator-supplied UEFI firmware are supported. Non-Linux guests are not a target. |
| x86_64 direct kernel | Uncompressed ELF `vmlinux` or `bzImage` | `vmon` loads these formats directly on KVM and WHP. |
| aarch64 direct kernel | Uncompressed `Image` | The demo can extract `Image` from a host `vmlinuz` on arm64 Linux. |
| UEFI | QEMU/EDK2 firmware supplied by the operator | Use `--boot-mode uefi --firmware <path>`; WHP maps x86_64 OVMF at the reset-vector address. |
| Linux networking | TAP with `--tap` | The TAP device and its host routing are operator-managed. |
| macOS networking | `--net user` backed by libslirp | This is entitlement-free user-mode NAT. The local build needs native `libslirp` and `pkg-config`. |
| Windows networking | `--net user` backed by libslirp | Provides in-process outbound NAT, DHCP, and DNS. TAP-Windows remains available when an operator-managed adapter is supplied. |
| Host IPC | Unix sockets on Linux/macOS; local named pipes on Windows | Windows control, guest-agent, and remote-filesystem endpoints reject remote named-pipe clients. |
| Devices | Serial console, virtio-blk, virtio-net, virtio-console agent, virtio-rng, writable/read-only virtio-fs, and remote virtio-fs | The guest agent is Linux-only. Device availability also depends on the guest kernel. |
| Snapshot restore | Same backend and architecture as capture | KVM snapshots restore on KVM, HVF snapshots on HVF, and WHP snapshots on WHP. Cross-hypervisor and cross-architecture migration are out of scope. |

## Apple Silicon macOS specifics

The macOS hypervisor check queries `kern.hv_support`; it must be `1`. `just build` and `just release` apply the required ad-hoc signature automatically. The entitlement file grants only `com.apple.security.hypervisor`; restricted entitlements such as `com.apple.vm.networking` cannot be carried by this ad-hoc signature and cause launch refusal.

HVF itself does not require root. If vmnet networking is available in an environment that supports it, the project run recipe requires running the whole command under `sudo`; the usual ad-hoc-signed local path is `--net user` instead. See [Build from Source](build-from-source.md) for signing and libslirp requirements.

## Windows/WHP specifics

WHP requires an x86_64 Windows host with hardware virtualization enabled. Direct-kernel and UEFI boot, snapshot/restore/fork, named-pipe lifecycle and agent transport, remote virtio-fs, TAP-Windows, and `--net user` are supported. The named-pipe endpoints are local-machine transports; they are not network listeners.

## Linux and Lima

On Linux, the normal path is KVM directly. From macOS, the `lima-*` recipes can drive a separate Linux guest with nested KVM using `limactl shell`. That guest must have its own Vibemon checkout: the macOS checkout is not mounted there. The recipe defaults are `VMON_LIMA_VM=kvm` and `LIMA_REPO=~/vmon` and can be overridden.

## Diagnose the active host

Run the diagnostic from a checkout or with `VMON_BIN` pointing at an executable binary:

```sh
vmon doctor
```

It checks the executable, platform hypervisor availability, macOS signing when applicable, image tools, `mkfs.ext4`, guest kernel, bundled guest agent, daemon endpoint, and host environment. It exits non-zero for hard failures. A server- or panel-only host need not satisfy every VM-launch prerequisite.
