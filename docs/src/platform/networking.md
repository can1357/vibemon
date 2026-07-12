# Networking

A guest network device is virtio-net. Vibemon supports operator-managed TAP networking and libslirp user-mode networking. The guest sees Ethernet in both cases; the host backend determines how traffic reaches the network.

## Choose a backend

| Host and backend | Guest connectivity | Host prerequisites and limits |
| --- | --- | --- |
| Linux/KVM with `--tap <name>` | The guest NIC sends and receives through the named Linux TAP device. | The TAP device and its host routing, bridge, firewall, and address assignment are operator-managed. Opening/configuring it commonly needs `sudo` or the relevant capabilities. `--netns <path>` enters an operator-supplied network namespace before opening TAP, but only with `--jail --id <name>`: `--netns` requires `--jail`, and `--jail` requires `--id`. |
| macOS/HVF or Windows/WHP with `--net user` | libslirp provides userspace outbound NAT, DHCP, and DNS. The guest network is `10.0.2.0/24`, with host `10.0.2.2`, DNS `10.0.2.3`, and DHCP starting at `10.0.2.15`. | This mode does not provide general inbound reachability. macOS builds need native `libslirp` and `pkg-config`; Windows builds use the vendored backend. |
| macOS/HVF with `--tap` | Not a supported substitute for user-mode NAT on the normal ad-hoc-signed build. | vmnet-style host networking needs `com.apple.vm.networking`, which the documented ad-hoc signature cannot grant. |
| Windows/WHP with `--tap <adapter>` | The guest sends and receives through an operator-supplied TAP-Windows adapter. | Adapter creation, addressing, routing, and firewall policy are operator-managed. Prefer `--net user` when outbound NAT is sufficient. |

TAP backends move the virtio network header between the guest and host adapter. User networking consumes and produces ordinary Ethernet frames, so Vibemon removes or synthesizes that header at the backend boundary. User-mode networking advertises no TAP offloads.

### Linux TAP example

The TAP device must already exist and be integrated into the host network by
the operator. The following starts a VMM against `tap0`; it does **not** create
a bridge, configure an IP address, enable forwarding, or install firewall
rules:

```sh
# Linux/KVM host; default-sandboxed root launch. Run from a non-root account
# that can read the guest assets; these substitutions make the VMM drop to it.
sudo ./target/release/vmon vmm \
  --sandbox-uid "$(id -u)" \
  --sandbox-gid "$(id -g)" \
  --kernel <kernel-image> \
  --initrd <initramfs.cpio.gz> \
  --tap tap0 \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"
```

Plan the host-side TAP topology before starting a workload. A TAP attached to
an isolated bridge is not egress by itself; a routed/NATed bridge needs host
forwarding and firewall policy; and a bridged TAP makes the guest part of the
attached Layer-2 domain. Those are host networking decisions outside the VMM.

To enter an operator-supplied network namespace, use a jail rather than adding
`--netns` to the standalone command above. `--netns <path>` requires `--jail`,
and `--jail` requires `--id <name>`; the jail setup also determines which host
paths and sockets are available inside the namespace.

### macOS and Windows user-mode NAT example

```sh
# Apple Silicon macOS/HVF or x86_64 Windows/WHP.
./target/release/vmon vmm \
  --kernel <kernel-image> \
  --initrd <initramfs.cpio.gz> \
  --net user \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"
```

Use this mode for guest egress without creating a host TAP device. It is not an inbound listener. Expose guest services through explicit server-managed forwarding or tunnels.

## Control-plane policy and exposure

The authoritative control plane is gRPC. `SandboxService.NetworkGet` reports
the active configuration, interfaces, and routes; `NetworkSet` updates
network-access controls, CIDR allowlists, and domain allowlists. `Tunnels`
reports active tunnels and port forwardings mapped to a sandbox. These are
separate from the plain HTTP guest port-proxy surface served by `vmon serve`.

Conceptually, use the layers as follows:

| Need | Mechanism | Do not assume |
| --- | --- | --- |
| Guest outbound access | A host backend (TAP topology or user-mode NAT), then the sandbox network policy. | An allowlist itself creates a route, DNS service, or NAT. |
| Inspect/update policy | gRPC `NetworkGet` / `NetworkSet`. | The policy RPC changes host adapter or hypervisor configuration. |
| Reach a guest service from outside | An explicitly configured port forwarding/tunnel, visible through gRPC `Tunnels`; the server's HTTP port proxy is the infrastructure HTTP surface. | A listening guest socket is automatically published, especially behind `--net user`. |

`NetworkSet` is presence-aware for its CIDR and domain lists: omitting a list
leaves that allowlist unchanged. Supply valid CIDRs; invalid rule formats are
rejected by the gRPC API. Authentication and server binding are described in
[Server Operation](server.md).

## Snapshot interaction

A VMM snapshot records virtio-net device state. On macOS and Windows user networking, it also records libslirp's guest-visible DHCP and ARP/NAT state. Host-side sockets cannot move across restore: in-flight TCP flows reset, while new outbound connections work after restore. TAP snapshots record the adapter hint and NIC state; continued connectivity depends on the referenced host adapter and its configuration.

Snapshot files are architecture- and hypervisor-specific. Do not plan KVM, HVF, or WHP cross-backend restores. See [Snapshots, Restore, and Fork](snapshots.md) for the compatibility rules.
