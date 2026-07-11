# Networking

A guest network device is virtio-net. Vibemon has two host backends with
different operational and platform requirements. The guest sees Ethernet in
both cases; the host backend determines how that traffic reaches a network.

## Choose a backend

| Host and backend | Guest connectivity | Host prerequisites and limits |
| --- | --- | --- |
| Linux/KVM with `--tap <name>` | The guest NIC sends and receives through the named Linux TAP device. | The TAP device and its host routing, bridge, firewall, and address assignment are operator-managed. Opening/configuring it commonly needs `sudo` or the relevant capabilities. `--netns <path>` enters an operator-supplied network namespace before opening TAP, but only with `--jail --id <name>`: `--netns` requires `--jail`, and `--jail` requires `--id`. |
| macOS/HVF with `--net user` | libslirp provides userspace outbound NAT, DHCP, and DNS. The guest network is `10.0.2.0/24`, with host `10.0.2.2`, DNS `10.0.2.3`, and DHCP starting at `10.0.2.15`. | This is entitlement-free but needs native `libslirp` and `pkg-config` when building. It does not provide general inbound reachability: inbound access requires explicit forwarding support. |
| macOS/HVF with `--tap` | Not a supported substitute for user-mode NAT on the normal ad-hoc-signed build. | vmnet-style host networking needs `com.apple.vm.networking`, an entitlement that the documented ad-hoc signing path cannot grant. Vibemon reports this case as an error. |

On Linux, TAP moves the virtio network header between the guest and
`/dev/net/tun`. macOS user networking consumes and produces ordinary Ethernet
frames, so Vibemon removes or synthesizes that header at the backend boundary.
This implementation detail is why offload behavior is backend-specific:
user-mode networking advertises no TAP offloads.

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

### macOS user-mode NAT example

```sh
# macOS 15+ on Apple Silicon/HVF; binary must have the documented HVF entitlement.
./target/release/vmon vmm \
  --kernel <kernel-image> \
  --initrd <initramfs.cpio.gz> \
  --net user \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"
```

Use this mode for guest egress without a vmnet networking entitlement. It is
not an inbound listener. Do not advertise a service as reachable merely
because it is listening in the guest; expose it through an explicit
server-managed port forwarding or tunnel when that feature is configured.

## Control-plane policy and exposure

The authoritative control plane is gRPC. `SandboxService.NetworkGet` reports
the active configuration, interfaces, and routes; `NetworkSet` updates
network-access controls, CIDR allowlists, and domain allowlists. `Tunnels`
reports active tunnels and port forwardings mapped to a sandbox. These are
separate from the plain HTTP guest port-proxy surface served by `vmon serve`.

Conceptually, use the layers as follows:

| Need | Mechanism | Do not assume |
| --- | --- | --- |
| Guest outbound access | A host backend (TAP topology or macOS user-mode NAT), then the sandbox network policy. | An allowlist itself creates a route, DNS service, or NAT. |
| Inspect/update policy | gRPC `NetworkGet` / `NetworkSet`. | The policy RPC changes host bridge or macOS entitlement configuration. |
| Reach a guest service from outside | An explicitly configured port forwarding/tunnel, visible through gRPC `Tunnels`; the server's HTTP port proxy is the infrastructure HTTP surface. | A listening guest socket is automatically published, especially behind `--net user`. |

`NetworkSet` is presence-aware for its CIDR and domain lists: omitting a list
leaves that allowlist unchanged. Supply valid CIDRs; invalid rule formats are
rejected by the gRPC API. Authentication and server binding are described in
[Server Operation](server.md).

## Snapshot interaction

A VMM snapshot records virtio-net device state. For macOS user networking, it
also records libslirp's guest-visible state, including DHCP lease and ARP/NAT
tables. It cannot carry host-side sockets across restore: in-flight TCP flows
reset, while new outbound connections can be made after restore. TAP snapshots
record the TAP backend hint and NIC state; their continued connectivity still
depends on the referenced host TAP and its live host configuration.

Snapshot files are architecture- and hypervisor-backend-specific. In
particular, do not plan a KVM-to-HVF or HVF-to-KVM network restore. See
[Snapshots, Restore, and Fork](snapshots.md) for the complete compatibility
rules.
