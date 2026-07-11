# Vibemon documentation

Vibemon (`vmon`) runs Linux workloads as KVM or Hypervisor.framework microVMs. One Rust binary provides the command-line interface, the `vmon serve` control plane, and the `vmon vmm` per-VM monitor. Python, Go, and TypeScript packages are clients of that control plane.

Start with the [platform architecture](platform/overview.md) and [installation guide](platform/installation.md) to run a local server. Use the [SDK overview](sdk/overview.md) to choose a client library, then follow its language-specific guide.

## Choose a path

- **Operate Vibemon:** install the Rust binary, configure `vmon serve`, then use the CLI, web panel, or an SDK.
- **Run directly on a host:** use [Low-Level VMM](platform/low-level-vmm.md) for direct kernel or UEFI boot, device configuration, and the VMM control socket.
- **Run a cluster:** use [Mesh and High Availability](platform/mesh.md) for bootstrap, placement, durability tiers, recovery, and writable-volume leases.
- **Write an application:** use the Python, Go, or TypeScript SDK guide. All clients share the same daemon API and connection-string model.

## Documentation conventions

Examples use a `vmon` binary available on `PATH`. The platform reference distinguishes **operator** behavior, which configures or runs the control plane, from **client** behavior, which uses that plane. Commands that require hardware virtualization or elevated host permissions name that requirement explicitly.

The gRPC contract in `proto/vmon/v1/api.proto` is the public API. Health, metrics, static UI, and guest port proxying are HTTP surfaces; browsers and the TypeScript SDK use the server's gRPC-over-WebSocket bridge.
