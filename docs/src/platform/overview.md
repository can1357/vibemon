# Architecture

Vibemon is a Rust virtual-machine monitor for Linux guests. Its `vmon` executable combines three roles that are often deployed separately:

- The ordinary operator CLI (`vmon run`, `vmon ps`, and related commands).
- The `vmon serve` control-plane server, implemented by the `vmond` crate.
- The low-level per-VM monitor entered as `vmon vmm`, implemented by the `vmm` crate.

There is no separate Python server process. The Python, Go, and TypeScript packages are clients of the Rust API; they do not install the daemon, panel, guest agent, or VMM.

## Runtime topology

```text
Web panel / Rust CLI / SDK clients
        │
        ├─ gRPC over native h2c
        └─ gRPC over the WebSocket bridge
           (HTTP is retained for health, metrics, and port proxying)
        │
vmon serve — axum + tonic, vmond crate; local UDS is supported
        │
        ├─ sandbox registry, image pipeline, pools, mesh, and volumes
        └─ one `vmon vmm … --api-sock <socket>` child per microVM
                  │
                  └─ virtio-console, length-prefixed binary frames
                           │
                    vmon-agent in the Linux guest
```

The gRPC API is the control-plane interface. Native clients use gRPC over h2c; browser-facing and TypeScript clients use the `/grpc` WebSocket bridge for the same protobuf calls. Health, Prometheus metrics, and sandbox port proxying remain HTTP routes. `vmon serve` also exposes the local Unix-domain socket used by the CLI and local SDK clients.

The server owns the sandbox registry and launches a monitor child for each microVM. The guest agent is a Linux-guest component; it communicates back through the virtio-console channel rather than becoming a host-side server.

## What the monitor does

The low-level boot path is `Config::from_args()`, then `vmm::run()`, `Vmm::build()`, and `Vmm::start()`. Building a VMM allocates guest memory, instantiates virtio backends, and registers them on the device bus. Starting it creates one hypervisor loop thread for each vCPU and one worker thread for each device.

The vCPU loop runs KVM on Linux or Hypervisor.framework (HVF) on macOS. It handles MMIO and Port I/O traps through the device bus and signals virtio queues. Device workers poll queue, backend, and control eventfds before raising completion interrupts.

The monitor's host control plane is a Unix-socket JSON protocol with `ping`, `info`, `pause`, `resume`, `snapshot`, `quit`, `metrics`, and `extend` operations. Socket handling sends requests through a channel to the owning VMM thread rather than accessing the VMM directly. Pause quiesces vCPUs with a real-time signal on Linux and a backend kicker callback on HVF.

## Host boundary

The same executable can serve the panel and API on a host that is not suitable for local microVM execution. Starting `vmon serve` provides the Rust control plane; launching real microVMs additionally requires a supported local hypervisor, guest assets, and—when using image references—the image tools described in [Installation](installation.md).

For supported host and guest combinations, architecture restrictions, networking constraints, and snapshot portability, see [Support Matrix](support-matrix.md). For server operation, see [Server Operation](server.md); direct monitor invocation is documented in [Low-Level VMM](low-level-vmm.md).
