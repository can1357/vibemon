# Limits

This page lists caps and defaults verified in the server and VMM configuration.
A value is not a capacity guarantee: available host memory, CPU, storage,
hypervisor capability, and mesh eligibility can impose a lower practical limit.

## Hard launch-time caps

These are enforced by the low-level VMM to prevent accidental fanout.

| Setting | Accepted range | Default |
| --- | --- | --- |
| Sandbox launch count | 1–32 | 1 |
| vCPUs per sandbox | 1–64 | 1 |
| Guest memory | 1 MiB–65,536 MiB (64 GiB) | 256 MiB |
| Execution timeout | 1–86,400 seconds, when set | unset |
| Memory target | 1 MiB through one MiB less than guest memory, when set | unset |
| Compressed page-store cap | 1 MiB through guest memory, when set; requires a memory target | unset |

The count cap applies to one low-level launch invocation; it is not a mesh-wide
quota. Per-worker `ResourceSpec` declares whole vCPUs, memory bytes, ephemeral
disk bytes, architecture, HA policy, volumes, and network policy, but the proto
does not define global numeric caps for those fields.

## Server request caps

| Setting | Accepted range | Default |
| --- | --- | --- |
| Snapshot fork batch | 1–32 clones | none; `count` is required |

Fork batches are atomic. A failed clone causes the server to remove the other
clones created by that request.

## S3 mount admission

A sandbox create request supports at most **8** S3 mounts. At creation, the
server validates access to each referenced bucket or prefix with a one-key
`ListObjectsV2` request. The request must therefore be able to reach the S3 or
S3-compatible endpoint and have permission to list the requested source; this
is an admission check, not a guarantee that the source remains available for
the sandbox lifetime.

## `vmon serve` defaults

These settings can be overridden through the server configuration surface.

| Setting | Default | Notes |
| --- | --- | --- |
| Bind address | `127.0.0.1:8000` | TCP listener default. |
| Function artifact maximum | 4 GiB | Maximum accepted size of one streamed function artifact. |
| Idle sandbox timeout | 300 seconds | Server default. |
| Async replica count | 1 | Desired replica fan-out. |
| Concurrent replica transfers | 2 | Limits simultaneous transfers. |
| Warm-pool size | 1 | Default ready-instance pool size. |
| Mesh heartbeat | 3 seconds | Membership cadence. |
| Mesh reap age | 300 seconds | Membership reaping threshold. |
| Mesh idempotency cache TTL | 900 seconds | Cache retention. |
| Mesh create dispatch timeout | 120 seconds | Dispatch timeout. |

With mesh enabled, the effective checkpoint cadence defaults to 60 seconds;
without mesh it is 0 seconds. Restore quorum defaults on when expected
membership is at least three, and off otherwise. The architecture-level
per-sandbox HA default in a mesh is `async`; outside a mesh it is `off`. See
[Mesh and High Availability](../platform/mesh.md) for the resulting RPO/RTO
semantics, rather than interpreting these values as a zero-loss guarantee.

## Platform-dependent constraints

The supported host and guest combinations are part of the effective limit:
Linux uses KVM on `x86_64` or `aarch64`; macOS uses Hypervisor.framework on
Apple Silicon and supports `aarch64` Linux guests. PCI virtio is `x86_64`-only;
MMIO works across supported architectures. Snapshot, restore, fork, migration,
and replica restore require compatible backend and architecture, even where a
fresh boot may be eligible. See [Support Matrix](../platform/support-matrix.md)
and [Snapshots, Restore, and Fork](../platform/snapshots.md).

Mesh placement is also constrained by requested architecture, image and node
capabilities, and available compatible pools. A writable volume requires a
three-or-more-node mesh for its quorum-fenced lease; it is rejected on smaller
mesh contexts rather than weakened. These are admission constraints, not
numeric tenant quotas.
