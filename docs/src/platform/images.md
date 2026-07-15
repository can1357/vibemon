# OCI Images

Vibemon turns a Linux OCI image into a bootable microVM template. The image is
not run through a container runtime: its OCI filesystem is converted into an
ext4 root filesystem, then Vibemon boots and verifies that root filesystem
before saving the template's VMM snapshot.

## Prerequisites

Image-backed templates require `skopeo`, `umoci`, an ext4 formatter
(`mkfs.ext4` or `mke2fs`), a compatible Linux guest kernel, and a static
musl-built `vmon-agent`. Run `vmon doctor` on the server host to check the
local prerequisites. On macOS, the ext4 utilities may come from an e2fsprogs
installation; Vibemon searches the usual Homebrew locations as well as the
system paths.

The guest architecture must match the host hypervisor architecture. OCI
selection is explicitly for `linux/<arch>`; common manifest spellings are
normalized (`amd64`/`x64` to `x86_64`, and `arm64` to `aarch64`). macOS/HVF
runs only aarch64 Linux guests. See the [Support Matrix](support-matrix.md)
for the host constraints.

## Pipeline

| Stage | What Vibemon does | Operator implication |
| --- | --- | --- |
| Resolve | Uses `skopeo inspect` for the selected Linux architecture and resolves a registry image to its SHA-256 manifest digest. | A mutable tag is re-inspected when it is registered, so it can move. Keep the digest-pinned reference returned by the resolver when reproducibility matters. |
| Acquire | Uses `skopeo copy` to make a local OCI layout. Supported input transports include registry (`docker://`), OCI layout, directory, Docker archive, OCI archive, and containers storage. | The server host, not the guest, needs access to the image source. |
| Unpack | Uses `umoci unpack` to materialize the OCI root filesystem. | OCI image metadata supplies the image entrypoint, command, environment, working directory, and user defaults. |
| Prepare | Injects the static guest agent and creates an ext4 image from the unpacked tree. | A dynamically linked or missing agent is rejected; set `VMON_AGENT=/path/to/static-agent` only to a static ELF agent appropriate for the guest architecture. |
| Verify | Boots the ext4 rootfs with the chosen kernel and requested device slots, then snapshots that verified VM as a template. | A template is a boot-verified artifact, not merely an unpacked image. A boot failure prevents a usable template. |
| Cache | Keys the rootfs cache by image digest, disk size, and agent digest; includes memory, CPU, filesystem-slot, host-share, NIC, and TAP-slot choices in the template identity. | Changing any of those template-shaping options selects a different template. |

The ext4 disk-size request defaults to 1024 MiB in the image pipeline. It is a
capacity choice for the generated root filesystem; ensure it can hold the
unpacked image plus the injected agent.

## Dockerfile builds

Dockerfile builds require `buildctl` and an isolated BuildKit endpoint in `VMON_BUILDKIT_ADDR`. Vibemon does not invoke Docker, Buildah, a shell, or an inherited host environment.

The daemon rejects contexts that escape through symlinks or exceed 1 GiB. It sends the context to BuildKit with a cleared environment, accepts at most 4 GiB of OCI output, validates the OCI layout, and only then publishes it under a content-addressed cache key. The build timeout is 30 minutes.

BuildKit executes Dockerfile instructions, so run the daemon behind `VMON_BUILDKIT_ADDR` as a disposable, least-privileged service. The bundled Compose and Helm deployments use a separate rootless BuildKit workload.

The build and pulled-image caches are not archives. Local `oci:<path>` references are accepted only when the resolved OCI layout is under the server's `builds/` or `images/` cache.

## Commands and image defaults

For an image-backed sandbox, the default process argument vector is:

1. image `Entrypoint` followed by image `Cmd`; or
2. if a non-empty command override is supplied, the entrypoint followed by that
   override; if no entrypoint exists, the override alone.

Environment entries are parsed as `KEY=value` pairs. This describes template
and sandbox defaults; it does not turn arbitrary container-runtime settings
into microVM devices or networking policy.

## Kernel and agent assets

Vibemon has pinned default guest-kernel downloads for `x86_64` and `aarch64`.
If no supported pinned kernel exists for the host architecture, provide one
with `VMON_KERNEL=/path/to/Image-or-bzImage`. A user-supplied image must still
be a Linux guest root filesystem: Vibemon directly boots a kernel rather than
booting a full container runtime or a non-Linux OS.

For lower-level, operator-supplied rootfs and kernel boot commands, see
[Low-Level VMM](low-level-vmm.md). Template state and its restore constraints
are covered in [Snapshots, Restore, and Fork](snapshots.md).
