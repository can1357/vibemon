# Changelog

All notable changes to this project are recorded here.

## Unreleased

### Breaking Changes

- Renamed Rust hypervisor binary/crate from `vmon` to `vmm`, mapping the user-facing self-identification and binary name in build and CI scripts, and resolved naming collision between the Rust binary and Python CLI.
- Renamed Python guest-agent host client from `vmon/agent.py` to `vmon/agent_client.py` and updated all importers (`sandbox.py`, `vmm.py`, test suite) to resolve "agent" naming collision.
- Removed the now-unneeded PATH collision-skip logic from the Python binary locator in `vmm.py`.
- Dropped all support for legacy snapshots; previous snapshots must be recaptured

### Added

- Added support for warm-restoring sandboxes with multiple virtio-fs volumes
- Included hypervisor backend and architecture in node state for mesh placement compatibility
- Added outbound internet access for microVMs by default on macOS/HVF

- Added outbound networking support via user-mode NAT for microVMs on macOS (HVF)
- Added automatic guest kernel provisioning for environments without a local kernel (e.g., macOS/HVF)
- Zero-setup `vmon shell`/`run` on hosts without a guest kernel (e.g., macOS/HVF): when neither `$VMON_KERNEL` nor a matching `/boot` kernel is present, the daemon downloads a pinned, checksum-verified kernel into `~/.vmon/assets` on first boot — no manual `just fetch-assets`. `find_binary()` now locates the locally built, HVF-signed `vmm` VMM through `cargo metadata` (native and cross `debug`/`release` layouts), so `$VMON_BIN` is no longer required, and `mkfs.ext4` is resolved from a keg-only Homebrew e2fsprogs install (`/opt/homebrew/opt/e2fsprogs/sbin`).

### Changed

- Refactored stdin forwarding loop in `vmon exec` to improve terminal responsiveness
- Enabled warm-restore path for networked sandboxes (block_network=True) with volumes

- Updated `vmon run` to enable networking by default on macOS, removing the requirement for `--block-network`
- Renamed the hypervisor binary from `vmon` to `vmm` to resolve naming collisions
- Renamed the project from VibeVMM to Vibemon, and the `VVM`/`vvm` brand prefix to `VMON`/`vmon` throughout. The binary, Python package, CLI, and daemon are now `vmon`/`vmond` (`python -m vmon`); environment variables are `VMON_*` (e.g. `VMON_HOME`, `VMON_API_TOKEN`, `VMON_E2E`); the state directory is `~/.vmon` with the daemon socket at `~/.vmon/vmond.sock`; guest kernel-cmdline keys, serial markers (`VMON_OK`), the bundled `vmon-agent`, the served web UI title, and the Rich console theme keys all follow suit. The generic term "virtual machine monitor" (`vmm`/`VMM`) is unchanged.
- Switched the snapshot on-disk format from bincode to postcard and reset it to format version 1, dropping every legacy snapshot format: the v3–v6 bincode migration paths and the pre-manifest `vmstate.bin`/`memory.bin` file pair. Snapshots captured by earlier builds are unsupported and must be recaptured.

### Fixed

- Fixed template resolution to account for virtio-fs slot variations
- Prevented potential deadlocks in stdin forwarding when handling non-TTY streams
- Fixed mesh placement to strictly enforce hypervisor and architecture compatibility

- Fixed hanging `vmon exec` commands by correctly closing stdin when run from a TTY
- Fixed TTY stdin forwarding to prevent daemon crashes during shell execution
- Prevented premature stdin-EOF teardown during non-interactive shell commands

## 0.2.0

### Added

- A Modal-style `vmon` CLI: colorized, grouped help (rounded command panels, green accent) rendered with `click` + `rich`, plus a new `vmon shell` command that drops into an ephemeral interactive Linux shell — attaching to a running VM instantly, warm-booting a snapshot, or cold-booting a fresh image (default `debian:stable-slim`, override with `$VMON_SHELL_IMAGE`) and removing it on exit. The shell allocates a PTY automatically when attached to a terminal (`--pty`/`--no-pty`), forwards `SIGWINCH` resizes and raw stdin, and `-c '<cmd>'` runs a one-off command; `vmon exec -t` reuses the same interactive PTY path. Ephemeral VMs are torn down server-side even on client disconnect or boot failure. The CLI keeps its stdlib daemon client and emits plain, pipe-parseable output when not attached to a terminal.
- Docker-like client/daemon split for the `vmon` CLI: a thin stdlib client (`vmon.client`) talks to a zero-config, auto-started local daemon (`vmon.daemon`, `python -m vmon.daemon`) over a Unix socket at `~/.vmon/vmond.sock`, mirroring the VMM's newline-delimited JSON protocol. The daemon is the single owner of `~/.vmon`: it wraps a new dependency-free engine (`vmon.core.Engine`) that holds the VM registry, rehydrates running VMs from disk on restart (re-acquiring volume locks), and spawns one VMM process per microVM. The CLI no longer imports the VM SDK, spawns the VMM, or touches `~/.vmon` directly; `vmon run`/`ps`/`logs`/`exec`/`stop` and the new `vmon daemon start|stop|status` all route through the daemon. The FastAPI `Supervisor` became a thin adapter over the same engine, and `vmon serve` now runs the daemon **and** the HTTP/web gateway over one engine (single owner per `~/.vmon`). The engine/daemon/client stay stdlib-only (only the CLI's presentation layer adds `click`/`rich`); `fastapi`/`uvicorn` stay in the optional `[server]` extra. Set `VMON_REMOTE=host:port` (with `VMON_API_TOKEN`) to drive a remote daemon started with `VMON_DAEMON_TCP=host:port`.
- Apple Silicon macOS HVF host support was added for `aarch64` Linux guests: direct-kernel `Image` boot, serial console, in-kernel GICv3, PSCI vCPU bring-up, virtual timer, virtio-mmio block/fs/console, and HVF snapshot capture plus cold restore and copy-on-write fork into fresh vCPUs, selected at compile time when KVM is absent. The binary is ad-hoc codesigned with the `com.apple.security.hypervisor` entitlement. TAP networking (`--tap`) is not supported on macOS/HVF.
- A capability-driven end-to-end integration suite under `tests/` that runs the same tests across Linux/KVM (`x86_64` and `aarch64`), macOS/HVF, and Lima/KVM-on-macOS, opted into with `VMON_E2E=1`. Per-architecture guest assets (x86_64 `vmlinux`, aarch64 `Image`) are fetched by `demo/fetch-test-assets.sh`, and macOS runs route each test binary through `demo/hvf-test-runner.sh` to ad-hoc codesign the spawned VMM. Adds user-mode-NAT networking tests (DHCP + outbound NAT) and a hypervisor-free CLI capability matrix (`tests/cli_matrix.rs`).
- VMM wall-clock deadlines via `--timeout-secs`, `status.json` lifecycle output, and deadline extension through the control API.
- Writable virtio-fs named volumes with repeatable `--volume tag:dir[:ro]`, plus SDK `Volume(name)` support with a single-writer lock.
- Guest-agent pty exec with resize, TCP readiness probes, virtio-fs mounting, and network configuration hooks.
- Sandbox secrets through `Secret.from_dict` and `Secret.from_env`; secret values are injected into exec environments and omitted from VM metadata.
- Sandbox egress controls for blocked networking, CIDR allowlists, DNS-pinned domain allowlists, inbound CIDR restrictions, public tunnels, and connect-token authenticated port proxying.
- Snapshot-to-image flow through `Sandbox.snapshot_filesystem(...)`, with a 30-day default TTL and `Sandbox.create(template=...)` restore.
- Warm pools for template-backed sandboxes using pre-forked copy-on-write clones with cold-restore fallback.
- Sandbox tags, `Sandbox.from_id`, async `Sandbox.aio.*` wrappers, REST filtering by tag, SSE lifecycle events, metrics, OpenAPI docs, and pty exec over WebSocket.
- Transparent guest-RAM zram/paging via `--mem-target-mib`, with compressed in-process storage, swap overflow, userfaultfd fault-in, and pager metrics.
- Host KSM hints via `--ksm` so co-resident forked clones can re-merge identical private pages when the operator enables KSM.

- Linux `x86_64` and `aarch64` KVM host support is documented, including kernel image expectations and virtio transport support.
- x86_64 virtio PCI transport with MSI-X support for supported devices; MMIO remains the portable default.
- Snapshot restore and copy-on-write fork paths for MMIO-backed block, net, console, and serial state.
- x86_64 virtio PCI transport snapshot, restore, and copy-on-write fork, complementing the MMIO snapshot path.
- Writable and read-only virtio-fs device state (shared tag, mount metadata, inode table, and mode) captured in snapshots and reconstructed on restore.
- Snapshots use on-disk format version 1, recording the hypervisor-backend tag and distinct virtio-net backend variants so a snapshot is only restored on the backend that captured it (KVM on KVM, macOS/HVF on macOS/HVF); cross-backend or unsupported-version restores are rejected with a clear error.
- Virtio-fs device support for exposing host directories to the guest, including writable named volumes.
- Virtio-console guest-agent channel and post-restore command dispatch.
- `metrics` JSON lifecycle method exposing additive runtime counters over the v1 control API.
- UEFI boot via operator-supplied firmware: x86_64 OVMF/EDK2 ROM mapping and aarch64 `QEMU_EFI.fd` firmware boot through `--boot-mode uefi --firmware <fd>`.
- Linux process sandbox: seccomp syscall filtering, Landlock path policy, `no_new_privs`, resource-limit tightening, and an optional root UID/GID drop.
- Minimal CI, release notes, and vulnerability-reporting policy.

### Changed

- Stage-B process filters (seccomp syscall filtering, Landlock path policy, `no_new_privs`, and resource-limit tightening) are now enabled by default. Use `--no-sandbox` to opt out for local development (rejected together with `--jail`); `--sandbox` is still accepted but redundant, and `--jail` always forces the filters on. `--sandbox-uid`/`--sandbox-gid` remain required only under `--jail`, while standalone default-on filters drop privileges only when started as root with both provided.
- Named volumes are excluded from snapshots and re-attached by name/path on restore or fork, matching persistent volume semantics.

### Fixed

- The default seccomp allowlist now permits `setsockopt` and `renameat`, which the control/agent servers (accepted-socket read/write timeouts) and snapshot publish (`fs::rename`) require. Previously the sandbox silently dropped every control client before the banner and failed snapshot writes on `aarch64`.
- The pinned `aarch64` integration kernel is now the firecracker-ci `vmlinux-6.1.128` Image; the previous quickstart kernel did not drive vmon's `ns16550a` UART and hung at boot under both KVM and HVF.
- macOS/HVF pause kicks now use per-vCPU `hv_vcpu_run` entry tracking instead of broadcasting to every registered vCPU, avoiding routine kicks to parked or host-side vCPUs during pause/resume and delta snapshots.

### Hardened

- Snapshot files are written as generations and published through a manifest after state and memory data are fsynced.
- Snapshot restore validates version, architecture, memory layout, serial FIFO size, device addressing, backend/device consistency, queue counts, and ready virtqueue RAM ranges.
- Snapshot restore fails closed on genuinely unsupported inputs, including architecture mismatches and snapshot versions newer than the supported format.
- Pause/snapshot coordination now bounds vCPU and worker waits and propagates worker drain failures instead of snapshotting partially drained device state.
- virtio-blk drains in-flight `io_uring` requests before snapshot and reports short or failed completions as I/O errors.
- Demo scripts now mark failed in-guest checks and exit nonzero instead of treating guest failures as successful host runs.
- Control socket binding requires a private parent directory, refuses unsafe stale paths, uses mode `0600`, bounds command lines/timeouts, and checks same-UID peers on Linux.
- CLI validation rejects unsupported host/transport combinations and caps accidental launch fanout, CPU count, and memory size.

### Known limitations

- Production isolation is not claimed; the VMM has not had a security audit.
- CI does not boot guests and does not require `/dev/kvm`.
- Snapshot restore requires a matching build architecture, the same hypervisor backend that captured the snapshot, and the current on-disk format version (1); snapshots from older or newer versions are rejected with a clear error (recapture required).