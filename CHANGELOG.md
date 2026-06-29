# Changelog

All notable changes to this project are recorded here.

## Unreleased

### Breaking Changes

- Renamed Rust hypervisor binary/crate from `vmon` to `vmm`, mapping the user-facing self-identification and binary name in build and CI scripts, and resolved naming collision between the Rust binary and Python CLI.
- Renamed Python guest-agent host client from `vmon/agent.py` to `vmon/agent_client.py` and updated all importers (`sandbox.py`, `vmm.py`, test suite) to resolve "agent" naming collision.
- Removed the now-unneeded PATH collision-skip logic from the Python binary locator in `vmm.py`.
- Dropped all support for legacy snapshots; previous snapshots must be recaptured

### Added

- Added `vmon context`, a client-side cluster target with gateway failover. `vmon context create <name> --server <url>` bootstraps a named context by fetching the gateway's mesh roster (`/v1/mesh/status`) and persisting its ordered peer-endpoint list; `ls`/`use`/`refresh`/`rm`/`inspect` manage them (`local` selects the local `vmond` daemon). The CLI transport (`MeshClient`) walks the roster in order and fails over to the next gateway only on a connection-establishment failure, so a dead ingress no longer breaks the client; attached `run`/`exec`/`shell` and non-idempotent calls fail over solely during a `/healthz` probe and then run exactly once, while detached `run`/`restore` carry a stable idempotency key, so a delivered request is never duplicated. A selected context that is missing or has no endpoints is a hard error (never a silent fall-through to the local daemon). Bearer tokens are read from `$VMON_API_TOKEN` and never written to `~/.vmon/contexts.json`.
- Proved cluster crash-survival end-to-end on real hardware (gated two-node KVM e2e: boot a real microVM → replicate its checkpoint to a peer → kill the owner → the survivor restores the sandbox and the client fails over to it). Fixed two checkpoint bugs this surfaced that also broke `vmon mesh migrate` (previously only fake-tested): `MicroVM.restore` now persists the guest `mem`/`cpus` into VM metadata (a warm-restored sandbox otherwise failed replication/migration with "cannot determine the sandbox memory size"), and migration/replication checkpoints now carry the `agent-ready.json` template marker (with the volatile `content_digest` stripped) so the owning node can serve its own content-addressed checkpoint instead of returning 404. Also surfaced two Linux footguns the e2e tripped over: a deep `$VMON_HOME` exceeds the Unix-socket `SUN_LEN` limit, and `vmon` selects the host `/boot` kernel (which lacks built-in virtio) over a pinned microVM kernel.
- Extended crash-survival replication and `vmon mesh migrate` to **stateful sandboxes with named volumes**: a volume's data is captured into the content-addressed checkpoint and re-materialized under the survivor's `$VMON_HOME/volumes` on restore. A read-only volume is divergence-free and always restored; a writable volume is restored only under `VMON_RESTORE_QUORUM`, since `Volume.acquire` is a host-local `flock` that cannot stop a partitioned old owner from also writing its copy. A restore that would clobber an existing same-named volume on the survivor is refused (there is no cluster-unique volume identity yet), and a create that fails after materialization rolls back the freshly-copied volume directories so a retry is not poisoned. The public create model now accepts `{mountpoint: {name, read_only}}`. Verified by a gated two-node KVM e2e (seed a read-only volume on node A → replicate → kill A → the survivor restores the sandbox and the guest reads the data back via `cat /data/...`) plus engine-level unit tests for capture, coercion, dedup, the collision/quorum guards, and rollback.
- Added per-sandbox runtime metrics in the Dashboard's **Metrics** tab
- Added a virtio-rng entropy device feeding the guest `/dev/hwrng` from the host CSPRNG (`/dev/urandom`), seeding the kernel CRNG early so first-boot `getrandom(2)` (TLS, key generation, language runtimes) does not block on a fresh microVM. Exposed as the `--rng` VMM flag on both the MMIO transport (all architectures) and the x86_64 virtio-PCI transport, and captured/reconstructed across snapshot/restore/fork. Agent sandboxes (`vmon run`, the SDK, and the web panel) boot with it by default — cached templates rebuild once on a bumped boot version, and the cross-node mesh stops advertising pre-rng templates. Verified by a gated HVF boot test reading guest entropy and an SDK end-to-end test that a sandbox exposes a working `/dev/hwrng`. The on-disk snapshot format is bumped to version 2 (older snapshots are rejected and must be recaptured).

- Added `vmon ls <name>[:<path>]` to browse a microVM's guest filesystem
- Added client-side retry logic for idempotent sandbox creation and restoration
- Enabled idempotent sandbox creation and restoration across mesh-connected nodes
- Added live workload mobility across the mesh: `vmon mesh migrate <name> <node>` moves a running sandbox to another cluster node, and `vmon mesh leave --drain` evacuates a node's sandboxes before it leaves. Migration is offline and snapshot-based — the source is checkpointed (machine state + guest RAM), the target pulls the content-addressed checkpoint and restores an identical live sandbox (secrets carried over the cluster channel), ownership is remapped cluster-wide so every gateway transparently proxies to the new host, and the source is dropped only after the target confirms (a failed target restores the source from the same checkpoint, so a migration never loses the VM). Scoped to block-network sandboxes with no host-local state — named volumes and `fs_dir` shares are host-bound, so those (and networked sandboxes, which need per-node host networking) are rejected with a clear error. Backed by owner-routed `POST /v1/sandboxes/{id}/migrate`, plus `POST /v1/mesh/migrate/receive` and `POST /v1/mesh/owner`.
- Added crash-survival HA for eligible mesh sandboxes. Operators can opt in with `VMON_REPLICATE_SEC` for a periodic, non-destructive checkpoint cadence and `VMON_REPLICAS` for replica fan-out; each eligible owner pushes checkpoints to rendezvous-ranked peers, and a surviving replica holder can automatically restore an orphaned sandbox, claim ownership at a higher epoch, and broadcast the new owner. Ownership is epoch-fenced and persisted across daemon restart so a superseded owner self-fences when the mesh converges; this is best-effort fencing rather than consensus. Replica secret environment is kept in memory only, and a replica that loses those secrets refuses automatic restore instead of silently degrading. Added `/v1/mesh/replica/receive` and `/v1/mesh/replica/list` for replica transfer and inspection.
- Added opt-in quorum-gated crash restore for mesh HA. `VMON_RESTORE_QUORUM` makes an elected replica holder collect a strict majority of the expected cluster before automatically restoring an orphaned sandbox, using the new authenticated `GET /v1/mesh/reachable/{node}` reachability probe and a persisted expected-membership high-water mark so minority partitions defer instead of split-brain restoring.
- Added a scoped `VMON_CLIENT_TOKEN` authorization tier that lets operators hand clients a token for normal sandbox routes while rejecting mesh-admin routes with `403`; the full `VMON_API_TOKEN` remains required for mesh control. Inter-node WebSocket proxying now supports `wss` when peers advertise `https` URLs.
- Added HA observability to `vmon mesh status` and `GET /v1/mesh/status`: per-node `stats` counters for replication, restore, and fencing, plus top-level `replicas_held`.
- Added comma-separated `VMON_API_TOKEN` and `VMON_CLIENT_TOKEN` values so operators can run old and new tokens together during rotation; any listed value authorizes for that token's tier.
- Added gateway TLS configuration through `vmon serve --tls-cert` / `--tls-key` and `VMON_TLS_CERT` / `VMON_TLS_KEY`; peers advertising `https` continue to use `wss` for the inter-node exec proxy.
- Added the gated two-node cluster end-to-end runner (`just cluster-e2e` / `demo/cluster-e2e.sh`) and CI coverage on the self-hosted hypervisor runner for boot, failover, and restore.
- Added support for idempotent sandbox creation to prevent duplicate VM instantiation
- Added `vmon inspect <name>` to print detailed VM configuration as JSON
- Added `vmon stats <name>` to display live runtime VMM metrics
- Added `vmon extend <name> <secs>` to update a VM's runtime deadline
- Added `vmon inspect <name>`, `vmon stats <name>`, and `vmon extend <name> <secs>` CLI commands. `inspect` prints a VM's full detail view as highlighted JSON, `stats` renders the VMM's live runtime counters, and `extend` resets a running VM's wall-clock deadline (persisted so a rehydrated daemon reports the extended window). All three route through both the `vmond` daemon and the HTTP gateway; `stats` is backed by a new `GET /v1/sandboxes/{id}/metrics` route.
- Added `vmon ls <name>[:<path>]` to browse a microVM's guest filesystem from the CLI: it lists a directory as an `ls -l`-style table (mode, size, mtime, name; directories first, with `ls -F` suffixes) and falls back to a single `stat` row when the path is a file. The path defaults to `/`. The command routes through both the `vmond` daemon (new `fs_list`/`fs_stat` methods) and the HTTP gateway (`GET /v1/sandboxes/{id}/fs/list` and `/fs/stat`), reusing the engine filesystem API that already backs the web panel's file browser.
- Added `@function` decorator to execute local Python functions in a remote sandbox
- Added `RemoteFunction` class to manage serialized function execution and sandbox lifecycle
- Added `RemoteFunctionError` for handling failures occurring inside remote execution environments
- Added support for warm-restoring sandboxes with multiple virtio-fs volumes
- Included hypervisor backend and architecture in node state for mesh placement compatibility
- Added outbound internet access for microVMs by default on macOS/HVF
- Added outbound networking support via user-mode NAT for microVMs on macOS (HVF)
- Added automatic guest kernel provisioning for environments without a local kernel (e.g., macOS/HVF)
- Zero-setup `vmon shell`/`run` on hosts without a guest kernel (e.g., macOS/HVF): when neither `$VMON_KERNEL` nor a matching `/boot` kernel is present, the daemon downloads a pinned, checksum-verified kernel into `~/.vmon/assets` on first boot — no manual `just fetch-assets`. `find_binary()` now locates the locally built, HVF-signed `vmm` VMM through `cargo metadata` (native and cross `debug`/`release` layouts), so `$VMON_BIN` is no longer required, and `mkfs.ext4` is resolved from a keg-only Homebrew e2fsprogs install (`/opt/homebrew/opt/e2fsprogs/sbin`).
- Made `@vmon.function` remote functions usable beyond toy snippets: the shipped source now carries the module-level imports, helper functions/classes, and literal constants the function references (resolved with scope-aware `symtable` analysis, so parameters/locals never shadow a module symbol or pull an unused dependency into the guest), multi-line decorators are stripped via AST, and guest `print()` output is forwarded to the host. Added `RemoteFunction.map()`/`.starmap()` for parallel execution across an ephemeral, auto-torn-down sandbox pool (bounded concurrency, ordered results), and typed the `function` decorator with overloads so `.remote`/`.map`/`.starmap` are statically visible on the decorated symbol. Verified end-to-end in a real microVM (gated `tests/test_e2e.py::test_remote_function_runs_in_real_vm`).
- Completed the async `Sandbox.aio` surface to mirror the synchronous SDK: `snapshot`, `extend`, `metrics`, `tunnels`, and `create_connect_token`, plus an async `aio.filesystem` facade (`read_bytes`/`read_text`/`write_bytes`/`write_text`/`list_files`/`make_directory`/`remove`/`stat`), each dispatched off the event loop via `asyncio.to_thread`, with a parity guard test so future `Sandbox` methods do not silently skip `.aio`. Added a matching `RemoteFunction.aio` facade so remote functions are awaitable too (`await fn.aio.remote(...)`/`.map(...)`/`.starmap(...)`).
- Added `vmon doctor`, a first-run prerequisite check (vmm binary, macOS codesign entitlement, HVF/KVM, Docker/Podman, `mkfs.ext4`, guest kernel, bundled agent, daemon, and host/Python environment) that prints remediation hints and exits non-zero on a hard failure, plus `vmon completion [bash|zsh|fish]` to emit a sourceable Click shell-completion script. The CLI's daemon-connection error path now points users at `vmon doctor`.

### Changed

- Flattened nested VMM counter groups into `group.field` rows in `vmon stats` and Dashboard
- Improved prewarm pool logic to distinguish between networked and block-network sandbox flavors

- Updated mesh request handling to distinguish between unreachable peers and ambiguous responses
- Changed HA replication to apply `VMON_REPLICATE_CONCURRENCY` backpressure (default `2`), skip unchanged-digest re-pushes from the owner, and dedupe peer-side when a receiver already holds the same sandbox id and digest.
- Improved error messaging for sandbox creation to indicate when retries with an idempotency key are required
- Refactored stdin forwarding loop in `vmon exec` to improve terminal responsiveness
- Enabled warm-restore path for networked sandboxes (block_network=True) with volumes
- Updated `vmon run` to enable networking by default on macOS, removing the requirement for `--block-network`
- Renamed the hypervisor binary from `vmon` to `vmm` to resolve naming collisions
- Renamed the project from VibeVMM to Vibemon, and the `VVM`/`vvm` brand prefix to `VMON`/`vmon` throughout. The binary, Python package, CLI, and daemon are now `vmon`/`vmond` (`python -m vmon`); environment variables are `VMON_*` (e.g. `VMON_HOME`, `VMON_API_TOKEN`, `VMON_E2E`); the state directory is `~/.vmon` with the daemon socket at `~/.vmon/vmond.sock`; guest kernel-cmdline keys, serial markers (`VMON_OK`), the bundled `vmon-agent`, the served web UI title, and the Rich console theme keys all follow suit. The generic term "virtual machine monitor" (`vmm`/`VMM`) is unchanged.
- Switched the snapshot on-disk format from bincode to postcard and reset it to format version 1, dropping every legacy snapshot format: the v3–v6 bincode migration paths and the pre-manifest `vmstate.bin`/`memory.bin` file pair. Snapshots captured by earlier builds are unsupported and must be recaptured.

### Fixed

- Fixed the remote page-source URL builder (`_remote_page_url`) to coerce the resolved host to `str`, fixing a type error and guarding the IPv6-bracketing check against non-string `getaddrinfo` results.
- Fixed the KVM vCPU loop to treat a `KVM_RUN` `EAGAIN` as a transient retry (re-enter the run loop) like `EINTR` rather than a fatal vCPU error, matching Cloud Hypervisor / Firecracker; `EAGAIN` occurs notably under nested virtualization (e.g. KVM-on-cloud-VM, Lima).
- Restored a green AArch64 Linux clippy CI gate: the FUSE_MKNOD/`FUSE_CREATE` mode checks now suppress `unnecessary_cast` for the `libc::S_IF*` constants (signed `c_int` on macOS, `c_uint` on Linux), `PagerFatal::new` is `const fn`, and the remote-pager test server takes its 4 KiB page by reference.
- Fixed template resolution to account for virtio-fs slot variations
- Prevented potential deadlocks in stdin forwarding when handling non-TTY streams
- Fixed mesh placement to strictly enforce hypervisor and architecture compatibility
- Fixed hanging `vmon exec` commands by correctly closing stdin when run from a TTY
- Fixed TTY stdin forwarding to prevent daemon crashes during shell execution
- Prevented premature stdin-EOF teardown during non-interactive shell commands
- Scoped the web panel's sandbox **Metrics** tab to the selected sandbox: it now polls `GET /v1/sandboxes/{id}/metrics` (the VMM's live per-sandbox runtime counters) instead of the process-global Prometheus `/metrics`, renders nested counter groups (`vm_exits`, `pager`, …) as grouped tables, and shows a neutral placeholder for non-running sandboxes rather than error-polling the running-only endpoint. Dropped the now-dead global-metrics client helper and its Vite dev-proxy entry.
- `vmon stats` now flattens nested VMM counter groups (`vm_exits`, `snapshot`, `pager`, `ksm`) into `group.field` rows instead of printing raw Python dict reprs.
- Corrected the `prewarm` contract and its docstring: a networked Linux sandbox needs a per-sandbox host TAP a pre-forked pool cannot bake in, so `prewarm` warms the block-network flavor (claimed by `Sandbox.create(image=ref, block_network=True)` — the shape the web panel's create form and `vmon shell` use) while a default networked Linux create warm-restores directly; macOS warms the user-NAT flavor a default create claims. Added regression tests pinning the prewarm→claim path on each host.
- `find_binary()` now resolves the most recently built `vmm` across the cargo `release`/`debug` layouts, so a fresh `just build` (debug) is no longer shadowed by a stale `release` artifact from an earlier `just release` (which surfaced as `unexpected argument '--rng'` against an outdated binary). `$VMON_BIN` still wins and `PATH` remains the final fallback.

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