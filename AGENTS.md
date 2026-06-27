# Repository Guidelines

## Project Overview

Vibemon (`vmon`) is a small KVM/HVF-based Linux microVM monitor. It pairs a Rust VMM core with a Python orchestration layer that offers a Docker-like CLI, a Modal-style sandbox SDK, a daemon, a REST/WebSocket server, and a React web panel.

- **Platforms:** Linux + KVM (`x86_64`, `aarch64`); macOS 15+ Apple Silicon + HVF (`aarch64` guests only). Backend is selected at compile time — there is no runtime switch, and `x86_64` macOS is unsupported.
- **Capabilities:** direct-kernel and UEFI boot, virtio devices (blk/net/console/fs), snapshot / restore / fork with copy-on-write, in-process sandboxing (seccomp + Landlock + jailer), warm pools, secrets, volumes, egress control, PTY exec, and metrics.

## Architecture & Data Flow

Three layers. The Python daemon spawns one Rust `vmm` process per microVM; the guest agent runs inside the VM and talks back over a virtio-console channel.

```
Web UI (React SPA)
   │ HTTP / WebSocket
vmon serve (FastAPI, server.py)
   │ Unix socket  $VMON_HOME/vmond.sock
vmond (daemon.py) ──> Engine (core.py, single registry owner)
   │ spawns subprocess per VM, --api-sock JSON control socket
vmm binary (Rust VMM)
   │ virtio-console, length-prefixed binary frames (GC4 / proto.rs)
vmon-agent (guest agent, Linux guest only)
```

**Rust boot path:** `Config::from_args()` → `vmm::run()` → `Vmm::build()` (boot or restore/fork) → allocate guest memory, instantiate virtio device backends, register on the device `Bus` → `Vmm::start()` spawns one thread per vCPU and one worker thread per device. vCPU threads run the hypervisor loop (`KVM_RUN` / HVF), trap MMIO/PortIO to the `Bus`, and notify virtio queues; device workers `poll()` queue/backend/control eventfds and signal completion interrupts.

**Control plane:** Unix-socket JSON protocol (`ping`, `info`, `pause`, `resume`, `snapshot`, `quit`, `metrics`, `extend`). The socket thread never touches the `Vmm` directly — requests cross a `flume` channel to the owner thread. `PauseGate` quiesces vCPUs via an RT signal without `SA_RESTART` on Linux (handler is a no-op; `EINTR` rechecks run state) and via a backend kicker callback on HVF.

## Key Directories

- `src/` — Rust VMM core (the `vmm` binary).
  - `src/hv/` — hypervisor seam; `kvm/` and `hvf/` backends selected by `#[cfg(target_os)]`.
  - `src/arch/` — architecture-specific boot/setup (`x86_64/`: MP table, GDT, MSR; `aarch64/`: FDT, GIC).
  - `src/virtio/` — virtio device model: `mod.rs` (trait + worker loop), `mmio.rs`, `pci.rs` (x86_64-only), `net.rs`, `block.rs`, `fs.rs`, `console.rs`.
  - `src/os/` — OS primitives (`EventFd`: real `eventfd(2)` on Linux, pipe-backed shim on macOS).
  - `src/devices/`, `src/snapshot/`.
- `agent/` — `vmon-agent` guest agent crate (Linux guest only).
- `tests/` — Rust integration tests; shared helpers in `tests/common/mod.rs`.
- `python/vmon/` — Python package (CLI, daemon, server, Engine, sandbox SDK). Bundled assets: `_agent/` (static guest agent), `web/` (built UI).
- `python/tests/`, `python/e2e.py`, `python/cli_e2e.py` — Python unit + e2e suites.
- `ui/` — React + Vite + TypeScript web panel; **builds into `python/vmon/web/`**.
- `demo/` — runnable demo and asset-fetch scripts (Ubuntu/arm64 boots, OCI→ext4, Lima bridge for macOS).

## Development Commands

`just` is the canonical task runner. Recipes are OS-conditional (Linux uses `sudo` for `/dev/kvm` + TAP; macOS auto-codesigns).

```bash
just build           # debug build (auto-codesigns on macOS)
just release         # release build (+ codesign on macOS)
just run *args       # build then run vmon (sudo on Linux)
just format          # format every language (biome | ruff | cargo fmt)
just lint            # lint every language (biome | ruff | clippy)
just check           # type-check every language (tsc | mypy | cargo check)
just test            # cargo test (unit + integration; KVM-gated cases auto-skip)
just integration     # VMON_E2E=1 cargo test --tests -- --test-threads=1
just soak            # VMON_E2E=1 VMON_SOAK=1 cargo test --test soak -- --test-threads=1
just fetch-assets    # ./demo/fetch-test-assets.sh  (kernels/images → target/test-assets/)
just ui              # cd ui && bun install && bun run build  → python/vmon/web/
just agent-musl      # build static vmon-agent → python/vmon/_agent/vmon-agent-<arch>
```

macOS HVF requires the `vmm` binary to be ad-hoc codesigned with `hvf.entitlements` (`com.apple.security.hypervisor`) before running — `just codesign` / `just build` handle this. Hypervisor.framework needs no root; only vmnet networking needs `sudo`.

Python tooling runs from the `python/` directory (`pyproject.toml`/`uv.lock` live there): `cd python && uv run vmon ...`, `cd python && uv run pytest`, `cd python && uv run ruff check`, `cd python && uv run mypy`. UI dev server: `cd ui && bun run dev` (proxies API to `:8000`). Per-language recipes are suffixed `-rust`/`-py`/`-ui` (e.g. `just lint-py`, `just fmt-ui`, `just check-rust`, `just test-py`).

## Code Conventions & Common Patterns

**Rust**
- **Formatting:** edition 2024; hard tabs, tab width 3, `max_width 100`; `group_imports = "StdExternalCrate"`, `imports_granularity = "Crate"`. Always run `just fmt` — never hand-format.
- **Lints:** workspace clippy `deny` correctness/suspicious, `warn` pedantic/nursery/perf/style; `undocumented_unsafe_blocks` and `allow_attributes_without_reason` are warnings. Every `unsafe` block needs a `// SAFETY:` comment; every `#[allow]` needs a reason.
- **Errors:** crate-wide `Result<T> = Result<T, Box<dyn Error + Send + Sync + 'static>>` (`src/result.rs`). Use the `bail!("…")` macro for early returns and `err()` to build string errors; box underlying crate errors rather than defining bespoke enums.
- **Concurrency:** no async runtime. Blocking syscalls + `EventFd` wakeups + `poll()` loops. Shared devices are `Arc<Mutex<…>>` (`parking_lot`), cross-thread control uses `flume` channels, status flags are `Atomic*`.
- **Platform abstraction:** isolate OS/arch differences behind `src/os/`, `src/hv/`, `src/arch/`, `src/tap.rs` with `#[cfg(target_os/target_arch)]` — do not scatter `cfg` through call sites.

**Python**
- `PascalCase` classes, `snake_case` functions, `_leading_underscore` privates; `ruff` (line-length 100, target py314, select E/F/W/I/UP/B/C4) and `mypy` (typed, `ignore_missing_imports` + `warn_redundant_casts`/`warn_unused_ignores`; not `--strict`).
- Project Python is 3.14+. Write for that target instead of preserving older
  compatibility, and verify syntax claims with `cd python && uv run python -m
  compileall ...` or targeted `py_compile` before changing working code.
- PEP 758 is in scope here: `except ValueError, AttributeError:` is valid
  parenthesis-free multiple-exception syntax because this project targets Python
  3.14+. Do not call it Python 2 syntax, do not rewrite it only to add
  parentheses, and do not generalize this to older Python or to `as exc` forms
  unless the configured interpreter, formatter, or linter verifies the syntax.
- Prefer modern annotations: built-in generics (`dict[str, Any]`), PEP 604
  unions (`Path | str`), `collections.abc` protocols for inputs, concrete
  containers for owned return values, `Self` for fluent APIs, `type` statements
  for aliases, and PEP 695 generic syntax where it improves clarity. Avoid
  `typing.List`/`Dict`/`Tuple`/`Set`, `Optional`, `Union`, and compatibility
  aliases.
- Keep annotation runtime behavior intentional. Add or remove
  `from __future__ import annotations` only after checking code that inspects
  annotations; Python 3.14's default annotation semantics are not the same as
  stringized future annotations.
- Prefer modern stdlib idioms: `pathlib.Path`/`PathLike`, `Path.open()`,
  explicit `encoding="utf-8"` for text files, `time.monotonic()` for deadlines,
  `contextlib.suppress()` for deliberately ignored cleanup errors, and
  `hashlib.file_digest()` for file hashes.
- After changing `requires-python` or dependency constraints, regenerate
  `python/uv.lock` with `cd python && uv lock`; never hand-edit generated
  lockfile markers.
- **Synchronous core:** `Engine` and the daemon are threaded/blocking (registry guarded by `RLock`). Only `server.py` (FastAPI, via `asyncio.to_thread`) and `net.py` tunnels use `asyncio`. CLI rendering (`click` + `rich`) lives only in `console.py`; core stays dependency-light and FastAPI is lazy-imported for `vmon serve`.
- **Errors:** typed exceptions with `code` fields (`EngineError`, `NotFound`, `NotRunning`, `Busy`, `Invalid`, `Unsupported`; `DaemonError`, `AgentError`). Adapters map codes → JSON frames / HTTP status.
- **State:** single daemon per `$VMON_HOME` (flock `vmond.lock`); `VMRecord`s persist to `~/.vmon/vms/*/meta.json` and rehydrate on restart. Secrets live in memory only — never written to disk.

**UI** — React function components + hooks; same-origin `fetch` client (`api.ts`) with bearer auth and WebSocket exec; polling via hooks (`useSandboxes`); OKLCH dark-theme design tokens in `styles.css`. TypeScript strict, `verbatimModuleSyntax`, `noUnusedLocals/Parameters`.

## Important Files

- `src/main.rs` — binary entry point.
- `src/vmm.rs` — VMM lifecycle (build/start/pause/snapshot); owns vCPUs, devices, `PauseGate`.
- `src/config.rs` — manual CLI parser (no clap) and all launch-time flags + hard caps.
- `src/control.rs` — Unix-socket JSON control plane and `PauseGate`.
- `src/result.rs` — error type, `Result<T>`, `bail!`.
- `agent/src/main.rs`, `agent/src/proto.rs` — guest agent and its frame protocol.
- `python/vmon/cli.py` (`vmon` entry point), `daemon.py`, `server.py`, `core.py` (`Engine`), `vmm.py` (`MicroVM`), `sandbox.py`.
- `Cargo.toml` (workspace + lints + profiles), `justfile`, `rust-toolchain.toml`, `rustfmt.toml`, `python/pyproject.toml`, `ui/vite.config.ts`, `hvf.entitlements`.

## Runtime/Tooling Preferences

- **Rust:** pinned `nightly-2026-04-29` (`rust-toolchain.toml`, with rustfmt/clippy/rust-analyzer). Release profile: `opt-level = 2`, `lto = "thin"`, `codegen-units = 1`, `strip = true`.
- **Python:** `>=3.14`; **`uv`** for everything, run from `python/` (`uv run`, `uv sync`). Build backend is `setuptools`; dev deps live in `[dependency-groups]`. Runtime deps: `click`, `rich`; `[server]` extra adds `fastapi`, `uvicorn`.
- **UI:** **bun** for everything (`bun.lock`; no `package-lock.json`). React 18.3 / Vite 5.4 / TS 5.6; biome (`ui/biome.json`) formats + lints `ui/src`, `tsc` type-checks. `just {fmt,lint,check}-ui` and CI all run via bun.
- **Env vars:** `VMON_HOME`, `VMON_BIN`, `VMON_KERNEL`, `VMON_AGENT`, `VMON_API_TOKEN`, `VMON_REMOTE`. The Rust binary is located by `find_binary()` (cargo target dirs → `$VMON_BIN` → `PATH`).

## Testing & QA

**Rust** — `cargo test` runs unit tests (`#[cfg(test)]` embedded in `src/`, e.g. `config.rs`, `snapshot/mod.rs`, `virtio/*.rs`) plus integration tests in `tests/`. Most integration tests boot a real VM and are gated by `VMON_E2E=1` (see `tests/common/mod.rs::require_hv`, which also checks `/dev/kvm` on Linux / `kern.hv_support` on macOS); soak tests additionally need `VMON_SOAK=1`. `tests/cli_matrix.rs` validates flag rejection with no hypervisor needed.

- `boot.rs`, `blk.rs`, `lifecycle.rs`, `net.rs`, `pager.rs`, `snapshot.rs`, `timeout.rs`, `uefi.rs`, `soak.rs` — one concern each (boot markers, block I/O, control protocol, networking, pager eviction, snapshot/fork, timeout self-kill, UEFI boot, stability).
- Integration runs single-threaded (`--test-threads=1`). Boot tests require assets from `just fetch-assets` (cached in `target/test-assets/`). macOS uses `demo/hvf-test-runner.sh` to codesign spawned test binaries.

**Python** — `pytest` (`testpaths = ["tests"]`). Unit tests use fake backends / `FastAPI TestClient` and need **no** hypervisor (`test_cli.py`, `test_daemon.py`, `test_server.py`, `test_volume.py`, `test_secret.py`, `test_vmm_args.py`). `test_e2e.py` and the `python/e2e.py` / `python/cli_e2e.py` drivers exercise real VMs on a Linux/KVM **or** macOS/HVF host (a built `vmm` binary + static agent + guest kernel + docker/podman). Networked sandboxes get outbound egress on both (TAP on Linux, user-mode NAT via `--net user` on macOS); inbound port tunnels and host-side egress allowlists are Linux/TAP-only, so those cases auto-skip off Linux. virtio-fs works with the default aarch64 kernel (the auto-downloaded Cloud Hypervisor `Image` ships `CONFIG_VIRTIO_FS=y`) but the x86_64 firecracker kernel lacks it, so virtio-fs cases auto-skip on x86_64 or under a custom `VMON_KERNEL` without it. `test_e2e.py` is gated by `VMON_KVM_E2E=1`; the standalone drivers self-detect the hypervisor in their `preflight()`.

**CI** — `ci.yml` (ubuntu): fmt-check, check, clippy `-D warnings`, AArch64 check/clippy, `cargo test`, cargo-audit; macOS job builds + codesigns + `cargo test --no-run`. `integration.yml` runs the KVM (self-hosted x64) and HVF (self-hosted arm64) e2e + scheduled soak suites. `release.yml` builds musl binaries (`cargo-zigbuild`) and the Python wheel/sdist with bundled agents on `v*` tags.

When changing exported Rust symbols, check call sites with the language server (`lsp references`) before editing. Verify behavioral changes with the specific gated test rather than relying on `cargo check` alone.
