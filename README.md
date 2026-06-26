<p align="center">
  <img src="assets/hero.png" alt="Vibemon — a KVM/HVF microVM monitor" width="880">
</p>

<p align="center">
  <img alt="host: Linux KVM · macOS HVF" src="https://img.shields.io/badge/host-Linux%20KVM%20%C2%B7%20macOS%20HVF-1f8fd4?style=flat-square&logo=linux&logoColor=white">
  <img alt="arch: x86_64 · aarch64" src="https://img.shields.io/badge/arch-x86__64%20%C2%B7%20aarch64-1f8fd4?style=flat-square">
  <img alt="VMM: Rust edition 2024" src="https://img.shields.io/badge/VMM-Rust%20edition%202024-1f8fd4?style=flat-square&logo=rust&logoColor=white">
  <img alt="SDK: Python 3.14+" src="https://img.shields.io/badge/SDK-Python%203.14%2B-1f8fd4?style=flat-square&logo=python&logoColor=white">
  <img alt="status: experimental" src="https://img.shields.io/badge/status-experimental-e0922f?style=flat-square">
</p>

**Vibemon** (`vmon`) is a small KVM/HVF-based virtual machine monitor for Linux guests. It pairs a Rust VMM core with a Python orchestration layer: a Docker-like CLI, a Modal-style sandbox SDK, a zero-config daemon, a REST/WebSocket server, and a React web panel. Boot a container as a hardware-isolated microVM, suspend and resume it, snapshot a booted machine into a template, then **warm-boot** or **copy-on-write fork** that template in milliseconds.

It is built for local development and experimentation — not as a production isolation boundary (see [Security & limitations](#security--limitations)).

## Highlights

- **Run containers as microVMs** — `vmon run alpine` builds an image rootfs, injects a tiny PID-1 + a static guest agent, and boots it under KVM or HVF.
- **Snapshot / restore / fork** — capture full machine state (vCPU regs, interrupt controllers, device + queue state, guest RAM, virtio-fs metadata); restore a 256 MiB template in **~120 ms** without a kernel boot, or fork copy-on-write clones at **~3 ms / ~22 MiB RSS** each.
- **virtio device model** — serial console, virtio-blk, virtio-net, virtio-console agent channel, and writable or read-only virtio-fs. MMIO transport everywhere; PCI + MSI-X on `x86_64`.
- **Two boot paths** — direct kernel (`vmlinux`/`bzImage`/`Image`) and operator-supplied UEFI firmware.
- **Sandbox platform** — named volumes, in-memory secrets, PTY exec, egress controls, authenticated port tunnels, tags, warm pools, and VMM-enforced wall-clock timeouts.
- **Defense in depth** — Stage-B process filters (seccomp + Landlock + `no_new_privs` + rlimit tightening) are on by default; `--jail` adds cgroup v2, namespaces, pivot-root, and uid/gid drop.
- **Dependency-light** — the engine, daemon, and CLI client are stdlib-only (`pip install vmon`); FastAPI/uvicorn stay in the optional `[server]` extra.

## Architecture

<p align="center">
  <img src="assets/architecture.png" alt="Vibemon architecture: Web panel → vmon serve → vmond/Engine → vmon binary → vmon-agent" width="760">
</p>

Three layers. The Python daemon owns the registry and spawns one Rust `vmon` process per microVM; the guest agent runs inside the VM and talks back over a virtio-console channel.

- **Web panel** (`ui/`, React SPA) — dashboard, terminal, files, and metrics, served by `vmon serve`.
- **`vmon serve`** (`server.py`, FastAPI) — REST + WebSocket gateway, authenticated port proxy, SSE lifecycle events, and OpenAPI docs over the same engine the CLI uses.
- **`vmond` → Engine** (`daemon.py`, `core.py`) — single owner of `$VMON_HOME` (default `~/.vmon`): holds the VM registry, rehydrates VMs from disk on restart, and spawns one VMM subprocess per microVM over a JSON control socket.
- **`vmon` binary** (`src/`, Rust VMM) — allocates guest memory, instantiates virtio backends, runs one thread per vCPU (KVM `KVM_RUN` / HVF) plus a worker thread per device, and serves the pause/resume/snapshot/quit control plane.
- **`vmon-agent`** (`agent/`, Linux guest only) — the in-guest PID-1 helper for exec, file transfer, and network configuration.

The `vmon` CLI is a thin client: it talks to the auto-started local daemon over `~/.vmon/vmond.sock`, exactly like `docker` ↔ `dockerd` ↔ `runc`, so you never type the VMM's flags by hand.

## Quickstart

### The `vmon` CLI (Docker-like)

```sh
# 1. Build the Rust VMM (Linux + /dev/kvm, or macOS 15+ Apple Silicon)
just release                # → target/release/vmon  (auto-codesigns on macOS)

# 2. Install the Python CLI + SDK (stdlib-only core)
pip install -e python/      # provides the `vmon` command

# 3. Run a container as a microVM
vmon run alpine -- sh -c 'echo hello from a microVM; uname -a'

# 4. Snapshot it, then warm-boot or fork copies
vmon snapshot myvm tpl --stop
vmon restore tpl --name warm    # warm-boot, ~120 ms, no kernel reboot
vmon fork tpl --count 5         # copy-on-write clones, ~3 ms each
```

`vmon shell` drops you into an ephemeral interactive Linux shell — attach a running VM, warm-boot a snapshot, or cold-boot a fresh image and remove it on exit.

### The web panel + REST API

```sh
just ui                            # build the React panel into python/vmon/web/
pip install -e 'python[server]'    # adds FastAPI + uvicorn
VMON_API_TOKEN=secret vmon serve --host 127.0.0.1 --port 8000
```

Then open **http://127.0.0.1:8000** (paste the token top-right), **`/docs`** for interactive OpenAPI, and **`/healthz`** for a health check. `vmon serve` is the same single-owner process as the daemon, so the CLI and the REST API share one VM registry.

> The full setup guide — including running the hypervisor in a Lima VM and driving it from macOS — is in **[MANUAL.md](MANUAL.md)**. The Python SDK reference is in **[python/README.md](python/README.md)**.

## Snapshot, restore, fork

<p align="center">
  <img src="assets/lifecycle.png" alt="Snapshot a booted microVM into a template, then restore (~120 ms) or copy-on-write fork (~3 ms)" width="840">
</p>

- **snapshot** — serializes vCPU regs/MSRs/xstate, interrupt controllers/timers, device + queue state, guest RAM, and virtio-fs inode/mode metadata into a versioned on-disk template.
- **restore** — reconstructs that state into a fresh VM with no kernel boot.
- **fork** — maps the template's RAM `MAP_PRIVATE`, so every clone shares clean pages through the host page cache and pays copy-on-write only for what it touches.

Snapshot restore is **backend- and architecture-specific**: a KVM snapshot restores only on a KVM build, an HVF snapshot only on an HVF build, and only at the current on-disk format version (cross-hypervisor migration is out of scope). Named-volume data is not copied into snapshots; the SDK re-attaches volumes by name on restore or fork.

**Compared with Modal sandboxes:** Vibemon's wedge is local ownership of the sandbox stack. It can create from memory snapshots via copy-on-write fork, with a warm pool keeping clones ready for near-instant starts, and ships a self-hosted REST API. GPU passthrough is a non-goal.

## CLI reference

`vmon <command> --help` documents each command; commands that boot or touch a VM only work on a hypervisor host.

| Command | What it does | Example |
| --- | --- | --- |
| `run` | boot a container image / Dockerfile as a microVM | `vmon run alpine -- sh -c 'uname -a'` |
| `shell` | ephemeral interactive shell (attach a VM, warm-boot a snapshot, or boot an image) | `vmon shell --image alpine` |
| `exec` | run a command in a running microVM (`-t` for a PTY) | `vmon exec web sh -lc 'echo hi'` |
| `cp` | copy files host ↔ guest | `vmon cp web:/etc/os-release ./` |
| `ps` / `logs` | list microVMs / show a console (`-f` to follow) | `vmon logs web -f` |
| `pause` / `resume` | suspend / resume | `vmon pause web` |
| `snapshot` | snapshot a VM into a template | `vmon snapshot web tpl --stop` |
| `restore` / `fork` | warm-boot / CoW-clone N copies from a snapshot | `vmon fork tpl --count 5` |
| `stop` / `rm` | stop / remove a microVM | `vmon stop web` |
| `daemon` | `start` / `stop` / `status` of the local `vmond` | `vmon daemon status` |
| `serve` | run the daemon **and** the REST API + web panel | `vmon serve --token secret` |

## Sandbox SDK

The Modal-style `Sandbox` SDK drives the VMM directly (KVM/HVF host required for `create`/`run`):

```python
from vmon.sandbox import Sandbox
from vmon.secret import Secret
from vmon.volume import Volume

sb = Sandbox.create(
    image="alpine",
    timeout_secs=300,
    volumes={"/data": Volume("agent_data")},
    secrets=[Secret.from_env("TOKEN"), Secret.from_dict({"MODE": "ci"})],
    tags={"kind": "oneshot"},
    ports=[8080],
    egress_allow_domains=["api.github.com"],
    pool_size=2,
)

proc = sb.exec("bash", pty=True)
proc.resize(40, 120)

image = sb.snapshot_filesystem("img1")   # default TTL: 30 days
clone = Sandbox.create(template=image)
```

Named volumes persist outside snapshots under a single-writer host lock; secrets are merged into exec environments and never written to VM metadata. Exposed ports surface through `sb.tunnels()` and the authenticated REST proxy at `/v1/sandboxes/{id}/ports/{port}/...`. `Sandbox.aio.*` mirrors the synchronous SDK with thread-backed async methods.

## Support matrix

| | **Linux + `/dev/kvm`** | **macOS 15+ Apple Silicon** |
| --- | --- | --- |
| Hypervisor | KVM | Hypervisor.framework (HVF), ad-hoc codesigned |
| Host arch | `x86_64`, `aarch64` | `aarch64` |
| Guest | Linux (`x86_64`/`aarch64`) | Linux `aarch64` only |
| Networking | TAP (`--tap`) | user-mode NAT (`--net user`); `--tap` unavailable |
| Boot | direct kernel + UEFI | direct kernel `Image` + UEFI |
| Snapshot / restore / fork | MMIO + PCI virtio, virtio-fs | MMIO virtio |

The backend is selected at compile time; there is no runtime switch, and `x86_64` macOS is unsupported. macOS/HVF needs no root (only `--net user` requires native `libslirp` + `pkg-config`, e.g. `brew install libslirp pkg-config`).

## Building from source

```sh
just release        # release build (+ codesign on macOS)
just check          # cargo check --workspace --all-targets
just clippy         # clippy -D warnings
just fmt            # cargo fmt --all
just test           # unit + integration (KVM-gated cases auto-skip)
```

On macOS 15+ Apple Silicon, `just build`/`just release` automatically ad-hoc codesign the binary with `hvf.entitlements`, which grants `com.apple.security.hypervisor` (the only entitlement ad-hoc signing can carry). Building by hand:

```sh
cargo build --release
codesign --sign - --entitlements hvf.entitlements --force target/release/vmon
```

## Running the `vmon` binary directly

The CLI/daemon normally drives the binary for you, but it can be launched standalone.

Boot a Linux kernel with an initramfs:

```sh
sudo ./target/release/vmon \
  --kernel <kernel-image> \
  --initrd <initramfs.cpio.gz> \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"
```

Boot a virtio-blk root disk, or add networking (Linux/KVM uses TAP; macOS/HVF uses entitlement-free user-mode NAT):

```sh
sudo ./target/release/vmon --kernel <k> --rootfs <disk.img> --cmdline "console=ttyS0 root=/dev/vda rw"
sudo ./target/release/vmon --kernel <k> --initrd <i> --tap tap0 --cmdline "console=ttyS0 ..."   # Linux/KVM
./target/release/vmon       --kernel <k> --initrd <i> --net user --cmdline "console=ttyS0 ..."   # macOS/HVF
```

Drive the JSON control socket, then restore or fork a snapshot:

```sh
sudo ./target/release/vmon --kernel <k> --initrd <i> \
  --api-sock /tmp/vmon/control.sock --snapshot-root /tmp/vmon-snapshots --cmdline "console=ttyS0 ..."

# The server writes one banner line first: {"vmon":"0.1.0","api":1}
printf '%s\n' \
  '{"id":1,"method":"pause","params":{}}' \
  '{"id":2,"method":"snapshot","params":{"name":"demo"}}' \
  '{"id":3,"method":"resume","params":{}}' \
  '{"id":4,"method":"quit","params":{}}' \
  | socat - UNIX-CONNECT:/tmp/vmon/control.sock

sudo ./target/release/vmon --restore /tmp/vmon-snapshots/demo
sudo ./target/release/vmon --fork-from /tmp/vmon-snapshots/demo --count 4
```

Use PCI virtio transport (`--transport pci`, `x86_64` only), expose host directories with virtio-fs (`--fs-tag shared --fs-dir <dir>` read-only; `--volume tag:dir[:ro]` writable named volumes), or boot operator-supplied UEFI firmware (`--boot-mode uefi --firmware <OVMF_CODE.fd|QEMU_EFI.fd> --transport pci`). Pinned UEFI/cloud-image assets can be fetched with the best-effort `demo/fetch-test-assets.sh`.

### Production platform flags

The CLI also accepts the lifecycle, agent, jail, networking, and logging flags used by the SDK and server, including:

- `--snapshot-root <dir>` — root for named JSON lifecycle snapshots.
- `--timeout-secs <n>` — VMM-enforced wall-clock deadline (1 s … 24 h); on timeout it writes `status.json` with `reason:"timeout"` and exit code `124`.
- `--mem-target-mib <n>` / `--zram-store-max-mib <n>` / `--zram-swap-file <path>` — Linux transparent guest-RAM paging with a compressed in-process store and swap overflow.
- `--ksm` — mark guest RAM `MADV_MERGEABLE` for host KSM page merging across co-resident guests.
- `--agent-sock <path>` — guest-agent byte bridge over virtio-console.
- `--jail`, `--id <name>`, `--jail-root <dir>`, `--cgroup-*`, `--netns <path>` — namespace/cgroup/pivot-root jail.
- `--seccomp-action kill|errno|log` — seccomp default action (default `errno`; CLI `kill` maps to a `Trap`/SIGSYS for diagnostics).
- `--no-sandbox` — opt out of the default-on Stage-B filters for local dev (incompatible with `--jail`).
- `--sandbox-uid <uid>` / `--sandbox-gid <gid>` — uid/gid to drop to after filters are applied.

Launch-time caps guard against accidental fanout: up to **64 vCPUs**, **64 GiB RAM**, and **32 fork children**.

## Testing

The same Rust integration suite under `tests/` runs end-to-end against each hypervisor; each test declares the capabilities it needs and skips where they are unavailable.

| Environment | Backend / arch | Run it with | Networking exercised |
| --- | --- | --- | --- |
| Linux host | KVM, `x86_64` or `aarch64` | `just integration` | TAP (`--tap`) |
| macOS host | HVF, `aarch64` (Apple Silicon) | `just integration` | user-mode NAT (`--net user`) |
| Lima on macOS | KVM, `aarch64` (nested) | `just lima-integration` | TAP (`--tap`) |

Set `VMON_E2E=1` to opt in to booting guests (the recipes do it for you and fetch the pinned per-architecture assets). The hypervisor-free CLI capability matrix (`tests/cli_matrix.rs`) runs everywhere under a plain `cargo test`. Python unit tests use fake backends and need no KVM; `cd python && uv run pytest`. The KVM end-to-end suite (`test_e2e.py`) is gated by `VMON_KVM_E2E=1`.

## Security & limitations

- **This is not a production isolation boundary and has not had a security audit.** The trusted computing base includes the `vmon` process, KVM/HVF, the host kernel, guest kernel/image inputs, disk images, snapshots, and host paths passed on the command line.
- Treat guests, guest-controlled virtqueue data, and restored snapshot files as **untrusted**; treat operator-supplied kernel/initrd/rootfs images and host paths as **trusted configuration**.
- Stage-B filters (seccomp + Landlock + `no_new_privs` + rlimit tightening) are on by default; `--jail` is the full production isolation path on top of them. Control and agent sockets are operator-owned, mode `0600`, in private parent directories, and on Linux accept only root or the launch uid.
- Snapshot restore requires a matching build architecture, hypervisor backend, and on-disk format version. `--net user` rejects snapshot creation until its NAT backend state is serializable.
- Bare VMM networking uses host TAP on Linux. On macOS/HVF, `--net user` provides outbound/DHCP/DNS connectivity only — not same-LAN bridging or inbound host port forwarding — and `--tap` is unavailable to the ad-hoc-signed binary.
- Do not expose control sockets, filesystem shares, TAP devices, or forwarded ports across trust boundaries without `--jail` and external host network policy.

## Documentation

- **[MANUAL.md](MANUAL.md)** — practical, copy-paste guide: panel + API, full Linux microVMs, and running the hypervisor in Lima from macOS.
- **[python/README.md](python/README.md)** — Python CLI + SDK reference (`MicroVM`, `Sandbox`, REST API).
- **[SECURITY.md](SECURITY.md)** — security policy.
- **[CHANGELOG.md](CHANGELOG.md)** — release notes.
- **[AGENTS.md](AGENTS.md)** — repository layout, conventions, and development commands.
