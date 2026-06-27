# Vibemon — User Manual

A practical, copy-paste guide to building, launching, and testing this project.

There are two layers:

- **`vmm`** — the low-level KVM microVM monitor (Rust binary).
- **`vmon`** — a friendly Python layer on top: a CLI, an SDK, a REST API server,
  and a React **web panel**.

---

## 1. Does it run on macOS?

**Yes** on Apple-silicon Macs (macOS 15+) via Apple's Hypervisor.framework
(HVF) — booting microVMs needs no Linux and no KVM. Read this before anything
else.

| What | macOS (Apple silicon) | Linux + `/dev/kvm` |
| --- | --- | --- |
| Web panel (UI) | ✅ builds & runs | ✅ |
| REST API / `vmon serve` | ✅ runs | ✅ |
| Python SDK logic + unit tests | ✅ | ✅ |
| Building the Rust `vmm` binary | ✅ (HVF backend, auto-codesigned) | ✅ |
| **Booting a microVM** (`vmon run`, exec, snapshot, fork) | ✅ (HVF) | ✅ |
| virtio-fs volumes / host shares | ✅ (default aarch64 kernel has it) | ✅ on aarch64; x86_64/custom kernels need `CONFIG_VIRTIO_FS` |
| Outbound networking (egress) | ✅ (HVF user-mode NAT) | ✅ (TAP) |
| Port tunnels / host egress policy | ❌ Linux-only (TAP) | ✅ |
| Demos (`demo/*.sh`) | ⚠️ Linux-oriented; some need Lima | ✅ |

So on an Apple-silicon Mac you get the full thing — panel, API, and real
microVMs — natively. Caveats: **aarch64 guests only**; the `vmm` binary must be
ad-hoc codesigned with the hypervisor entitlement (`just build` / `just codesign`
handle this); networked sandboxes get outbound egress via macOS user-mode NAT,
but inbound port tunnels and host egress allowlists are Linux-only. virtio-fs volumes/shares work out of the box with the default
aarch64 kernel; x86_64/firecracker and custom kernels need `CONFIG_VIRTIO_FS`. Intel
Macs and x86_64 guests still need
Linux/KVM — run those in a Linux VM with nested KVM (see [§6, Lima](#6-run-the-hypervisor-in-lima-control-it-from-macos)).

---

## 2. Prerequisites

**For the panel + API only:**
- Python 3.14+ (`python3 --version`)
- Bun (`bun --version`) — builds the web panel

**For the full path (real microVMs), additionally:**
- A hypervisor host: Apple-silicon Mac (macOS 15+, HVF) **or** Linux with `/dev/kvm`
- A Rust toolchain (`rustup`)
- `docker` **or** `podman`, plus `e2fsprogs` (`brew install e2fsprogs` on macOS)

On macOS the `vmm` binary must be codesigned with the hypervisor entitlement;
`just build` / `just release` do this automatically, and HVF itself needs no root.

---

## 3. Quickstart — launch the web panel + API (works on macOS)

This is the part you can run right now on your Mac.

### Step 1 — build the web panel (once)

```sh
cd ui
bun install
bun run build      # outputs into python/vmon/web so `vmon serve` can serve it
```

### Step 2 — install the server dependencies (once)

Use a virtualenv — this avoids macOS's `externally-managed-environment` error
and keeps things isolated. The `test` extra also installs `pytest` for §8.

```sh
cd ../python
python3 -m venv .venv
source .venv/bin/activate            # do this in every new terminal
python -m pip install -e '.[server,test]'
```

From now on, with the venv active you can use the `vmon` command directly
(`vmon serve ...`) instead of `python3 -m vmon`.

### Step 3 — launch the server

The server **requires an auth token**. Pick any string.

```sh
# from the python/ directory, without installing the `vmon` command:
VMON_API_TOKEN=secret PYTHONPATH=. python3 -m vmon serve --host 127.0.0.1 --port 8000

# or, if you ran `pip install -e .` so the `vmon` command exists:
vmon serve --host 127.0.0.1 --port 8000 --token secret
```

You should see `Uvicorn running on http://127.0.0.1:8000`.

> ⚠️ There is no module-level `app`, so `uvicorn vmon.server:app` does **not**
> work. Always launch with `vmon serve` — it is the single owner of `~/.vmon`
> (it serves the local `vmond` socket the `vmon` CLI uses **and** the HTTP/web API).

### Step 4 — open it

1. Open **http://127.0.0.1:8000** in your browser.
2. Paste your token (`secret`) into the **API token** box at the top-right.
3. The "Authentication required" message disappears and the panel is live.

Other URLs:
- **http://127.0.0.1:8000/docs** — interactive API documentation (OpenAPI).
- **http://127.0.0.1:8000/healthz** — health check, returns `{"ok":true}`.

### Step 5 — stop the server

Press **Ctrl-C** in the terminal running it. If you started it in the
background:

```sh
lsof -ti tcp:8000 | xargs kill
```

> On an Apple-silicon Mac, `vmon run` and the panel's "+ New" boot real microVMs
> via HVF once the `vmm` binary is built (`just build`). Networking works out of
> the box (outbound egress via user-mode NAT); only exposed ports (`-p` / tunnels)
> and host egress allowlists need Linux. Intel Macs have no aarch64-guest HVF
> support — use Linux or Lima (§5 / §6).

---

## 4. The `vmon` CLI

`vmon` is a thin client for a local daemon (`vmond`) that owns the VM registry and
spawns one VMM per microVM; the first command auto-starts it. Run
`vmon <command> --help` for details. Commands that boot or touch a VM need a
hypervisor host (an Apple-silicon Mac with HVF, or Linux with `/dev/kvm`).

| Command | What it does | Example |
| --- | --- | --- |
| `run` | boot a container image / Dockerfile as a microVM | `vmon run alpine -- sh -c 'uname -a'` |
| `run -f` | build & boot a Dockerfile | `vmon run -f ./Dockerfile --context .` |
| `run -d` | run detached (background) | `vmon run -d --name web nginx` |
| `shell` | drop into an ephemeral interactive shell (attach a running VM, warm-boot a snapshot, or boot a fresh image) | `vmon shell` · `vmon shell web` · `vmon shell --image alpine` |
| `exec` | run a command in a running microVM (`-t` for an interactive PTY) | `vmon exec web sh -lc 'echo hi'` |
| `cp` | copy files host↔guest | `vmon cp web:/etc/os-release ./` |
| `ps` | list microVMs | `vmon ps` |
| `logs` | show a VM's console (`-f` to follow) | `vmon logs web -f` |
| `pause` / `resume` | suspend / resume | `vmon pause web` |
| `snapshot` | snapshot a VM into a template | `vmon snapshot web tpl --stop` |
| `restore` | warm-boot from a snapshot | `vmon restore tpl --name web2` |
| `fork` | CoW-clone N copies from a snapshot | `vmon fork tpl --count 5` |
| `stop` / `rm` | stop / remove a microVM | `vmon stop web` |
| `daemon` | `start` / `stop` / `status` of the local `vmond` daemon | `vmon daemon status` |
| `serve` | run the daemon **and** the REST API + web panel (one owner) | `vmon serve --token secret` |

Useful `run` flags: `--name`, `--mem <MiB>` (default 512), `--cpus` (default 1),
`--disk-mb` (default 1024), `--timeout <s>` (default 300), and `--block-network`
(boot with no NIC; optional — a networked sandbox otherwise gets outbound egress
via TAP on Linux or user-mode NAT on macOS, and `--block-network` also lets
`vmon run` skip the root-only TAP setup on Linux).

`vmon shell` drops you into an ephemeral Linux shell. With no argument (or
`--image <ref>`) it boots a fresh sandbox (default `debian:stable-slim`, override
with `$VMON_SHELL_IMAGE`) and removes it on exit; given a **running VM** it
attaches instantly; given a **snapshot** it warm-boots a throwaway clone. A PTY
is allocated automatically when stdin/stdout are a terminal (`--pty`/`--no-pty`
to force it); `-c '<cmd>'` runs a one-off command instead of an interactive
shell. The help screens (`vmon --help`, `vmon <cmd> --help`) are colorized and
grouped; color is dropped automatically when output is piped.

---

## 5. Full microVMs on a native host (Linux/KVM or macOS/HVF)

On Linux with `/dev/kvm`, or an Apple-silicon Mac (macOS 15+, no KVM needed):

```sh
# 1. Build the VMM binary  (on macOS `just build` auto-codesigns it for HVF)
cargo build --release            # produces target/release/vmm

# 2. Install the vmon CLI/SDK
pip install -e python/

# 3. Run a container as a microVM (networked by default: TAP on Linux, user-mode
#    NAT on macOS). Add --block-network for a no-NIC sandbox.
vmon run alpine -- sh -c 'echo hello from a microVM; uname -a'

# 4. Snapshot / restore / fork
vmon snapshot myvm tpl --stop
vmon restore tpl --name warm      # warm-boot (~120 ms)
vmon fork tpl --count 5           # CoW clones (~3 ms each)
```

The low-level binary can also be driven directly; see the project `README.md`
("Basic run commands") for `--kernel/--initrd/--rootfs/--tap/--api-sock/...`.

---

## 6. Run the hypervisor in Lima, control it from macOS

This is the nice setup: the **hypervisor runs in a Linux VM**, but you stay on
your Mac.

### Pointing the `vmon` CLI at a remote host

The `vmon` CLI is a thin client. By default it drives a **local** daemon (`vmond`)
that:
- spawns the `vmm` VMM as a **child process** on the same host,
- talks to it over **Unix domain sockets** (`control.sock`, `agent.sock`),
- builds container rootfs with a **local** docker/podman.

There are two ways to drive a remote KVM host from your Mac:
- the **REST API** (`vmon serve`), which wraps the same engine over HTTP/WS — the
  recommended, fully-featured remote interface; or
- the CLI's native **`VMON_REMOTE=host:port`** mode, which speaks the daemon's
  JSON protocol over TCP to a remote daemon started with `VMON_DAEMON_TCP=host:port`
  (authenticated with `VMON_API_TOKEN`). This never auto-starts a daemon.

Two practical patterns follow.

### One-time guest setup

Apple-silicon Macs (M3+, macOS 15+) can run a Linux VM with **nested KVM**:

```sh
brew install lima
limactl start --vm-type=vz --set='.nestedVirtualization=true' --name=kvm template:default
```

Then, inside the guest, get the code + toolchain ready (the repo is **not** the
Mac checkout unless it lives under your home dir, which Lima mounts read-only —
clone it fresh in the guest so builds can write):

```sh
limactl shell kvm           # drop into the guest
  # clone or copy the repo to ~/vmon, then:

  # --- base: hypervisor + REST control plane ---
  sudo apt-get update && sudo apt-get install -y python3-venv python3-pip curl build-essential pkg-config
  command -v cargo || { curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y; . "$HOME/.cargo/env"; }
  cd ~/vmon && cargo build --release          # builds the vmm binary in the guest
  cd python && python3 -m venv .venv && . .venv/bin/activate
  pip install -e '.[server]'

  # --- additionally, only for `vmon run <image>` (booting containers) ---
  sudo apt-get install -y docker.io e2fsprogs cpio gzip
  sudo usermod -aG docker "$USER"                 # then log out/in (or `newgrp docker`)
  rustup target add aarch64-unknown-linux-musl    # match the guest arch (x86_64-... on Intel)
  cd ~/vmon && just agent-musl                 # builds the static guest agent vmon injects
  exit
```

> The base block is enough for the REST control plane and for
> snapshot/restore/fork. The second block is required only for `vmon run`, which
> builds a container rootfs (`docker`/`podman` + `e2fsprogs`/`cpio`) and injects
  a **statically linked** guest agent (`just agent-musl`, or point `VMON_AGENT`
> at one). Rootless `podman` avoids the docker-group step.

### Pattern A (recommended) — REST API + web panel from your Mac

Run the server **in the guest**, reach it **from the Mac**:

```sh
# in the guest
limactl shell kvm -- bash -lc '
  cd ~/vmon/python && . .venv/bin/activate &&
  VMON_API_TOKEN=secret PYTHONPATH=. python3 -m vmon serve --host 0.0.0.0 --port 8137'
```

Lima automatically forwards a guest port bound to `0.0.0.0` to **`127.0.0.1` on
your Mac** (verify with `lsof -nP -iTCP:8137`). Now, from the Mac:

```sh
open http://127.0.0.1:8137                 # the web panel (token: secret)
curl -s http://127.0.0.1:8137/healthz      # {"ok":true}
curl -s -H 'Authorization: Bearer secret' http://127.0.0.1:8137/v1/sandboxes
```

> Security: binding `0.0.0.0` in the guest exposes the API to anything that can
> reach the guest's network. On a default local Lima VM that's just your Mac's
> `127.0.0.1`. For a hardened setup, bind `--host 127.0.0.1` in the guest and
> forward explicitly with SSH: `ssh -F ~/.lima/kvm/ssh.config -L 8137:127.0.0.1:8137 lima-kvm`.

### Pattern B — the `vmon` CLI, typed on the Mac, executed in Lima

Use the wrapper `demo/vmon-lima`, which relays the CLI into the guest via
`limactl shell`:

```sh
./demo/vmon-lima ps
./demo/vmon-lima run alpine -- sh -c 'uname -a'   # needs docker in the guest
./demo/vmon-lima logs web -f
```

It activates the guest venv, points `VMON_BIN` at the built binary, and runs
`python3 -m vmon` there. Override the VM name with `VMON_LIMA_VM=othervm`. Add
`alias vmon='/path/to/vmon/demo/vmon-lima'` to your shell rc and you have the
`vmon` command on your Mac, executing against the hypervisor in Lima.

> **Path caveat:** the command runs entirely *in the guest*, so file paths are
> **guest** paths, not macOS paths. `vmon run -f ./Dockerfile --context .`,
> `vmon cp web:/etc/x ./here`, etc. resolve inside Lima. The Mac's `/work/...`
> tree is **not** mounted in the guest — only your home dir is, read-only. Copy or
> clone what you need into the guest first.

### Pattern C — drive a remote daemon natively with `VMON_REMOTE`

The CLI can target a remote daemon directly, no `limactl shell` hop. In the
guest, start a daemon that listens on TCP:

```sh
# in Lima (the KVM host):
VMON_API_TOKEN=secret VMON_DAEMON_TCP=0.0.0.0:9137 python3 -m vmon.daemon
```

Then from the Mac, point the CLI at it (the same JSON protocol the local Unix
socket uses, authenticated with the token):

```sh
export VMON_REMOTE=127.0.0.1:9137 VMON_API_TOKEN=secret   # forward the guest port first
vmon ps
vmon run alpine -- sh -c 'uname -a'
```

`VMON_REMOTE` never auto-starts a daemon and `vmon daemon stop` is refused for it
(the pid lives on the remote host). For a fuller, browser-friendly interface use
the REST API (`vmon serve`) instead.

### Quick demos (low-level, no `vmon`)

`demo/run-on-lima.sh` relays the **macOS** script path into the guest, so it only
works when your checkout lives under your home dir (the path Lima mounts). If the
repo is elsewhere (e.g. `/work/vmon`, which is **not** mounted), run the
guest's own clone directly instead:

```sh
# only if the repo is under your mounted home dir:
demo/run-on-lima.sh demo kvm        # busybox + virtio-blk + virtio-net
demo/run-on-lima.sh ubuntu kvm      # real Ubuntu rootfs

# otherwise, run the guest clone directly:
limactl shell kvm -- bash ~/vmon/demo/run-arm64-demo.sh
limactl shell kvm -- bash ~/vmon/demo/run-arm64-ubuntu.sh
```

---

## 7. Programmatic use: Python SDK & REST API

### From macOS — REST client against a running `vmon serve`

Works against the local server (section 3) or the Lima-hosted one (section 6).
Any HTTP client works — here is the stdlib (no `pip install` needed):

```python
import json, urllib.request
BASE, TOK = "http://127.0.0.1:8137", "secret"

def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{BASE}{path}", data=data, method=method,
                                headers={"Authorization": f"Bearer {TOK}",
                                         "Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.load(r)

call("GET", "/healthz")        # {'ok': True}
call("GET", "/v1/sandboxes")   # list (auth required)

# create a sandbox — needs a hypervisor-backed server (macOS/HVF or Linux/KVM):
call("POST", "/v1/sandboxes", {"image": "alpine", "timeout_secs": 300})
```

Prefer `curl`? `curl -s -H 'Authorization: Bearer secret' http://127.0.0.1:8137/v1/sandboxes`.
The full surface (create / list / exec / files / snapshots / network / tunnels /
events) is documented live at `http://127.0.0.1:8137/docs`.

### On a hypervisor host — the in-process SDK

The `Sandbox` / `MicroVM` SDK drives the hypervisor directly and needs one
(import works anywhere, but `create`/`run` need macOS/HVF or Linux `/dev/kvm`):

```python
from vmon.sandbox import Sandbox

# Networked sandboxes work on macOS/HVF via user-mode NAT; set block_network=True only for a no-NIC sandbox.
sb = Sandbox.create(image="alpine", timeout_secs=300)
try:
    proc = sb.exec("sh", "-lc", "echo hi")
    print(proc.stdout.read())        # b'hi\n'
finally:
    sb.terminate()
```

`MicroVM.run` / `restore` / `fork` are the lower-level container and snapshot
primitives; `python/README.md` has the full SDK reference.

---

## 8. Smoke-testing every part

### Anywhere (macOS or Linux) — panel, CLI, API, and unit tests

```sh
# Web panel: typecheck + production build
cd ui && bun install && bun run typecheck && bun run build

# Python SDK / CLI / server unit + integration tests
#   (the 4 real-VM e2e tests auto-skip unless VMON_KVM_E2E=1)
cd ../python && python3 -m pytest -q          # -> passed, with a few skipped

# CLI is wired up
PYTHONPATH=. python3 -m vmon --help

# REST API + panel serve correctly (start `vmon serve` first, then:)
curl -s localhost:8000/healthz                                   # {"ok":true}
curl -s -o /dev/null -w '%{http_code}\n' localhost:8000/         # 200 (panel)
curl -s -o /dev/null -w '%{http_code}\n' localhost:8000/docs     # 200 (API docs)
curl -s -o /dev/null -w '%{http_code}\n' localhost:8000/v1/sandboxes                       # 401 (auth works)
curl -s -o /dev/null -w '%{http_code}\n' -H 'Authorization: Bearer secret' localhost:8000/v1/sandboxes  # 200
```

### On a hypervisor host (macOS/HVF or Linux/KVM) — the real VM behaviour

```sh
# Rust: same gates as CI
cargo fmt --check
cargo check --locked
cargo clippy --locked -- -D warnings
cargo test --locked

# Rust integration suite — boots real guests (KVM on Linux, HVF on macOS;
#   TAP-networking cases need root and run on Linux only)
just integration

# Python end-to-end against real VMs
cd python && VMON_KVM_E2E=1 python3 -m pytest tests/test_e2e.py -q
```

> ⚠️ `just integration` **silently skips** some coverage unless you set it up:
> - **Networking tests** need a host TAP: export `VMON_TAP=vmon0` and
>   `VMON_HOST_IP=192.168.249.1`, and create the TAP (see
>   `.github/workflows/integration.yml` for the exact `ip tuntap` commands).
> - **UEFI boot tests** need firmware + cloud images:
>   `VMON_UEFI_IMAGES=1 ./demo/fetch-test-assets.sh`.

---

## 9. Environment variables

| Variable | Used by | Meaning |
| --- | --- | --- |
| `VMON_API_TOKEN` | `vmon serve`, daemon TCP, `VMON_REMOTE` | bearer token for the REST API and remote daemon (required for REST) |
| `VMON_HOME` | vmon | state dir for VMs/snapshots/volumes + daemon socket/lock (default `~/.vmon`) |
| `VMON_REMOTE` | `vmon` CLI | `host:port` of a remote daemon to drive over TCP (no auto-start) |
| `VMON_DAEMON_TCP` | `vmond` | `host:port` for the daemon to also listen on for `VMON_REMOTE` clients |
| `VMON_BIN` | vmon | path to the `vmm` binary (else auto-detected) |
| `VMON_KERNEL` | vmon | guest kernel (else `/boot/vmlinuz-$(uname -r)` on Linux, or auto-downloaded on macOS) |
| `VMON_KVM_E2E` | tests | set to `1` to enable the real-VM e2e tests (Linux/KVM or macOS/HVF) |
| `VMON_E2E_IMAGE` | tests | image for e2e tests (default `alpine:latest`) |
| `VMON_KVM` | Rust tests | set to `1` to run KVM integration tests |

---

## 10. Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| `vmon serve requires --token or VMON_API_TOKEN` | Pass `--token X` or set `VMON_API_TOKEN`. |
| Panel shows "Authentication required" | Paste your token into the API-token box (top-right). |
| `uvicorn vmon.server:app` fails | No module-level `app`; use `vmon serve` instead. |
| `vmm binary not found` | `cargo build --release` (or `just build` on macOS), or set `VMON_BIN`. |
| `no kernel found` | Set `VMON_KERNEL=/path/to/Image-or-bzImage`. |
| `pip ... externally-managed-environment` (macOS) | Use a venv (see §3, Step 2). |
| `vmon run` fails on macOS | Build `vmm` first (`just build`). Networking works via user-mode NAT; only `-p`/port tunnels and egress allowlists need Linux. Intel Macs are unsupported — use Linux or Lima (§5/§6). |
