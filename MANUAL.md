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
- `skopeo` and `umoci` for OCI images, `e2fsprogs`, and optional `buildah` (or Docker with buildx) for Dockerfile builds (`brew install e2fsprogs` on macOS)

On macOS the `vmm` binary must be codesigned with the hypervisor entitlement;
`just build` / `just release` do this automatically, and HVF itself needs no root.

---

## 3. Quickstart — launch the web panel + API (works on macOS)

This is the part you can run right now on your Mac.

### Step 1 — build the web panel (once)

```sh
cd ui
bun install
bun run build      # outputs into vmond/web so `vmon serve` can serve it
```

### Step 2 — install the server dependencies (once)

Use a virtualenv — this avoids macOS's `externally-managed-environment` error
and keeps things isolated. The `test` extra also installs `pytest` for §9.

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
| `run` | boot a container image as a microVM | `vmon run alpine -- sh -c 'uname -a'` |
| `run -f` | build & boot a Dockerfile locally with buildah or Docker buildx | `vmon run -f ./Dockerfile --context . demo:latest` |
| `build` | build a Dockerfile into a local OCI layout | `vmon build -f Dockerfile -t demo:latest .` |
| `run -d` | run detached (background) | `vmon run -d --name web nginx` |
| `shell` | drop into an ephemeral interactive shell (attach a running VM, warm-boot a snapshot, or boot a fresh image) | `vmon shell` · `vmon shell web` · `vmon shell --image alpine` |
| `exec` | run a command in a running microVM (`-t` for an interactive PTY) | `vmon exec web sh -lc 'echo hi'` |
| `cp` | copy files host↔guest | `vmon cp web:/etc/os-release ./` |
| `ls` | list files in a microVM's guest filesystem (`<name>[:<path>]`) | `vmon ls web:/etc` |
| `ps` | list microVMs | `vmon ps` |
| `logs` | show a VM's console (`-f` to follow) | `vmon logs web -f` |
| `inspect` | print a VM's full detail view as JSON | `vmon inspect web` |
| `stats` | show a VM's live runtime metrics | `vmon stats web` |
| `pause` / `resume` | quiesce / resume a retained live VM | `vmon pause web` |
| `suspend` | durably checkpoint and release the live VM | `vmon suspend web` |
| `history` / `rollback` | list retained recovery points / restore the same sandbox ID to one point | `vmon history web` · `vmon rollback web checkpoint-...` |
| `extend` | reset a running VM's deadline (seconds from now) | `vmon extend web 600` |
| `snapshot` | snapshot a VM into a template | `vmon snapshot web tpl --stop` |
| `restore` | warm-boot from a snapshot | `vmon restore tpl --name web2` |
| `fork` | CoW-clone N copies from a snapshot | `vmon fork tpl --count 5` |
| `stop` / `rm` | stop / remove a microVM | `vmon stop web` |
| `daemon` | `start` / `stop` / `status` of the local `vmond` daemon | `vmon daemon status` |
| `serve` | run the daemon, gRPC API, and web panel (one owner) | `vmon serve --token secret` |
| `doctor` | print a prerequisite checklist (VMM binary, macOS codesign entitlement, HVF/KVM, `skopeo`, `umoci`, `mkfs.ext4`, guest kernel, bundled agent, daemon, and Python/host environment); `--serve` validates `ServeConfig`; exits non-zero on hard failures | `vmon doctor` |
| `completion [bash|zsh|fish]` | print a sourceable clap shell-completion script | `eval "$(vmon completion zsh)"` |

Useful `run` flags: `--name`, `--mem <MiB>` (default 512), `--cpus` (default 1),
`--disk-mb` (default 1024), `--timeout <s>` (default 300), `--arch x86_64|aarch64`,
and `--block-network` (boot with no NIC; optional — a networked sandbox otherwise
gets outbound egress via TAP on Linux or user-mode NAT on macOS, and
`--block-network` also lets `vmon run` skip the root-only TAP setup on Linux).

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
- builds container rootfs with local daemonless image tools (`skopeo` + `umoci`).

To drive a remote KVM host from your Mac, use the **REST API** (`vmon serve`) and
a named context. That is the only non-local transport; it wraps the same engine
over HTTP/WebSocket and gives the CLI/SDK gateway failover.

Three practical patterns follow.

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
  sudo apt-get install -y skopeo umoci e2fsprogs cpio gzip
  # Optional only for `vmon build` / `vmon run -f` Dockerfile builds:
  sudo apt-get install -y buildah
  rustup target add aarch64-unknown-linux-musl    # match the guest arch (x86_64-... on Intel)
  cd ~/vmon && just agent-musl                 # builds the static guest agent vmon injects
  exit
```

> The base block is enough for the REST control plane and for
> snapshot/restore/fork. The second block is required for `vmon run`, which
> pulls/unpacks an OCI image (`skopeo`/`umoci` + `e2fsprogs`/`cpio`) and injects
> a **statically linked** guest agent (`just agent-musl`, or point `VMON_AGENT`
> at one). Dockerfile builds add `buildah` or Docker buildx.

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
./demo/vmon-lima run alpine -- sh -c 'uname -a'   # needs skopeo/umoci in the guest
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

### Pattern C — normal CLI context from your Mac

After Pattern A starts `vmon serve` in Lima and Lima forwards the port, create a
context on the Mac and use the regular CLI/SDK against the gateway:

```sh
export VMON_API_TOKEN=secret
vmon context create lima --server http://127.0.0.1:8137 --save-token
vmon context use lima
vmon ps
vmon run alpine -- sh -c 'uname -a'
```

This uses the same HTTP/WebSocket gateway as the browser. `vmon context use local`
switches the CLI back to the Mac's local daemon.

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

## 7. Cluster: pool multiple servers

A cluster lets several `vmon serve` gateways act as one pool. Each gateway still owns the sandboxes running on its own host, but the CLI/SDK can keep a roster of gateways and route commands through reachable members.

### Step 1: form the cluster

`vmon serve` reads one config surface: dataclass defaults, then a TOML file (`--config` or `VMON_CONFIG`), then `VMON_*` environment variables, then CLI flags. Unknown keys are errors.

```toml
[serve]
host = "0.0.0.0"
port = 8000
token = "T"
replicas = 1
# replicate_sec defaults to 60 on mesh-enabled nodes; set 0 to disable.
```

Pick one node as the seed. Start its gateway with the shared bearer token:

```sh
vmon serve --config serve.toml
```

In another shell on the seed host, initialize the mesh. The command prints a `vmon mesh join <blob>` command; keep that blob for the other nodes.

```sh
VMON_API_TOKEN=T vmon mesh setup --advertise http://<seed-ip>:8000
```

On every other node, start the gateway with the same token, then run the join command printed by the seed:

```sh
vmon serve --config serve.toml
VMON_API_TOKEN=T vmon mesh join <blob>
```

All nodes must share the same full `--token` / `VMON_API_TOKEN`. The join blob embeds the shared token, so treat it as secret too.

Verify membership, health, capacity, and local sandbox tier/RPO rows from any node. The underlying `GET /v1/mesh/status` payload also includes top-level `replicas_held`, status warnings, and per-node HA counters (`stats.replication`, `stats.restore`, `stats.fence`):

```sh
VMON_API_TOKEN=T vmon mesh status
```

### Step 2: make the client use the cluster

Create a named context from any reachable gateway. The CLI calls `GET /v1/mesh/status`, pulls the full roster, and stores the ordered endpoint list in `~/.vmon/contexts.json`. By default it does not store the token; `--save-token` opts in to a private token file under `$VMON_HOME/credentials/`.

```sh
export VMON_API_TOKEN=T
vmon context create prod --server http://<any-node>:8000 --save-token
vmon context use prod
```

Now regular CLI commands go through the cluster context:

```sh
vmon run alpine -- echo hello
vmon ps
vmon exec <sandbox> -- uname -a
```

Use the context commands to inspect, refresh, or remove the saved roster:

```sh
vmon context ls
vmon context inspect prod
vmon context refresh prod
vmon context rm prod
vmon context use local
```

`vmon context refresh prod` re-pulls the roster from the cluster. `vmon context use local` switches back to the local `vmond` daemon. The CLI and SDK share the same `Transport` plane: `LocalTransport` for the daemon, `MeshTransport` for a context roster. In Python, use:

```python
import vmon
from vmon import Sandbox

client = vmon.connect("prod")
sb = client.sandboxes.create(image="alpine")
same = Sandbox.create(image="alpine", context="prod")
```

An explicit missing context is an error; it does not silently fall back to local.

### Step 3: make the advertised URLs reachable

Each node's advertise URL must be routable by every other node and by the client. On a LAN or a cloud VPC with reachable private IPs, this is usually just the node's host or VPC address.

There is no built-in NAT traversal and no relay service. If one of the machines is behind NAT, such as a laptop or home server, put all nodes and clients on a WireGuard or Tailscale overlay and advertise the overlay IP for each node:

```sh
VMON_API_TOKEN=T vmon mesh setup --advertise http://<overlay-ip>:8000
```

Use the same idea when joining other nodes: the URL carried in the mesh must be the address peers and clients can actually dial.

### Step 4: placement

Placement is request-scoped. `--arch x86_64|aarch64` is optional on `run`, `restore`, and `fork`; API/SDK create requests use the `arch` field. If unspecified, the coordinator derives compatible arches from the image manifest (`skopeo inspect`, cached) intersected with live node arches. A single live arch is used directly; mixed live arches with an underivable image return `arch_required`, and no live match returns `unplaceable`.

```sh
vmon run --arch aarch64 alpine -- uname -m
vmon restore tpl --arch x86_64 --name restored
vmon fork tpl --arch aarch64 --count 2
```

### Step 5: understand failover and downtime

Once the context exists, the client can tolerate any number of gateways going down as long as one saved endpoint is still reachable. Idempotent detached `run`/`restore` calls carry a stable key and may walk the roster; attached/interactive operations probe `/healthz` once and then run exactly once.

Before planned downtime, move the work:

```sh
VMON_API_TOKEN=T vmon mesh migrate <name> <node>
```

Or evacuate the node before it leaves:

```sh
VMON_API_TOKEN=T vmon mesh leave --drain
```

### Step 6: high availability

Mesh create records are durable before acknowledgement. On meshes with at least three expected members, the record must reach a strict majority; on a two-node mesh the tier is weaker: every live peer must ack, and with no live peer the local node accepts the record. Anti-entropy re-pushes records so a surviving gateway does not answer `unknown sid` for an acknowledged create.

Per-sandbox tiers are `ha=off|async|rerun|async+rerun`. Mesh-enabled nodes default to `ha=async`; local daemon creates default to `off`.

- `async`: checkpoint periodically and push to rendezvous-ranked peers. The default cadence is 60 seconds on mesh-enabled nodes; set `VMON_REPLICATE_SEC=0` to disable it.
- `rerun`: if no checkpoint exists, re-execute the durable create record at a higher epoch.
- `async+rerun`: prefer checkpoint restore, fall back to rerun.

Use the REST/SDK request field when you need a non-default tier:

```sh
curl -sS -H "Authorization: Bearer T" -H "Content-Type: application/json" \
  -d '{"image":"alpine","detach":true,"ha":"async+rerun"}' \
  http://<gateway>:8000/v1/run
```

`VMON_REPLICAS` sets replica fan-out `K` and defaults to `1`; `VMON_REPLICATE_CONCURRENCY` bounds concurrent peer pushes and defaults to `2`. Replication skips unchanged-digest re-pushes and receivers skip content they already hold, but each cadence still briefly quiesces every `ha=async` sandbox.

Automatic orphan restore is quorum-gated by default at `expected_members >= 3`: the elected survivor asks peers via `GET /v1/mesh/reachable/{node}` and must get a strict majority of the expected cluster to confirm the former owner is unreachable. Set `VMON_RESTORE_QUORUM=0` to force it off. A two-node mesh cannot form a post-failure majority, so quorum restore defaults off and mesh status carries a two-node warning. If a restore cannot safely complete, the orphan is requeued for the next reconciliation pass.

Networked sandboxes are HA/migration-eligible. Linux restores allocate a fresh TAP on the destination; macOS/HVF restores reopen user-net and replay guest-visible libslirp state. Host-side TCP flows are not preserved. `fs_dir` host shares are rejected on mesh creates; use a named volume.

Writable mesh volumes use quorum-granted, epoch-fenced leases with TTL self-fencing. The holder renews by `ttl/2`; if it cannot renew by that deadline, it stops writers, and a successor is not granted until the full TTL has elapsed. Writable volumes on mesh contexts require at least three expected members and are rejected otherwise. Read-only volumes are unrestricted. The local daemon still uses a plain host `flock`.

Fencing for non-volume state is best-effort rather than consensus. A higher `(epoch, node_id)` supersedes an older owner, and a node that discovers one of its local sandboxes has been superseded stops and drops its copy. This bounds split-brain to the partition window and converges after rejoin. Replica secrets live in memory only; if a replica node restarts and loses them, it refuses automatic restore for that sandbox instead of starting it without secrets.

### Step 7: scoped client tokens

Use `VMON_CLIENT_TOKEN` when clients should be able to create and operate sandboxes but not administer the mesh. Start each gateway with the full operator token plus the scoped client token:

```sh
VMON_CLIENT_TOKEN=C vmon serve --config serve.toml
```

Operators keep using the full token for mesh administration:

```sh
VMON_API_TOKEN=T vmon mesh status
VMON_API_TOKEN=T vmon mesh migrate <name> <node>
```

Clients receive the scoped token and pass it through the normal client environment variable:

```sh
VMON_API_TOKEN=C vmon run alpine -- echo hello
VMON_API_TOKEN=C vmon exec <sandbox> -- uname -a
```

The scoped token is accepted for normal sandbox routes (`run`, `exec`, `ps`, and similar operations) and rejected with `403` on mesh-admin routes, including `vmon mesh ...` and migrate. The full `VMON_API_TOKEN` keeps full control.

For token rotation, `VMON_API_TOKEN` and `VMON_CLIENT_TOKEN` can each contain a comma-separated list. Run gateways with both old and new values during rollover, then remove the old value after clients have moved. Any listed value authorizes for its tier; client-tier values still cannot administer mesh routes or migrate sandboxes.

### Step 8: serve the gateway over TLS

Use `vmon serve --tls-cert PATH --tls-key PATH` to serve HTTPS directly from the gateway. The same settings can come from `VMON_TLS_CERT` and `VMON_TLS_KEY`.

Advertise the matching `https://...` URL when forming or joining the mesh. When a peer advertises `https`, the inter-node exec proxy upgrades its WebSocket URL to `wss`.

### Step 9: test the real cluster path

Run `VMON_CLUSTER_E2E=1 just cluster-e2e` on a KVM/HVF host to exercise the gated cluster end-to-end suite (`tests/cluster_e2e.rs`, incl. live migration). CI runs the KVM and macOS/HVF cluster paths on self-hosted hypervisor runners. `mesh-soak.yml` runs the nightly three-node tc-netem soak, and `vmond/tests/mesh_fault_routes.rs` covers the fault-injection contracts.

### Security

The full bearer token is shared by all nodes and grants full control over the cluster. Keep it secret, avoid checking it into scripts, and rotate it if the join blob or environment leaks. Contexts write a token only when `--save-token` is used; otherwise the CLI reads either the full token or a scoped client token from `VMON_API_TOKEN` for each authenticated call.

### Known limitations

Crash recovery is asynchronous unless a workload uses `rerun`; it is not consensus-grade zero-loss memory replication. There is no NAT traversal; use a WireGuard or Tailscale overlay and advertise overlay IPs. Writable mesh volumes require at least three expected members.

---

## 8. Programmatic use: Python SDK & REST API

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

### Remote functions

`@vmon.function(...)` registers an immutable, content-addressed Python revision
and returns a typed `RemoteFunction`. `vmond` owns the durable call record,
input queue, worker pool, retries, cancellation, results, and logs. The Python
process may exit after `.spawn()`; another process can reconstruct the call with
`FunctionCall.from_id()`.

Normal package mode uploads the defining module or package tree and imports the
callable by module and qualified name. Use explicit include/exclude patterns for
local source files. Interactive functions and closures require trusted
`package_mode="cloudpickle"` execution and an exact Python/cloudpickle ABI
match.

Portable arguments and results use I-JSON or CBOR. Cloudpickle is available
only when `SerializerPolicy(..., allow_trusted_python=True)` is set; do not use
it as a cross-language or untrusted boundary. Large values spill into the
content-addressed artifact store instead of remaining in a worker filesystem.

Calls have **at-least-once** execution semantics. A worker can fail after an
external side effect but before its result is committed, so retryable functions
must use idempotency keys or otherwise make side effects safe to repeat.

```python
import math
import vmon

RF_CONST = 7


def triple(n: int) -> int:
    return n * 3


@vmon.function(block_network=True, execution_timeout=120)
def compute(x: int) -> dict[str, int]:
    return {"isqrt": math.isqrt(x), "tripled": triple(x), "const": RF_CONST}


print(compute.remote(16))  # {'isqrt': 4, 'tripled': 48, 'const': 7}
print(list(compute.map([1, 4, 9], max_in_flight=2)))

call = compute.spawn(25)
saved_id = call.id
same_call = vmon.FunctionCall.from_id(saved_id)
print(same_call.get(timeout=120))
```

---

## 9. Smoke-testing every part

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

## 10. Configuration and environment variables

`vmon serve` has one config surface. Values are resolved in this order:
dataclass defaults < TOML config file (`vmon serve --config PATH`, or
`VMON_CONFIG`) < `VMON_*` environment variables < explicit CLI flags. A TOML
file may put these keys at top level or under `[serve]`; unknown keys are errors
so typos do not silently disable protection.

| Config key | Default | Environment override | CLI flag | Meaning |
| --- | --- | --- | --- | --- |
| `home` | `~/.vmon` | `VMON_HOME` | `--home` | state dir for VMs, snapshots, volumes, daemon socket/lock, mesh state, and leases |
| `host` | `127.0.0.1` | `VMON_SERVE_HOST` | `--host` | HTTP bind host |
| `port` | `8000` | `VMON_SERVE_PORT` | `--port` | HTTP bind port |
| `token` | unset (required for `vmon serve`) | `VMON_API_TOKEN` | `--token` | full operator bearer token; comma-separated values support rotation |
| `client_token` | unset | `VMON_CLIENT_TOKEN` | `--client-token` | scoped client bearer token for sandbox routes, not mesh/admin routes |
| `tls_cert` | unset | `VMON_TLS_CERT` | `--tls-cert` | HTTPS certificate path; must be set together with `tls_key` |
| `tls_key` | unset | `VMON_TLS_KEY` | `--tls-key` | HTTPS private-key path; must be set together with `tls_cert` |
| `idle_timeout` | `300.0` seconds | `VMON_IDLE_TIMEOUT` | `--idle-timeout` | idle sandbox reaper timeout |
| `replicate_sec` | auto: `60.0` seconds when mesh is enabled, off when mesh is disabled | `VMON_REPLICATE_SEC` | `--replicate-sec` | checkpoint/replication cadence; explicit `0` disables replication |
| `replicas` | `1` | `VMON_REPLICAS` | `--replicas` | replica fan-out `K` |
| `replicate_concurrency` | `2` | `VMON_REPLICATE_CONCURRENCY` | `--replicate-concurrency` | concurrent peer replica pushes |
| `restore_quorum` | auto: on at `expected_members >= 3`, off below that; 2-node meshes warn | `VMON_RESTORE_QUORUM` | `--restore-quorum` / `--no-restore-quorum` | require majority confirmation before automatic orphan restore |
| `warm_pool_size` | `1` | `VMON_WARM_POOL_SIZE` | `--warm-pool-size` | default count for bare entries in `warm_images` |
| `warm_images` | `[]` | `VMON_WARM_IMAGES` | `--warm-images` | comma-separated image refs to prewarm; entries may be `REF` or `REF=COUNT` |
| `mesh_heartbeat_sec` | `3.0` seconds | `VMON_MESH_HEARTBEAT_SEC` | `--mesh-heartbeat-sec` | mesh heartbeat interval |
| `mesh_reap_sec` | `300.0` seconds | `VMON_MESH_REAP_SEC` | `--mesh-reap-sec` | stale-peer reap/orphan detection window |
| `mesh_idem_ttl_sec` | `900.0` seconds | `VMON_MESH_IDEM_TTL_SEC` | `--mesh-idem-ttl-sec` | idempotency key retention window for mesh creates |
| `mesh_create_timeout_sec` | `120.0` seconds | `VMON_MESH_CREATE_TIMEOUT_SEC` | `--mesh-create-timeout-sec` | peer create/proxy timeout |
| `mesh_w_warm` | `1000.0` | `VMON_MESH_W_WARM` | `--mesh-w-warm` | placement score weight for a warm pool/template |
| `mesh_w_free` | `100.0` | `VMON_MESH_W_FREE` | `--mesh-w-free` | placement score weight for free capacity |
| `mesh_w_local` | `50.0` | `VMON_MESH_W_LOCAL` | `--mesh-w-local` | placement score weight for the ingress-local node |
| `mesh_w_region` | `30.0` | `VMON_MESH_W_REGION` | `--mesh-w-region` | placement score weight for matching region |
| `mesh_w_inflight` | `80.0` | `VMON_MESH_W_INFLIGHT` | `--mesh-w-inflight` | placement score penalty for in-flight load |

Example config file:

```toml
[serve]
host = "0.0.0.0"
port = 8000
token = "operator-token"
replicate_sec = 60
replicas = 1
warm_images = ["alpine:latest=2", "debian:stable-slim"]
```

Run `vmon doctor --serve --config PATH` to print every resolved knob, its source
(`default`, `file`, `env`, or `flag`), and validation results.

Other common variables:

| Variable | Used by | Meaning |
| --- | --- | --- |
| `VMON_CONFIG` | `vmon serve`, `vmon doctor --serve` | TOML config file for the `ServeConfig` surface |
| `VMON_CONTEXT` | CLI / SDK | active context override (`local` selects the daemon) |
| `VMON_BIN` | vmon | path to the `vmm` binary (else auto-detected) |
| `VMON_KERNEL` | vmon | guest kernel (else `/boot/vmlinuz-$(uname -r)` on Linux, or auto-downloaded on macOS) |
| `VMON_KVM_E2E` | tests | set to `1` to enable the real-VM e2e tests (Linux/KVM or macOS/HVF) |
| `VMON_CLUSTER_E2E` | tests | set to `1` to enable gated cluster e2e tests |
| `VMON_E2E_IMAGE` | tests | image for e2e tests (default `alpine:latest`) |
| `VMON_E2E` | Rust tests | set to `1` to run Rust real-VM integration tests |

---

## 11. Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| `vmon serve requires --token or VMON_API_TOKEN` | Pass `--token X` or set `VMON_API_TOKEN`. |
| Panel shows "Authentication required" | Paste your token into the API-token box (top-right). |
| `uvicorn vmon.server:app` fails | No module-level `app`; use `vmon serve` instead. |
| `vmm binary not found` | `cargo build --release` (or `just build` on macOS), or set `VMON_BIN`. |
| `no kernel found` | Set `VMON_KERNEL=/path/to/Image-or-bzImage`. |
| `pip ... externally-managed-environment` (macOS) | Use a venv (see §3, Step 2). |
| `vmon run` fails on macOS | Build `vmm` first (`just build`). Networking works via user-mode NAT; only `-p`/port tunnels and egress allowlists need Linux. Intel Macs are unsupported — use Linux or Lima (§5/§6). |
