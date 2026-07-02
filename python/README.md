# Vibemon — a friendly microVM CLI & SDK

Run containers as hardware-isolated microVMs, suspend/resume them, snapshot a
booted container into a template, then **warm-boot** (restore) or **fork** that
template in milliseconds. Built on the [`vmm`](../) KVM (Linux) / HVF (Apple-silicon macOS) VMM.

```
                    OCI/container image
                              │  vmon run
                              ▼
   ┌──────────┐  snapshot  ┌────────────┐  restore (~120ms)  ┌──────────┐
   │ microVM  │ ─────────▶ │  template  │ ─────────────────▶ │ warm VM  │
   │ (booted) │            │ (on disk)  │  fork  (~3ms, CoW) │ clones   │
   └──────────┘            └────────────┘ ─────────────────▶ └──────────┘
```

## Requirements

Runs on Python 3.14+ on a Linux host with **KVM** (`/dev/kvm`) or an
Apple-silicon **macOS** host with **HVF**. Image-backed sandboxes need the
daemonless OCI image tools **skopeo** and **umoci**, `mkfs.ext4`/`mke2fs`, and
the `vmm` binary built (`cargo build --release`, or `just build` on macOS to
ad-hoc codesign it). A guest kernel is auto-detected from
`/boot/vmlinuz-$(uname -r)` on Linux and auto-downloaded into `$VMON_HOME/assets`
on macOS (override with `VMON_KERNEL`).

`run` boots an agent-enabled microVM: a small, statically linked guest agent
(`vmon-agent`) is injected into the image rootfs. Release wheels ship it; in a
source checkout, build and bundle it once with `just agent-musl` (or point
`VMON_AGENT` at a static build).

## Install

```sh
pip install -e python/        # provides the `vmon` command
# or run without installing:
PYTHONPATH=python python3 -m vmon --help
```

## CLI

`vmon` is a thin container-style client. It talks to a **zero-config local
daemon** (`vmond`) over a Unix socket at `$VMON_HOME/vmond.sock` (default
`~/.vmon/vmond.sock`); the first command auto-starts the daemon if it is not
already running. The daemon is the single owner of `~/.vmon`: it holds the VM
registry, rehydrates VMs from disk on restart, and spawns one `vmm` VMM process
per microVM. You never invoke the VMM's flags by hand — `run`, `ps`, `logs`,
`exec`, and `stop` all route through the daemon.

```sh
# Drop into an ephemeral interactive shell (fresh Debian sandbox, removed on exit)
vmon shell

# ...or a shell in a specific image / an attached running VM / a one-off command
vmon shell --image alpine
vmon shell web                 # attach to a running microVM (instant)
vmon shell --image alpine -c 'cat /etc/os-release'

# Boot a container image (runs the entrypoint, streams the console, exits when done)
vmon run alpine -- sh -c 'echo hello from a microVM; uname -a'

# Dockerfile builds are currently unsupported until a daemonless builder such as
# buildah/buildkit is wired in; publish or prebuild an OCI image, then run it.

# Long-running service, detached
vmon run -d --name web nginx

# Suspend / resume a running microVM
vmon pause web
vmon resume web

# Snapshot a booted container into a reusable template
vmon snapshot web webtemplate

# Warm-boot a fresh instance from the template (~120ms for a 256 MiB VM)
vmon restore webtemplate --name web2

# Fork N copy-on-write clones (shared clean RAM pages; ~3ms, ~22 MiB each)
vmon fork webtemplate --count 5

vmon ps          # list microVMs
vmon logs web    # show a microVM's console
vmon exec web -- sh -lc 'echo hi'   # run a command in an agent-enabled VM (-t for a PTY)
vmon stop web    # quit a microVM
vmon rm web      # remove it

# Daemon control (auto-started on first use; rarely needed by hand)
vmon daemon status   # running pid + socket path
vmon daemon stop     # stop the local daemon and remove its socket
vmon daemon start    # explicitly start it
```

## SDK

```python
from vmon import Sandbox

sb = Sandbox.create(image="alpine", memory=256)
proc = sb.exec("sh", "-c", "echo hi")
print(proc.wait())

# Dockerfile builds are currently unsupported until a daemonless builder such as
# buildah/buildkit is wired in; prefer image-backed sandboxes or prebuilt templates.

img = sb.snapshot_filesystem("template")      # snapshot the guest filesystem
again = Sandbox.create(template=img)          # warm-boot from the template
print(again.name)
```

## Sandbox SDK

`Sandbox` is the higher-level API for agent-style jobs. It restores from templates, attaches the guest agent, and exposes exec, files, networking, volumes, secrets, snapshots, and tunnels.

```python
from vmon.sandbox import Sandbox
from vmon.secret import Secret
from vmon.volume import Volume

sb = Sandbox.create(
    image="alpine",
    timeout_secs=300,
    volumes={"/data": Volume("agent_data")},
    secrets=[Secret.from_dict({"TOKEN": "sekret"}), Secret.from_env("API_KEY")],
    tags={"job": "oneshot"},
)

p = sb.exec("bash", pty=True)
p.resize(40, 120)
p.write_stdin("echo hello >/data/out\nexit\n")
code = p.wait()
```

Volumes are named host directories under `VMON_HOME`, mounted into the guest with writable virtio-fs by default. They persist across sandbox restores and are excluded from filesystem snapshots; `Sandbox.create(volumes={"/data": Volume("agent_data")})` re-attaches the same volume by name. Each volume has a single-writer lock so two live VMs cannot write it at once.

Secrets are injected into exec environments and are never written to `meta.json`. Use `Secret.from_dict({...})` for explicit values or `Secret.from_env("TOKEN")` to copy selected host variables.

### Networking and tunnels

`Sandbox.create` accepts `block_network=True`, CIDR `egress_allow=[...]`, DNS-pinned `egress_allow_domains=[...]`, `ports=[...]`, and `inbound_cidr_allowlist=[...]`. Domain allowlists are resolved to IP firewall rules and refreshed during the sandbox lifetime; they are not live TLS-SNI filtering.

```python
web = Sandbox.create(
    template="web",
    ports=[8080],
    egress_allow=["10.0.0.0/8"],
    egress_allow_domains=["api.github.com"],
    inbound_cidr_allowlist=["203.0.113.0/24"],
)

token = web.create_connect_token()
print(web.tunnels())  # {8080: ("127.0.0.1", 49152)}
```

The REST server exposes the authenticated proxy at `/v1/sandboxes/{id}/ports/{port}/...`; pass the token as `Authorization: Bearer <token>` or `?token=...`.

### Snapshots, timeouts, warm pools, and async

```python
img = sb.snapshot_filesystem("img1")          # default TTL: 30 days
again = Sandbox.create(template=img)

fast = Sandbox.create(template="base", pool_size=4)
same = Sandbox.from_id(fast.name)
```

`Sandbox.create(timeout_secs=...)` passes a hard deadline to the VMM. If it expires, `status.json` records `reason:"timeout"` and return code `124`; explicit termination records `137`. Use `Sandbox.extend(secs)` or `POST /v1/sandboxes/{id}/extend` to move the deadline.

Warm pools keep pre-forked copy-on-write clones for a template and fall back to cold restore on a miss. Tags are stored with the sandbox (`Sandbox.create(tags={"team": "ci"})`) and the REST API can filter them with `GET /v1/sandboxes?tag=team:ci`.

`Sandbox.aio.*` mirrors the synchronous SDK using `asyncio.to_thread`, so async callers do not need a second API.

### REST API

Install the server extra, then run `vmon serve` — the same single-owner process as
the daemon, plus a FastAPI HTTP/web gateway over the same engine, so the local
`vmon` CLI and the REST API share one VM registry:

```sh
pip install -e 'python[server]'
VMON_API_TOKEN=secret vmon serve --host 0.0.0.0 --port 8000
```

The REST API covers sandbox create/list/attach, exec and pty WebSocket exec, snapshots, network policy, tunnels, authenticated port proxying, deadline extension, metrics, and lifecycle events. `GET /v1/events` streams lifecycle events as SSE, and FastAPI serves OpenAPI docs at `/docs`. A bearer token (`--token` or `VMON_API_TOKEN`) is required; the `core.Engine` underneath is dependency-free (the `[server]` extra only adds FastAPI/uvicorn for this gateway).


## How it works

- **run** — `skopeo` fetches and inspects OCI images; `umoci` unpacks the
  rootfs; vmon injects the static guest agent, builds an ext4 filesystem, boots
  it under `vmm`, and execs through the agent-backed sandbox API.
- **pause/resume** — vmon parks the vCPUs at a safe point and quiesces device
  workers over its Unix control socket.
- **snapshot** — serializes the full machine state (vCPU regs/MSRs/xstate, the
  interrupt controllers/timers, device + queue state), guest RAM, and virtio-fs inode/mode metadata.
- **restore** — reconstructs that state into a fresh VM (no kernel boot); a 256
  MiB Alpine template warm-boots in ~120 ms on x86 KVM.
- **fork** — maps the template's RAM `MAP_PRIVATE`, so every clone shares clean
  pages through the host page cache and only pays copy-on-write for what it
  touches (~22 MiB RSS per clone vs ~256 MiB for a full VM).
- **volumes** — attaches named virtio-fs host directories, writable by default. Volume data is not included in snapshots; restores re-attach the named host directory.

## Notes / limits

- The static guest agent is injected into prepared images, including distroless images, so Sandbox exec/filesystem/network RPCs do not depend on `/bin/sh`.
- The `vmon` CLI never touches `~/.vmon` or the VMM directly; it speaks JSON to the daemon over `~/.vmon/vmond.sock`. The per-VM `vmm` VMM and its many flags are an internal runtime detail spawned by the daemon. Remote access always goes through the HTTP gateway (`vmon serve`): create a context with `vmon context create` and select it with `vmon context use`.
- `VMON_BIN` / `VMON_KERNEL` / `VMON_HOME` override the binary, kernel, and
  state directory (`~/.vmon`).
