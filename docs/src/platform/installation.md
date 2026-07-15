# Installation

Install Vibemon by building the Rust `vmon` executable from this repository. The executable is both the operator CLI and the `vmon serve` server; installing an SDK package does **not** install a server. Build instructions are in [Build from Source](build-from-source.md).

This page separates an API/panel deployment from a host that will launch local Linux microVMs. Start with the former when the server is only exposing its control plane; add the VM requirements only where a local VMM will run.

## Base operator prerequisites

A source build uses Cargo through the repository's `just` recipes, so the build host needs a Rust toolchain with `cargo` and the `just` command runner. The normal release build is:

```sh
just release
```

The resulting executable is ordinarily `target/release/vmon`. `CARGO_TARGET_DIR` or Cargo's `build.target-dir` configuration can relocate it; use the following recipe to print the resolved location for the selected profile:

```sh
just profile=release bin
```

Run the following after the binary is available:

```sh
vmon doctor
```

`vmon doctor` checks the local executable, hypervisor availability, image tooling, filesystem formatter, guest kernel and agent, daemon socket, and host environment. It exits non-zero for hard failures. On macOS it additionally checks that the executable has the Hypervisor entitlement. If the binary is not in the diagnostic's search locations, set `VMON_BIN` to its executable path before running the command.

## API and panel only

`vmon serve` is the Rust `vmond` server (axum plus tonic), not a Python process. It provides the gRPC API over native h2c and its WebSocket bridge, and retains HTTP endpoints for health, metrics, and port proxying. It can be run without local KVM or HVF when no local microVM launch is required.

To embed the web panel, build the UI with Bun from the repository checkout; this writes the compiled assets to `vmond/web/`:

```sh
cd ui && bun install && bun run build
cd ..
vmon serve --host 127.0.0.1 --port 8000 --token secret
```

Replace `secret` with an operator bearer token appropriate for the deployment. A token is required for a non-loopback bind: the server diagnostic reports a hard failure without one. A missing token on loopback TCP or a Unix-domain socket is a warning because those endpoints are local-only.

For a resolved server configuration—including token, TLS, replication, advertise, restore-quorum, and warm-image checks—run:

```sh
vmon doctor --serve --config PATH
```

Supply the path to the configuration file in place of `PATH`. A TLS certificate and key must be configured together or neither is configured. See [Server Operation](server.md) and [Configuration](configuration.md) for server details.

## Additional prerequisites for local microVMs

A host that will actually boot a guest needs a supported hypervisor and guest assets in addition to the base build requirements.

### Linux/KVM

Use Linux with `/dev/kvm` present and readable/writable. The diagnostic's remediation is to enable KVM, add the operator to the `kvm` group, and log in again. The project's Linux `just run` recipe runs the executable with `sudo`, because its KVM and TAP-networking path requires root privileges. A default-sandboxed root launch must name a non-root sandbox UID and GID. Run this from the unprivileged account that can read every supplied guest asset; the substitutions preserve that account's access after the recipe elevates:

```sh
just run vmm --sandbox-uid "$(id -u)" --sandbox-gid "$(id -g)" --kernel <kernel-image> --initrd <initramfs.cpio.gz> --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"
```

For `--tap`, create and authorize the TAP networking setup as a privileged Linux operation before starting the VMM. Linux guests use the host architecture; see [Support Matrix](support-matrix.md) for kernel formats and device support.

### Apple Silicon macOS/HVF

Use macOS 15 or later on Apple Silicon with Hypervisor.framework available. `vmon doctor` requires `kern.hv_support=1`. The binary must have `com.apple.security.hypervisor`; the `just build` and `just release` recipes ad-hoc sign it automatically. HVF itself needs no root privilege.

For macOS user-mode guest networking, install native `libslirp` and `pkg-config` locally (for example, `brew install libslirp pkg-config`) and use `--net user`. The ad-hoc-signed binary cannot use `--tap`: vmnet-style networking requires entitlements unavailable to that signing mode. See [Build from Source](build-from-source.md) for the manual signing equivalent.

### x86_64 Windows/WHP

Use x86_64 Windows with hardware virtualization and Windows Hypervisor Platform or Hyper-V enabled. Build with the Rust MSVC toolchain and Windows SDK. Use `--net user` for in-process outbound NAT, or attach an operator-managed TAP-Windows adapter.

Windows control and guest-agent endpoints use local named pipes. Remote virtio-fs uses the same request framing over named pipes. Direct-kernel and UEFI boot are supported; supply x86_64 OVMF/EDK2 firmware with `--boot-mode uefi --firmware <path>`.

### Image and guest filesystem tools

`vmon doctor` warns when image tools are absent and names `skopeo` and `umoci`; install both before using `vmon run` with image references. It also checks for `mkfs.ext4`. On macOS, its suggested installation is `brew install e2fsprogs`; on other platforms it advises installing `e2fsprogs`.

The diagnostic accepts `VMON_KERNEL` when it points at an existing bootable guest kernel. Otherwise, it reports an available cached kernel or warns that the first macOS boot auto-downloads a pinned kernel; on non-macOS hosts, supply `VMON_KERNEL` as needed. It also checks for the bundled static guest agent and suggests `just agent-musl` when missing.

## Deployment options

Deployment files live under `deploy/`.

### Single node

The systemd installer targets a dedicated Linux host with KVM:

```sh
sudo ./deploy/single-node/install.sh ./target/release/vmon
```

It installs the binary and service, creates `/etc/vmon/serve.toml` with a random admin token, and records an unprivileged UID/GID for the VMM sandbox. Re-running it keeps the existing configuration. Remove the service with:

```sh
sudo ./deploy/single-node/uninstall.sh
# Also delete /etc/vmon and /var/lib/vmon:
sudo ./deploy/single-node/uninstall.sh --purge
```

The Compose deployment includes a separate rootless BuildKit service for Dockerfile builds:

```sh
export VMON_API_TOKEN="$(openssl rand -hex 32)"
docker compose -f deploy/single-node/docker-compose.yml up -d
```

The `vmon` container needs `/dev/kvm`, `/dev/net/tun`, host networking, and elevated network capabilities. It is intended for a dedicated virtualization host, not a shared container cluster. A systemd install can use an operator-managed BuildKit daemon by setting `VMON_BUILDKIT_ADDR` in `/etc/default/vmon`; OCI image pulls do not require BuildKit.

### Kubernetes cluster

The Helm chart runs one stateful `vmon serve` pod per KVM host. Each pod serves the API and launches local VMM children. The chart uses stable pod DNS for mesh membership, PostgreSQL for authoritative cluster records, S3-compatible storage for portable artifacts, and a separate rootless BuildKit pod for Dockerfile builds.

Label the KVM nodes before installation:

```sh
kubectl label node worker-a worker-b worker-c virtualization=kvm
```

Then install with non-default credentials:

```sh
helm install vmon ./deploy/helm/vmon \
  --set nodes.apiToken="$(openssl rand -hex 32)" \
  --set postgresql.auth.password="replace-this-password" \
  --set s3Storage.auth.accessKey="replace-this-access-key" \
  --set s3Storage.auth.secretKey="replace-this-secret-key"
```

The bundled PostgreSQL and MinIO workloads are for evaluation. For production, set `postgresql.enabled=false` and `s3Storage.enabled=false`, then fill `externalDatabase` and `externalS3`. Set `buildkit.enabled=false` and `buildkit.address` to use an operator-managed builder.

`just validate-deploy` checks the shell scripts, Compose model, rendered Helm chart, and Kubernetes schemas. The full value contract is in `deploy/helm/vmon/values.schema.json`.
