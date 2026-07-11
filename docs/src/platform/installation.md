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

### Image and guest filesystem tools

`vmon doctor` warns when image tools are absent and names `skopeo` and `umoci`; install both before using `vmon run` with image references. It also checks for `mkfs.ext4`. On macOS, its suggested installation is `brew install e2fsprogs`; on other platforms it advises installing `e2fsprogs`.

The diagnostic accepts `VMON_KERNEL` when it points at an existing bootable guest kernel. Otherwise, it reports an available cached kernel or warns that the first macOS boot auto-downloads a pinned kernel; on non-macOS hosts, supply `VMON_KERNEL` as needed. It also checks for the bundled static guest agent and suggests `just agent-musl` when missing.
