# Build from Source

The repository builds one Rust executable, `vmon`. That binary contains the operator CLI, `vmon serve` from the `vmond` crate, and the `vmon vmm` monitor. Building an SDK package is not a substitute for building the server.

## Required build tools

The repository recipes invoke Cargo, so install a Rust toolchain that provides `cargo` and install `just` before building. The panel is optional: only build it when the `vmon serve` instance should embed the web UI, and make Bun available for that step.

For Apple Silicon macOS user-mode guest networking, the native build also needs `libslirp` and `pkg-config`; the project documentation gives this installation example:

```sh
brew install libslirp pkg-config
```

Image-reference workflows and guest filesystem preparation have separate runtime requirements: `skopeo`, `umoci`, and `mkfs.ext4`. See [Installation](installation.md) for when those tools are required.

## Build with `just`

From the repository root, make a debug build:

```sh
just build
```

Make a release build:

```sh
just release
```

`just release` calls the `build` recipe with `PROFILE=release`; the underlying compile action is `cargo build --release`. The ordinary output paths are `target/debug/vmon` and `target/release/vmon`. Do not assume `target/` if `CARGO_TARGET_DIR` or Cargo's `build.target-dir` redirects it. Ask the recipe for the active path instead:

```sh
just profile=release bin
```

The project uses Linux/KVM on Linux, HVF on Apple Silicon macOS, and WHP on x86_64 Windows. The backend is selected at compile time. Separate `lima-*` recipes drive Linux/KVM through `limactl shell` from macOS.

## macOS signing

On macOS, both `just build` and `just release` run the `codesign` recipe after compilation. It ad-hoc signs the selected `vmon` binary with `hvf.entitlements`:

```sh
codesign --sign - --entitlements hvf.entitlements --force target/release/vmon
```

That entitlement file grants `com.apple.security.hypervisor`, which is required for Hypervisor.framework execution. It does not grant `com.apple.vm.networking`: restricted entitlements cannot be carried by the ad-hoc signature and cause the kernel to refuse to launch the binary. Therefore use `--net user` with libslirp for the normal local macOS networking path; `--tap` requires vmnet-style support that this binary cannot obtain through the ad-hoc signature.

If you compile with Cargo directly on macOS, run the equivalent build and signing sequence yourself:

```sh
cargo build --release
codesign --sign - --entitlements hvf.entitlements --force target/release/vmon
```

Linux builds do not require codesigning; the Linux `codesign` recipe reports that KVM does not need it.

## Windows build

Build from an x86_64 Windows developer shell with the Rust MSVC toolchain and Windows SDK installed:

```powershell
cargo build --release
```

The host must have Windows Hypervisor Platform or Hyper-V enabled to run a guest. The Windows user-network backend is built from the vendored libslirp sources, so it does not require a separately installed libslirp runtime. Control, agent, and remote-filesystem paths are converted into local named-pipe names at runtime.

## Build the embedded panel

The Rust server can run without the web assets, but embedding the panel requires a Bun build from the checkout:

```sh
cd ui && bun install && bun run build
cd ..
```

This writes the panel bundle to `vmond/web/`. Start the server from the built binary, using a bearer token for an operator endpoint:

```sh
./target/release/vmon serve --host 127.0.0.1 --port 8000 --token secret
```

The command above assumes the default release output path; substitute the path printed by `just profile=release bin` when the Cargo target directory is customized. For server configuration and binding guidance, see [Server Operation](server.md).

## Confirm the local environment

After building, run:

```sh
./target/release/vmon doctor
```

Use the resolved binary path instead if the target directory is customized, or set `VMON_BIN` for diagnostic lookup. The diagnostic checks the executable and platform prerequisites, including the macOS signing entitlement when applicable, and exits non-zero on hard failures. A successful server or UI build does not make a host ready to boot a microVM: Linux requires usable KVM, while macOS requires Apple Silicon HVF support and the signed binary. [Support Matrix](support-matrix.md) lists the host, guest, architecture, and networking constraints.
