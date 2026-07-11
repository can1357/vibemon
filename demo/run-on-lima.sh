#!/bin/bash
# Host-side (macOS) launcher: runs a vmon demo INSIDE a Lima Linux/KVM VM
# via `limactl shell`. The project dir is visible at the same path in the guest
# (Lima home mount), so the in-guest scripts auto-locate the binary and kernel.
#
# Native macOS/HVF support is available on Apple silicon for aarch64 guests:
# build the local `vmon` binary with `just build`, then prefer the native demo
# scripts when you do not need Lima. Keep this wrapper for x86_64 guests,
# KVM coverage, or Linux-only features such as TAP networking.
#
# Usage:  run-on-lima.sh [demo|ubuntu] [vm-name] [extra args -> guest script]
#   demo    virtio-blk + virtio-net busybox demo (default)
#   ubuntu  real Ubuntu 24.04 rootfs on virtio-blk
#   vm-name Lima instance (default: kvm)
#
# First-time VM setup (Apple silicon, macOS 15+/M3+):
#   limactl start --vm-type=vz --set='.nestedVirtualization=true' --name=kvm template:default
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
PROJ=$(cd "$HERE/.." && pwd)
WHICH=${1:-demo}
VM=${2:-kvm}
[ $# -ge 1 ] && shift || true
[ $# -ge 1 ] && shift || true   # remaining args forwarded to the guest script

case "$WHICH" in
  demo)   SCRIPT="$HERE/run-arm64-demo.sh" ;;
  ubuntu) SCRIPT="$HERE/run-arm64-ubuntu.sh" ;;
  *) echo "usage: $(basename "$0") [demo|ubuntu] [vm-name]"; exit 1 ;;
esac

command -v limactl >/dev/null || { echo "error: limactl not found (brew install lima)"; exit 1; }
if ! limactl list -q 2>/dev/null | grep -qx "$VM"; then
  echo "error: lima VM '$VM' not found. Create one with nested KVM:" >&2
  echo "  limactl start --vm-type=vz --set='.nestedVirtualization=true' --name=$VM template:default" >&2
  exit 1
fi

# Build the aarch64 binary on the host if it's missing and zig is available.
BIN="${CARGO_TARGET_DIR:-$HOME/.cache/cargo-target}/aarch64-unknown-linux-gnu/release/vmon"
BIN_PROJ="$PROJ/target/aarch64-unknown-linux-gnu/release/vmon"
[ -x "$BIN" ] || BIN="$BIN_PROJ"
if [ ! -x "$BIN" ]; then
  ZIGBIN=${ZIG:-$(command -v zig 2>/dev/null || true)}
  if [ -n "$ZIGBIN" ]; then
    echo "[lima] building aarch64 vmon with zig as cross-linker"
    WRAP=$(mktemp)
    printf '#!/bin/sh\nexec %s cc -target aarch64-linux-gnu "$@"\n' "$ZIGBIN" > "$WRAP"
    chmod +x "$WRAP"
    ( cd "$PROJ" && RUSTFLAGS="-C linker=$WRAP" cargo build --release --target aarch64-unknown-linux-gnu )
    rm -f "$WRAP"
  else
    echo "[lima] no prebuilt binary and no zig found; the guest script will report how to build." >&2
  fi
fi

echo "[lima] running '$WHICH' demo in VM '$VM' via limactl shell"
exec limactl shell "$VM" -- bash "$SCRIPT" "$@"
