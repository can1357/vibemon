#!/bin/bash
# Boot a real Ubuntu 24.04 arm64 root filesystem under vmm on virtio-blk,
# proving a full distro userspace (not just the kernel). Runs on arm64 Linux
# hosts with /dev/kvm and on Apple-silicon macOS hosts with HVF.
#
# Usage:  run-arm64-ubuntu.sh [arm64-Image]
#   - binary:  $VMON_BIN, else auto-detected (cargo target dirs / PATH)
#   - kernel:  arg 1 / $VMON_KERNEL, else host-appropriate arm64 Image
# Needs:  curl, mkfs.ext4; Linux also needs sudo.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
HOST_OS=$(uname -s)
HOST_ARCH=$(uname -m)
if [ "$HOST_OS" = "Darwin" ]; then
  case "$HOST_ARCH" in
    arm64 | aarch64) ;;
    *) echo "error: Intel macOS is unsupported for this arm64/HVF demo; use Linux/KVM or Lima." >&2; exit 1 ;;
  esac
  # Homebrew's e2fsprogs is keg-only, so mkfs.ext4 is not on macOS' default PATH.
  export PATH="/opt/homebrew/opt/e2fsprogs/sbin:/usr/local/opt/e2fsprogs/sbin:/opt/homebrew/sbin:/usr/local/sbin:$PATH"
fi
WORK=${WORK:-/tmp/vmon-demo}
mkdir -p "$WORK"; cd "$WORK"

find_vmm() {
  if [ -n "${VMON_BIN:-}" ] && [ -x "${VMON_BIN:-}" ]; then echo "$VMON_BIN"; return 0; fi
  local proj base td
  proj=$(cd "$HERE/.." && pwd)
  base=$(printf '%s' "$proj" | sed -E 's#^(/Users/[^/]+|/home/[^/]+).*#\1#')
  # Resolve cargo's target dir (honors .cargo/config build.target-dir and
  # $CARGO_TARGET_DIR), matching the places the Rust CLI and just recipes use
  # so the demo finds the same vmon binary.
  td="${CARGO_TARGET_DIR:-}"
  [ -n "$td" ] || td=$(cd "$proj" && cargo metadata --no-deps --format-version 1 2>/dev/null \
    | python3 -c 'import json,sys;print(json.load(sys.stdin).get("target_directory",""))' 2>/dev/null || true)
  [ -n "$td" ] || td="$proj/target"
  if [ "$HOST_OS" = "Darwin" ]; then
    for c in \
      "$td"/release/vmon "$td"/debug/vmon \
      "$td"/aarch64-apple-darwin/release/vmon "$td"/aarch64-apple-darwin/debug/vmon \
      "$proj"/target/*/vmon \
      "$(command -v vmon 2>/dev/null || true)"; do
      [ -n "$c" ] && [ -x "$c" ] && { echo "$c"; return 0; }
    done
    return 1
  fi
  for c in \
    "$proj/target/aarch64-unknown-linux-gnu/release/vmon" \
    "${CARGO_TARGET_DIR:-/nonexistent}/aarch64-unknown-linux-gnu/release/vmon" \
    "$base/.cache/cargo-target/aarch64-unknown-linux-gnu/release/vmon" \
    "$HOME/.cache/cargo-target/aarch64-unknown-linux-gnu/release/vmon" \
    "$(command -v vmon 2>/dev/null || true)"; do
    [ -n "$c" ] && [ -x "$c" ] && { echo "$c"; return 0; }
  done
  return 1
}
BIN=$(find_vmm) || {
  if [ "$HOST_OS" = "Darwin" ]; then
    echo "error: vmon binary not found. Build the ad-hoc-signed HVF binary with:" >&2
    echo "  just build" >&2
  else
    echo "error: vmon binary not found. Build it with:" >&2
    echo "  RUSTFLAGS=\"-C linker=<zig-cc-wrapper>\" cargo build --release --target aarch64-unknown-linux-gnu" >&2
  fi
  echo "or set VMON_BIN=/path/to/vmon" >&2; exit 1
}
echo "[demo] vmon: $BIN"

KERNEL=${1:-${VMON_KERNEL:-}}
if [ -z "$KERNEL" ]; then
  if [ "$HOST_OS" = "Darwin" ]; then
    PROJ=$(cd "$HERE/.." && pwd)
    TARGET_KERNEL="$PROJ/target/test-assets/Image-aarch64"
    HOME_KERNEL="$HOME/.vmon/assets/Image-aarch64"
    if [ -f "$TARGET_KERNEL" ]; then
      KERNEL="$TARGET_KERNEL"
    else
      echo "[demo] fetching pinned arm64 test kernel for macOS/HVF"
      if VMON_ASSET_ARCH=aarch64 ASSET_DIR="$PROJ/target/test-assets" bash "$HERE/fetch-test-assets.sh"; then
        KERNEL="$TARGET_KERNEL"
      elif [ -f "$HOME_KERNEL" ]; then
        KERNEL="$HOME_KERNEL"
      else
        echo "error: arm64 Image kernel not found; set VMON_KERNEL or run demo/fetch-test-assets.sh" >&2
        exit 1
      fi
    fi
  else
    KERNEL="$WORK/Image"
    [ -f "$KERNEL" ] || bash "$HERE/extract-arm64-image.sh" "/boot/vmlinuz-$(uname -r)" "$KERNEL"
  fi
fi
[ -f "$KERNEL" ] || { echo "error: kernel not found: $KERNEL" >&2; exit 1; }
echo "[demo] kernel: $KERNEL"

if [ ! -f ubuntu-base.tar.gz ]; then
  BASE=https://cdimage.ubuntu.com/ubuntu-base/releases/24.04/release
  NAME=$(curl -fsSL "$BASE/" | grep -oE 'ubuntu-base-24\.04[0-9.]*-base-arm64\.tar\.gz' | head -1)
  echo "[demo] downloading $NAME"; curl -fsSL -o ubuntu-base.tar.gz "$BASE/$NAME"
fi

echo "[demo] building Ubuntu rootfs image"
if [ "$HOST_OS" = "Darwin" ] && ! command -v mkfs.ext4 >/dev/null 2>&1; then
  echo "error: mkfs.ext4 not found; install it with: brew install e2fsprogs" >&2
  exit 1
fi
if [ "$HOST_OS" = "Darwin" ]; then
  SUDO=
else
  SUDO=sudo
fi
$SUDO rm -rf uroot && $SUDO mkdir uroot && $SUDO tar -xzf ubuntu-base.tar.gz -C uroot
$SUDO tee uroot/vmon-init.sh >/dev/null <<'EOF'
#!/bin/sh
PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH

fail() {
  echo "vmon-ubuntu: FAIL: $*" >&2
  sync
  poweroff -f 2>/dev/null || {
    [ -w /proc/sysrq-trigger ] && echo o > /proc/sysrq-trigger 2>/dev/null
  }
  exit 1
}

shutdown_guest() {
  sync || fail "sync before poweroff"
  if poweroff -f 2>/dev/null; then
    exit 0
  fi
  if [ ! -w /proc/sysrq-trigger ]; then
    echo "vmon-ubuntu: FAIL: poweroff failed and sysrq fallback unavailable" >&2
    exit 1
  fi
  if ! echo o > /proc/sysrq-trigger 2>/dev/null; then
    echo "vmon-ubuntu: FAIL: poweroff fallback failed" >&2
    exit 1
  fi
  exit 0
}

mount -t proc proc /proc || fail "mount /proc"
mount -t sysfs sys /sys || fail "mount /sys"
mount -t devtmpfs dev /dev 2>/dev/null || true
echo; echo "##### REAL UBUNTU USERSPACE UNDER vmon #####"
os_name=$(grep '^PRETTY_NAME=' /etc/os-release 2>/dev/null) || fail "read /etc/os-release"
[ -n "$os_name" ] || fail "empty PRETTY_NAME in /etc/os-release"
echo "$os_name"
echo "kernel: $(uname -srm)  cpus: $(nproc)"
dpkg_info=$(dpkg --version 2>/dev/null) || fail "run dpkg --version"
dpkg_line=$(printf '%s\n' "$dpkg_info" | sed -n '1p')
[ -n "$dpkg_line" ] || fail "empty dpkg version"
echo "dpkg: $dpkg_line"
grep -q ' / ' /proc/mounts || fail "root mount missing from /proc/mounts"
grep ' / ' /proc/mounts
echo "##### vmon ubuntu PASS #####"
shutdown_guest
EOF
$SUDO chmod +x uroot/vmon-init.sh
$SUDO rm -f ubuntu.img; $SUDO mkfs.ext4 -F -q -d uroot ubuntu.img 2G; $SUDO chmod a+rw ubuntu.img

echo "[demo] launching vmm with Ubuntu 24.04 root on virtio-blk"
LOG="$WORK/arm64-ubuntu.log"
rm -f "$LOG"
set +e
if [ "$HOST_OS" = "Darwin" ]; then
  CMDLINE="console=ttyS0 earlycon=uart8250,mmio,0x9000000 root=/dev/vda rw init=/vmon-init.sh reboot=t panic=-1 sysrq_always_enabled=1"
  if command -v timeout >/dev/null 2>&1; then
    timeout 90 "$BIN" vmm \
      --kernel "$KERNEL" --rootfs "$WORK/ubuntu.img" --mem 1024 --cpus 2 \
      --cmdline "$CMDLINE" 2>&1 | tee "$LOG"
    status=${PIPESTATUS[0]}
  else
    "$BIN" vmm \
      --kernel "$KERNEL" --rootfs "$WORK/ubuntu.img" --mem 1024 --cpus 2 \
      --cmdline "$CMDLINE" 2>&1 | tee "$LOG"
    status=${PIPESTATUS[0]}
  fi
else
  sudo timeout 90 "$BIN" vmm \
    --kernel "$KERNEL" --rootfs "$WORK/ubuntu.img" --mem 1024 --cpus 2 \
    --cmdline "console=ttyS0 root=/dev/vda rw init=/vmon-init.sh reboot=t panic=-1 sysrq_always_enabled=1" 2>&1 | tee "$LOG"
  status=${PIPESTATUS[0]}
fi
set -e
if [ "$status" -ne 0 ]; then
  echo "[demo] timeout/vmm exited with status $status" >&2
  exit "$status"
fi
if grep -q 'vmon-ubuntu: FAIL:' "$LOG"; then
  echo "[demo] guest reported failed checks" >&2
  exit 1
fi
if ! grep -q '##### vmon ubuntu PASS #####' "$LOG"; then
  echo "[demo] guest did not report PASS" >&2
  exit 1
fi
echo "[demo] Ubuntu guest checks passed"
