#!/bin/bash
# Boot a real Ubuntu 24.04 arm64 root filesystem under vmm on virtio-blk,
# proving a full distro userspace (not just the kernel). Run INSIDE an arm64
# Linux host with /dev/kvm.
#
# Usage:  run-arm64-ubuntu.sh [arm64-Image]
#   - binary:  $VMON_BIN, else auto-detected (cargo target dirs / PATH)
#   - kernel:  arg 1, else auto-extracted from /boot/vmlinuz-$(uname -r)
# Needs:  curl, mkfs.ext4, sudo.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
WORK=${WORK:-/tmp/vmon-demo}
mkdir -p "$WORK"; cd "$WORK"

find_vmm() {
  if [ -n "${VMON_BIN:-}" ] && [ -x "${VMON_BIN:-}" ]; then echo "$VMON_BIN"; return 0; fi
  local proj base
  proj=$(cd "$HERE/.." && pwd)
  base=$(printf '%s' "$proj" | sed -E 's#^(/Users/[^/]+|/home/[^/]+).*#\1#')
  for c in \
    "$proj/target/aarch64-unknown-linux-gnu/release/vmm" \
    "${CARGO_TARGET_DIR:-/nonexistent}/aarch64-unknown-linux-gnu/release/vmm" \
    "$base/.cache/cargo-target/aarch64-unknown-linux-gnu/release/vmm" \
    "$HOME/.cache/cargo-target/aarch64-unknown-linux-gnu/release/vmm" \
    "$(command -v vmm 2>/dev/null || true)"; do
    [ -n "$c" ] && [ -x "$c" ] && { echo "$c"; return 0; }
  done
  return 1
}
BIN=$(find_vmm) || {
  echo "error: vmm binary not found. Build it with:" >&2
  echo "  RUSTFLAGS=\"-C linker=<zig-cc-wrapper>\" cargo build --release --target aarch64-unknown-linux-gnu" >&2
  echo "or set VMON_BIN=/path/to/vmm" >&2; exit 1
}
echo "[demo] vmm: $BIN"

KERNEL=${1:-}
if [ -z "$KERNEL" ]; then
  KERNEL="$WORK/Image"
  [ -f "$KERNEL" ] || bash "$HERE/extract-arm64-image.sh" "/boot/vmlinuz-$(uname -r)" "$KERNEL"
fi
echo "[demo] kernel: $KERNEL"

if [ ! -f ubuntu-base.tar.gz ]; then
  BASE=https://cdimage.ubuntu.com/ubuntu-base/releases/24.04/release
  NAME=$(curl -fsSL "$BASE/" | grep -oE 'ubuntu-base-24\.04[0-9.]*-base-arm64\.tar\.gz' | head -1)
  echo "[demo] downloading $NAME"; curl -fsSL -o ubuntu-base.tar.gz "$BASE/$NAME"
fi

echo "[demo] building Ubuntu rootfs image"
sudo rm -rf uroot && sudo mkdir uroot && sudo tar -xzf ubuntu-base.tar.gz -C uroot
sudo tee uroot/vmon-init.sh >/dev/null <<'EOF'
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
sudo chmod +x uroot/vmon-init.sh
sudo rm -f ubuntu.img; sudo mkfs.ext4 -F -q -d uroot ubuntu.img 2G; sudo chmod a+rw ubuntu.img

echo "[demo] launching vmm with Ubuntu 24.04 root on virtio-blk"
LOG="$WORK/arm64-ubuntu.log"
rm -f "$LOG"
set +e
sudo timeout 90 "$BIN" \
  --kernel "$KERNEL" --rootfs "$WORK/ubuntu.img" --mem 1024 --cpus 2 \
  --cmdline "console=ttyS0 root=/dev/vda rw init=/vmon-init.sh reboot=t panic=-1 sysrq_always_enabled=1" 2>&1 | tee "$LOG"
status=${PIPESTATUS[0]}
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
