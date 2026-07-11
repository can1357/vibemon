#!/bin/bash
# Boot Linux under vmm (aarch64) with a virtio-blk ext4 disk and
# virtio-net, exercising filesystem + network IO with a busybox initramfs.
# Runs on arm64 Linux/KVM and Apple-silicon macOS/HVF.
#
# Usage:  run-arm64-demo.sh [arm64-Image]
#   - binary:  $VMON_BIN, else auto-detected (cargo target dirs / PATH)
#   - kernel:  arg 1, else $VMON_KERNEL on macOS, else platform default
# Needs:  busybox, mkfs.ext4, cpio, iproute2, sudo (Linux only for /dev/kvm + tap).
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
WORK=${WORK:-/tmp/vmon-demo}
TAP=${TAP:-tap0}
HOST_OS=$(uname -s)
HOST_MACHINE=$(uname -m)
IS_DARWIN=0
if [ "$HOST_OS" = "Darwin" ]; then
  case "$HOST_MACHINE" in
    arm64 | aarch64) IS_DARWIN=1 ;;
    *) echo "error: macOS/HVF is supported only on Apple silicon; use Linux/KVM or Lima on Intel macOS." >&2; exit 1 ;;
  esac
elif [ "$HOST_OS" != "Linux" ]; then
  echo "error: unsupported host OS '$HOST_OS' (want Linux/KVM or Apple-silicon macOS/HVF)" >&2
  exit 1
fi
REPO=$(cd "$HERE/.." && pwd)
ASSET_DIR="$REPO/target/test-assets"
MACOS_KERNEL="$ASSET_DIR/Image-aarch64"
MACOS_BUSYBOX="$ASSET_DIR/busybox-aarch64"
MACOS_MUSL_LOADER="$ASSET_DIR/ld-musl-aarch64.so.1"
if [ "$IS_DARWIN" -eq 1 ]; then
  # Homebrew's e2fsprogs is keg-only; keep this macOS-only so Linux PATH
  # resolution stays exactly as before.
  export PATH="/opt/homebrew/opt/e2fsprogs/sbin:/usr/local/opt/e2fsprogs/sbin:/opt/homebrew/sbin:/usr/local/sbin:$PATH"
fi
mkdir -p "$WORK"; cd "$WORK"

find_vmm() {
  if [ -n "${VMON_BIN:-}" ] && [ -x "${VMON_BIN:-}" ]; then echo "$VMON_BIN"; return 0; fi
  local proj base
  proj="$REPO"
  # If the project lives under a mounted host home, the cargo target dir may be
  # redirected to <home>/.cache/cargo-target; derive that home prefix.
  base=$(printf '%s' "$proj" | sed -E 's#^(/Users/[^/]+|/home/[^/]+).*#\1#')
  local td
  td="${CARGO_TARGET_DIR:-}"
  [ -n "$td" ] || td=$(cd "$proj" && cargo metadata --no-deps --format-version 1 2>/dev/null \
    | python3 -c 'import json,sys;print(json.load(sys.stdin).get("target_directory",""))' 2>/dev/null || true)
  [ -n "$td" ] || td="$proj/target"
  if [ "$IS_DARWIN" -eq 1 ]; then
    for c in \
      "$td"/release/vmon "$td"/debug/vmon \
      "$td"/aarch64-apple-darwin/release/vmon "$td"/aarch64-apple-darwin/debug/vmon \
      "$proj/target/release/vmon" "$proj/target/debug/vmon" \
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
  echo "error: vmon binary not found. Build it with:" >&2
  if [ "$IS_DARWIN" -eq 1 ]; then
    echo "  just build" >&2
  else
    echo "  RUSTFLAGS=\"-C linker=<zig-cc-wrapper>\" cargo build --release --target aarch64-unknown-linux-gnu" >&2
  fi
  echo "or set VMON_BIN=/path/to/vmon" >&2; exit 1
}
echo "[demo] vmm: $BIN"

fetch_macos_assets() {
  echo "[demo] fetching missing macOS/HVF aarch64 test assets"
  VMON_ASSET_ARCH=aarch64 bash "$HERE/fetch-test-assets.sh"
}

ensure_macos_default_kernel() {
  [ -f "$MACOS_KERNEL" ] || fetch_macos_assets
}

ensure_macos_busybox() {
  if [ -f "$MACOS_BUSYBOX" ] && [ -f "$MACOS_MUSL_LOADER" ]; then
    return 0
  fi
  fetch_macos_assets
}

KERNEL=${1:-}
if [ -z "$KERNEL" ]; then
  if [ "$IS_DARWIN" -eq 1 ]; then
    if [ -n "${VMON_KERNEL:-}" ]; then
      KERNEL="$VMON_KERNEL"
    elif [ -f "$MACOS_KERNEL" ]; then
      KERNEL="$MACOS_KERNEL"
    elif [ -f "$HOME/.vmon/assets/Image-aarch64" ]; then
      KERNEL="$HOME/.vmon/assets/Image-aarch64"
    else
      ensure_macos_default_kernel
      KERNEL="$MACOS_KERNEL"
    fi
  else
    KERNEL="$WORK/Image"
    [ -f "$KERNEL" ] || bash "$HERE/extract-arm64-image.sh" "/boot/vmlinuz-$(uname -r)" "$KERNEL"
  fi
fi
if [ "$IS_DARWIN" -eq 1 ]; then
  ensure_macos_busybox
fi
echo "[demo] kernel: $KERNEL"

echo "[demo] ext4 rootfs on virtio-blk"
rm -rf content && mkdir content
echo "Hello from the virtio-blk filesystem (read OK)" > content/MARKER.txt
rm -f disk.img; mkfs.ext4 -F -q -d content disk.img 64M

echo "[demo] busybox initramfs"
rm -rf irfs && mkdir -p irfs/bin
if [ "$IS_DARWIN" -eq 1 ]; then
  cp "$MACOS_BUSYBOX" irfs/bin/busybox
  mkdir -p irfs/lib
  cp "$MACOS_MUSL_LOADER" "irfs/lib/$(basename "$MACOS_MUSL_LOADER")"
else
  cp "$(command -v busybox)" irfs/bin/busybox
fi
cat > irfs/init <<'EOF'
#!/bin/busybox sh
/bin/busybox --install -s /bin
PATH=/bin:/sbin:/usr/bin:/usr/sbin
export PATH
NETWORK_MODE=tap
[ -r /etc/vmon-demo-network-mode ] && . /etc/vmon-demo-network-mode

fail() {
  echo "vmon-demo: FAIL: $*" >&2
  sync
  poweroff -f 2>/dev/null || exit 1
  exit 1
}

mkdir -p /proc /sys /dev /mnt
mount -t proc proc /proc || fail "mount /proc"
mount -t sysfs sys /sys || fail "mount /sys"
mount -t devtmpfs dev /dev 2>/dev/null || true
echo; echo "##### vmon guest: $(uname -srm) #####"
blocks=$(ls /sys/block 2>/dev/null | tr '\n' ' ')
nets=$(ls /sys/class/net 2>/dev/null | tr '\n' ' ')
echo "cpus=$(nproc) block=$blocks net=$nets"

echo "--- virtio-blk ---"
mount -t ext4 /dev/vda /mnt || fail "mount /dev/vda"
marker=$(cat /mnt/MARKER.txt 2>/dev/null) || fail "read /mnt/MARKER.txt"
[ "$marker" = "Hello from the virtio-blk filesystem (read OK)" ] || fail "unexpected MARKER.txt contents"
echo "read: $marker"
printf '%s\n' "guest wrote this" > /mnt/W.txt || fail "write /mnt/W.txt"
sync || fail "sync after write"
umount /mnt || fail "umount after write"
mount -t ext4 /dev/vda /mnt || fail "remount /dev/vda"
readback=$(cat /mnt/W.txt 2>/dev/null) || fail "read back /mnt/W.txt"
[ "$readback" = "guest wrote this" ] || fail "unexpected /mnt/W.txt contents"
echo "read-back: $readback"
umount /mnt || fail "final umount"

echo "--- virtio-net ---"
IF=
for dev in /sys/class/net/*; do
  [ -d "$dev" ] || continue
  name=${dev##*/}
  [ "$name" = "lo" ] && continue
  IF=$name
  break
done
[ -n "$IF" ] || fail "no non-loopback network interface"
ip link set "$IF" up || fail "bring up $IF"
if [ "$NETWORK_MODE" = "user" ]; then
  ip addr add 10.0.2.15/24 dev "$IF" || fail "assign 10.0.2.15 to $IF"
  ip route add default via 10.0.2.2 dev "$IF" 2>/dev/null || true
  ok=
  for i in 1 2 3 4 5; do
    ping -c1 -W2 10.0.2.2 >/dev/null 2>&1 && { ok=1; break; }
    sleep 1
  done
  [ -n "$ok" ] || fail "ping slirp gateway 10.0.2.2 after retries"
  ping -c2 -W2 10.0.2.2 >/dev/null 2>&1 || fail "confirm ping slirp gateway 10.0.2.2"
  echo "ping slirp gateway: OK"
else
  ip addr add 172.16.0.2/24 dev "$IF" || fail "assign 172.16.0.2 to $IF"
  ok=
  for i in 1 2 3 4 5; do
    ping -c1 -W2 172.16.0.1 >/dev/null 2>&1 && { ok=1; break; }
    sleep 1
  done
  [ -n "$ok" ] || fail "ping 172.16.0.1 after retries"
  ping -c2 -W2 172.16.0.1 >/dev/null 2>&1 || fail "confirm ping 172.16.0.1"
  echo "ping: OK"
fi
echo "##### vmon demo PASS #####"
sync
poweroff -f 2>/dev/null || exit 1
exit 0
EOF
chmod +x irfs/init irfs/bin/busybox
if [ "$IS_DARWIN" -eq 1 ]; then
  mkdir -p irfs/etc
  echo 'NETWORK_MODE=user' > irfs/etc/vmon-demo-network-mode
fi
if [ "$IS_DARWIN" -eq 1 ]; then
  ( cd irfs && find . -print | LC_ALL=C sort | cpio -o -H newc 2>/dev/null | gzip -1 > "$WORK/initramfs.cpio.gz" )
else
  ( cd irfs && find . -print0 | cpio --null -o -H newc 2>/dev/null | gzip -1 > "$WORK/initramfs.cpio.gz" )
fi

LOG="$WORK/arm64-demo.log"
rm -f "$LOG"
if [ "$IS_DARWIN" -eq 1 ]; then
  echo "[demo] launching vmm (4 vCPUs, 1 GiB, virtio-blk + virtio-net user)"
  TIMEOUT=()
  if command -v timeout >/dev/null 2>&1; then
    TIMEOUT=(timeout 60)
  elif command -v gtimeout >/dev/null 2>&1; then
    TIMEOUT=(gtimeout 60)
  else
    echo "[demo] warning: timeout/gtimeout not found; running vmm without a host-side timeout" >&2
  fi
  set +e
  "${TIMEOUT[@]}" "$BIN" vmm \
    --kernel "$KERNEL" --initrd "$WORK/initramfs.cpio.gz" \
    --rootfs "$WORK/disk.img" --net user --mem 1024 --cpus 4 \
    --cmdline "console=ttyS0 reboot=t panic=-1 earlycon=uart8250,mmio,0x9000000 rdinit=/init" 2>&1 | tee "$LOG"
  status=${PIPESTATUS[0]}
  set -e
else
  echo "[demo] (re)create host tap $TAP (172.16.0.1/24)"
  sudo ip link del "$TAP" 2>/dev/null || true
  sudo ip tuntap add "$TAP" mode tap
  sudo ip addr add 172.16.0.1/24 dev "$TAP"
  sudo ip link set "$TAP" up

  echo "[demo] launching vmm (4 vCPUs, 1 GiB, virtio-blk + virtio-net)"
  set +e
  sudo timeout 60 "$BIN" vmm \
    --kernel "$KERNEL" --initrd "$WORK/initramfs.cpio.gz" \
    --rootfs "$WORK/disk.img" --tap "$TAP" --mem 1024 --cpus 4 \
    --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init" 2>&1 | tee "$LOG"
  status=${PIPESTATUS[0]}
  set -e
fi
set -e
if [ "$status" -ne 0 ]; then
  echo "[demo] timeout/vmm exited with status $status" >&2
  exit "$status"
fi
if grep -q 'vmon-demo: FAIL:' "$LOG"; then
  echo "[demo] guest reported failed checks" >&2
  exit 1
fi
if ! grep -q '##### vmon demo PASS #####' "$LOG"; then
  echo "[demo] guest did not report PASS" >&2
  exit 1
fi
echo "[demo] guest checks passed"
