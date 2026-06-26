#!/bin/bash
# Boot Linux under vmon (aarch64) with a virtio-blk ext4 disk and a
# virtio-net tap, exercising filesystem + network IO with a busybox initramfs.
# Run this INSIDE an arm64 Linux host that has /dev/kvm.
#
# Usage:  run-arm64-demo.sh [arm64-Image]
#   - binary:  $VMON, else auto-detected (cargo target dirs / PATH)
#   - kernel:  arg 1, else auto-extracted from /boot/vmlinuz-$(uname -r)
# Needs:  busybox, mkfs.ext4, cpio, iproute2, sudo (for /dev/kvm + tap).
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
WORK=${WORK:-/tmp/vmon-demo}
TAP=${TAP:-tap0}
mkdir -p "$WORK"; cd "$WORK"

find_vmon() {
  if [ -n "${VMON:-}" ] && [ -x "${VMON:-}" ]; then echo "$VMON"; return 0; fi
  local proj base
  proj=$(cd "$HERE/.." && pwd)
  # If the project lives under a mounted host home, the cargo target dir may be
  # redirected to <home>/.cache/cargo-target; derive that home prefix.
  base=$(printf '%s' "$proj" | sed -E 's#^(/Users/[^/]+|/home/[^/]+).*#\1#')
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
BIN=$(find_vmon) || {
  echo "error: vmon binary not found. Build it with:" >&2
  echo "  RUSTFLAGS=\"-C linker=<zig-cc-wrapper>\" cargo build --release --target aarch64-unknown-linux-gnu" >&2
  echo "or set VMON=/path/to/vmon" >&2; exit 1
}
echo "[demo] vmon: $BIN"

KERNEL=${1:-}
if [ -z "$KERNEL" ]; then
  KERNEL="$WORK/Image"
  [ -f "$KERNEL" ] || bash "$HERE/extract-arm64-image.sh" "/boot/vmlinuz-$(uname -r)" "$KERNEL"
fi
echo "[demo] kernel: $KERNEL"

echo "[demo] ext4 rootfs on virtio-blk"
rm -rf content && mkdir content
echo "Hello from the virtio-blk filesystem (read OK)" > content/MARKER.txt
rm -f disk.img; mkfs.ext4 -F -q -d content disk.img 64M

echo "[demo] busybox initramfs"
rm -rf irfs && mkdir -p irfs/bin && cp "$(command -v busybox)" irfs/bin/busybox
cat > irfs/init <<'EOF'
#!/bin/busybox sh
/bin/busybox --install -s /bin
PATH=/bin:/sbin:/usr/bin:/usr/sbin
export PATH

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
ip addr add 172.16.0.2/24 dev "$IF" || fail "assign 172.16.0.2 to $IF"
ok=
for i in 1 2 3 4 5; do
  ping -c1 -W2 172.16.0.1 >/dev/null 2>&1 && { ok=1; break; }
  sleep 1
done
[ -n "$ok" ] || fail "ping 172.16.0.1 after retries"
ping -c2 -W2 172.16.0.1 >/dev/null 2>&1 || fail "confirm ping 172.16.0.1"
echo "ping: OK"
echo "##### vmon demo PASS #####"
sync
poweroff -f 2>/dev/null || exit 1
exit 0
EOF
chmod +x irfs/init irfs/bin/busybox
( cd irfs && find . -print0 | cpio --null -o -H newc 2>/dev/null | gzip -1 > "$WORK/initramfs.cpio.gz" )

echo "[demo] (re)create host tap $TAP (172.16.0.1/24)"
sudo ip link del "$TAP" 2>/dev/null || true
sudo ip tuntap add "$TAP" mode tap
sudo ip addr add 172.16.0.1/24 dev "$TAP"
sudo ip link set "$TAP" up

echo "[demo] launching vmon (4 vCPUs, 1 GiB, virtio-blk + virtio-net)"
LOG="$WORK/arm64-demo.log"
rm -f "$LOG"
set +e
sudo timeout 60 "$BIN" \
  --kernel "$KERNEL" --initrd "$WORK/initramfs.cpio.gz" \
  --rootfs "$WORK/disk.img" --tap "$TAP" --mem 1024 --cpus 4 \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init" 2>&1 | tee "$LOG"
status=${PIPESTATUS[0]}
set -e
if [ "$status" -ne 0 ]; then
  echo "[demo] timeout/vmon exited with status $status" >&2
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
