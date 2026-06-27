#!/usr/bin/env bash
# Boot a real Ubuntu 24.04 cloud image under vmm through operator-supplied
# UEFI firmware (OVMF on x86_64, QEMU_EFI.fd on aarch64) to a serial login,
# proving the full firmware -> bootloader -> kernel -> userspace chain rather
# than vmm's direct-kernel path. Use Linux/KVM on x86_64/aarch64, or
# Apple-silicon macOS/HVF for the aarch64 guest path.
#
# Usage:  run-uefi-ubuntu.sh
#   - binary:    $VMON_BIN, else auto-detected (cargo target dirs / PATH)
#   - firmware:  $VMON_X86_UEFI / $VMON_AARCH64_UEFI, else a distro/Homebrew
#                path, else downloaded from the EDK2 nightly (see "firmware" below)
#   - image:     pinned Ubuntu 24.04 cloud image for the host architecture
# Needs:  curl, qemu-img (qemu-utils; on macOS: brew install qemu),
#         sha256sum|shasum, timeout|gtimeout, and sudo only on Linux for /dev/kvm.
# Optional:  virt-customize (libguestfs-tools) to set a root password and a
#            serial autologin so you land in a shell instead of a login prompt;
#            enable with VMON_UEFI_CUSTOMIZE=1.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
WORK=${WORK:-/tmp/vmon-uefi}
MEM=${MEM:-2048}
CPUS=${CPUS:-2}
TIMEOUT=${TIMEOUT:-240}
HOST_OS=$(uname -s)
HOST_MACHINE=$(uname -m)
case "$HOST_OS" in
  Linux)
    SUDO=(sudo)
    TIMEOUT_CANDIDATES=(timeout)
    ;;
  Darwin)
    SUDO=()
    TIMEOUT_CANDIDATES=(gtimeout timeout)
    # Homebrew installs qemu-img/coreutils outside the system PATH in some shells.
    export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
    ;;
  *) echo "error: unsupported host OS $HOST_OS (use Linux/KVM or Apple-silicon macOS/HVF)" >&2; exit 1 ;;
esac

mkdir -p "$WORK"; cd "$WORK"

# qemu helpers commonly live in /sbin or /usr/sbin on Linux hosts.
export PATH="$PATH:/usr/sbin:/sbin"

# --- preconditions ----------------------------------------------------------
missing=0
need() {  # need <command> <install-hint>
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required tool '$1' not found on PATH" >&2
    echo "       hint: $2" >&2
    missing=1
  fi
}
if [ "$HOST_OS" = Darwin ]; then
  need curl "curl is included with macOS; install Command Line Tools or run: brew install curl"
  need qemu-img "brew install qemu"
else
  need curl "apt-get install curl  |  dnf install curl"
  need qemu-img "apt-get install qemu-utils  |  dnf install qemu-img"
  need sudo "run as a user that can sudo for /dev/kvm access"
fi
find_timeout() {
  local c
  for c in "${TIMEOUT_CANDIDATES[@]}"; do
    if command -v "$c" >/dev/null 2>&1; then command -v "$c"; return 0; fi
  done
  return 1
}
TIMEOUT_BIN=$(find_timeout) || {
  if [ "$HOST_OS" = Darwin ]; then
    echo "error: required tool 'gtimeout' not found on PATH" >&2
    echo "       hint: brew install coreutils" >&2
  else
    echo "error: required tool 'timeout' not found on PATH" >&2
    echo "       hint: install GNU coreutils" >&2
  fi
  missing=1
  TIMEOUT_BIN=timeout
}
if command -v sha256sum >/dev/null 2>&1; then
  SHA256_TOOL=sha256sum
elif command -v shasum >/dev/null 2>&1; then
  SHA256_TOOL="shasum -a 256"
else
  echo "error: need 'sha256sum' or 'shasum' on PATH" >&2
  if [ "$HOST_OS" = Darwin ]; then
    echo "       hint: shasum ships with macOS; alternatively run: brew install coreutils" >&2
  else
    echo "       hint: install coreutils or perl-Digest-SHA" >&2
  fi
  missing=1
  SHA256_TOOL=
fi
[ "$missing" -eq 0 ] || { echo "error: install the missing tool(s) above, then re-run" >&2; exit 1; }

calc_sha256() {  # calc_sha256 <path>
  local sum
  read -r sum _ < <($SHA256_TOOL "$1")
  printf '%s\n' "$sum"
}

verify_sha256() {  # verify_sha256 <path> <expected> <label>
  local path=$1 expected=$2 label=$3 actual
  [ -n "$expected" ] || return 0   # nothing pinned -> caller handles guidance
  actual=$(calc_sha256 "$path")
  if [ "$actual" != "$expected" ]; then
    echo "error: checksum mismatch for $label" >&2
    echo "       path:     $path" >&2
    echo "       expected: $expected" >&2
    echo "       actual:   $actual" >&2
    return 1
  fi
}

fetch_verified() {  # fetch_verified <label> <url> <path> <sha256>
  local label=$1 url=$2 path=$3 expected=$4 tmp
  if [ -f "$path" ] && verify_sha256 "$path" "$expected" "$label"; then
    echo "[uefi] $label: using existing $path"; return 0
  fi
  echo "[uefi] $label: downloading $url"
  tmp=$(mktemp "$WORK/.${label}.XXXXXX")
  curl --fail --location --show-error --retry 3 --connect-timeout 20 --output "$tmp" "$url"
  if ! verify_sha256 "$tmp" "$expected" "$label"; then rm -f "$tmp"; exit 1; fi
  mv "$tmp" "$path"
}

# --- locate the vmm binary for the host architecture --------------------
case "$HOST_MACHINE" in
  x86_64|amd64)
    if [ "$HOST_OS" = Darwin ]; then
      echo "error: Intel macOS is unsupported; use Linux/KVM or Apple-silicon macOS/HVF" >&2
      exit 1
    fi
    ARCH=x86_64; TRIPLE=x86_64-unknown-linux-gnu
    ;;
  aarch64|arm64)
    ARCH=aarch64
    if [ "$HOST_OS" = Darwin ]; then
      TRIPLE=aarch64-apple-darwin
    else
      TRIPLE=aarch64-unknown-linux-gnu
    fi
    ;;
  *) echo "error: unsupported host architecture $HOST_MACHINE" >&2; exit 1 ;;
esac

find_vmm() {
  if [ -n "${VMON_BIN:-}" ] && [ -x "${VMON_BIN:-}" ]; then echo "$VMON_BIN"; return 0; fi
  local proj base td
  proj=$(cd "$HERE/.." && pwd)
  base=$(printf '%s' "$proj" | sed -E 's#^(/Users/[^/]+|/home/[^/]+).*#\1#')
  # Resolve cargo's target dir (honors .cargo/config build.target-dir and
  # $CARGO_TARGET_DIR), mirroring python/vmon/vmm.py::_cargo_target_dir.
  td="${CARGO_TARGET_DIR:-}"
  [ -n "$td" ] || td=$(cd "$proj" && cargo metadata --no-deps --format-version 1 2>/dev/null \
    | python3 -c 'import json,sys;print(json.load(sys.stdin).get("target_directory",""))' 2>/dev/null || true)
  [ -n "$td" ] || td="$proj/target"
  for c in \
    "$td/$TRIPLE/release/vmm" "$td/$TRIPLE/debug/vmm" \
    "$td/release/vmm" "$td/debug/vmm" \
    "$proj/target/$TRIPLE/release/vmm" "$proj/target/release/vmm" \
    "$proj/target/$TRIPLE/debug/vmm" "$proj/target/debug/vmm" \
    "$base/.cache/cargo-target/$TRIPLE/release/vmm" \
    "$HOME/.cache/cargo-target/$TRIPLE/release/vmm" \
    "$(command -v vmm 2>/dev/null || true)"; do
    [ -n "$c" ] && [ -x "$c" ] && { echo "$c"; return 0; }
  done
  return 1
}
BIN=$(find_vmm) || {
  echo "error: vmm binary not found. Build it for $TRIPLE (on macOS: just build) or set VMON_BIN=/path/to/vmm" >&2
  exit 1
}
echo "[uefi] vmm: $BIN  (arch: $ARCH)"

# --- per-architecture image + firmware selection ----------------------------
# Ubuntu 24.04 "Noble" cloud images. These are qcow2 (despite the .img name)
# and are converted to raw below. The sha256 values are pinned against the
# immutable dated release snapshot release-20260615/, taken verbatim from
# https://cloud-images.ubuntu.com/releases/noble/release-20260615/SHA256SUMS
# (override IMG_URL/IMG_SHA256 to track a newer snapshot; re-pin from its
# matching SHA256SUMS, never by hand).
IMG_BASE=${IMG_BASE:-https://cloud-images.ubuntu.com/releases/noble/release-20260615}
if [ "$ARCH" = x86_64 ]; then
  IMG_NAME=ubuntu-24.04-server-cloudimg-amd64.img
  IMG_SHA256=${IMG_SHA256:-5fa5b05e5ec239858c4531485d6023b0896448c2df7c63b34f8dae6ea6051a44}
  SERIAL=ttyS0
  TRANSPORT_ARGS=(--transport pci)   # PCI virtio is x86_64-only in vmon.
  FW_ENV=${VMON_X86_UEFI:-}
  # Common distro locations for an x86_64 OVMF/EDK2 firmware image. A CODE-only
  # image is enough to reach the cloud image's GRUB via the removable-media
  # fallback path; vmon maps it read-only as ROM.
  FW_CANDIDATES=(
    /usr/share/OVMF/OVMF_CODE_4M.fd
    /usr/share/OVMF/OVMF_CODE.fd
    /usr/share/OVMF/OVMF.fd
    /usr/share/edk2/ovmf/OVMF_CODE.fd
    /usr/share/edk2/x64/OVMF_CODE.fd
    /usr/share/qemu/ovmf-x86_64-code.fd
  )
  # EDK2 nightly (unofficial, UNPINNED): used only if no firmware is installed.
  FW_URL=https://retrage.github.io/edk2-nightly/bin/RELEASEX64_OVMF.fd
  FW_LOCAL="$WORK/RELEASEX64_OVMF.fd"
else
  IMG_NAME=ubuntu-24.04-server-cloudimg-arm64.img
  IMG_SHA256=${IMG_SHA256:-cafa1a965b591b7c4184b484ffd8e625981a79d48f9b4ae8a4adf7b4c5ade927}
  SERIAL=ttyAMA0
  TRANSPORT_ARGS=()                   # aarch64 uses the default MMIO transport.
  FW_ENV=${VMON_AARCH64_UEFI:-}
  # Prefer the unpadded ~2 MiB QEMU_EFI.fd build. vmon loads aarch64 firmware
  # at the start of guest DRAM and rejects images that overlap the FDT, so the
  # 64 MiB padded *_CODE.fd flash images may not load -- use QEMU_EFI.fd.
  FW_CANDIDATES=(
    /usr/share/qemu-efi-aarch64/QEMU_EFI.fd
    /usr/share/edk2/aarch64/QEMU_EFI.fd
    /usr/share/edk2/arm/QEMU_EFI.fd
    /usr/share/AAVMF/QEMU_EFI.fd
    /opt/homebrew/share/qemu/QEMU_EFI.fd
    /usr/local/share/qemu/QEMU_EFI.fd
  )
  FW_URL=https://retrage.github.io/edk2-nightly/bin/RELEASEAARCH64_QEMU_EFI.fd
  FW_LOCAL="$WORK/RELEASEAARCH64_QEMU_EFI.fd"
fi

# --- fetch + convert the cloud image (skip if present) ----------------------
QCOW="$WORK/$IMG_NAME"
RAW="$WORK/ubuntu-uefi-$ARCH.raw"
fetch_verified "$IMG_NAME" "$IMG_BASE/$IMG_NAME" "$QCOW" "$IMG_SHA256"

if [ ! -f "$RAW" ] || [ "$QCOW" -nt "$RAW" ]; then
  echo "[uefi] converting $IMG_NAME (qcow2) -> raw"
  qemu-img convert -O raw "$QCOW" "$RAW.tmp"
  mv "$RAW.tmp" "$RAW"
else
  echo "[uefi] raw image: using existing $RAW"
fi
"${SUDO[@]}" chmod a+rw "$RAW" 2>/dev/null || chmod a+rw "$RAW" 2>/dev/null || true

# --- resolve UEFI firmware --------------------------------------------------
# Resolution order: explicit env var -> installed distro firmware -> download
# the EDK2 nightly. The nightly has no stable upstream sha256 to pin, so it is
# NOT verified against a baked-in hash: its sha256 is printed below for you to
# record and pin via IMG-style verification or your own out-of-band check.
# Never trust an unpinned firmware blob on an untrusted network.
FW_SHA256=""   # intentionally empty: no stable upstream hash for nightly EDK2.
resolve_firmware() {
  if [ -n "$FW_ENV" ]; then
    [ -f "$FW_ENV" ] || { echo "error: firmware $FW_ENV (from env) not found" >&2; exit 1; }
    FIRMWARE=$FW_ENV; echo "[uefi] firmware: $FIRMWARE (from env)"; return 0
  fi
  local c
  for c in "${FW_CANDIDATES[@]}"; do
    if [ -f "$c" ]; then FIRMWARE=$c; echo "[uefi] firmware: $FIRMWARE (distro)"; return 0; fi
  done
  echo "[uefi] no installed firmware found; fetching EDK2 nightly (UNPINNED)" >&2
  echo "       to use a packaged firmware instead, install one of:" >&2
  if [ "$HOST_OS" = Darwin ]; then
    echo "         macOS: set VMON_AARCH64_UEFI to QEMU_EFI.fd, or let this script fetch EDK2" >&2
  else
    echo "         x86_64:  apt-get install ovmf   | dnf install edk2-ovmf" >&2
    echo "         aarch64: apt-get install qemu-efi-aarch64 | dnf install edk2-aarch64" >&2
  fi
  echo "       or set VMON_X86_UEFI / VMON_AARCH64_UEFI to a firmware path." >&2
  fetch_verified "firmware" "$FW_URL" "$FW_LOCAL" "$FW_SHA256"
  FIRMWARE=$FW_LOCAL
  echo "[uefi] firmware: $FIRMWARE (EDK2 nightly, UNPINNED)"
  echo "[uefi] pin this hash after verifying it out-of-band:"
  echo "       sha256: $(calc_sha256 "$FIRMWARE")  $FW_LOCAL"
}
resolve_firmware

# --- optional: set credentials + serial autologin via libguestfs -----------
# Stock cloud images need a cloud-init datasource to configure a login. With
# VMON_UEFI_CUSTOMIZE=1 and virt-customize installed we instead bake in a
# root password and a serial autologin so the boot lands in a shell and emits
# a marker we can assert on. Otherwise the demo boots to the getty login:
# prompt (which already proves the UEFI boot chain works).
LOGIN_MARKER="login:"
if [ "${VMON_UEFI_CUSTOMIZE:-0}" = 1 ]; then
  if command -v virt-customize >/dev/null 2>&1; then
    echo "[uefi] customizing image: root password + serial autologin on $SERIAL"
    mkdir -p "$WORK/cust"
    cat > "$WORK/cust/override.conf" <<EOF
[Service]
ExecStart=
ExecStart=-/sbin/agetty --autologin root --keep-baud 115200,38400,9600 %I \$TERM
EOF
    printf '%s\n' 'echo VMON_UEFI_LOGIN_OK' > "$WORK/cust/zz-vmon.sh"
    "${SUDO[@]}" virt-customize -a "$RAW" \
      --root-password password:ubuntu \
      --mkdir "/etc/systemd/system/serial-getty@${SERIAL}.service.d" \
      --upload "$WORK/cust/override.conf:/etc/systemd/system/serial-getty@${SERIAL}.service.d/override.conf" \
      --upload "$WORK/cust/zz-vmon.sh:/etc/profile.d/zz-vmon.sh" \
      --run-command "systemctl enable serial-getty@${SERIAL}.service"
    LOGIN_MARKER="VMON_UEFI_LOGIN_OK"
  else
    echo "[uefi] VMON_UEFI_CUSTOMIZE=1 but virt-customize not found; install" >&2
    echo "       libguestfs-tools to use it. Booting the stock image instead." >&2
  fi
fi

# --- boot through UEFI firmware ---------------------------------------------
echo "[uefi] launching vmm: --boot-mode uefi --firmware <fw> --rootfs <img>${TRANSPORT_ARGS:+ ${TRANSPORT_ARGS[*]}}"
LOG="$WORK/uefi-ubuntu-$ARCH.log"
rm -f "$LOG"
set +e
"${SUDO[@]}" "$TIMEOUT_BIN" "$TIMEOUT" "$BIN" \
  --boot-mode uefi \
  --firmware "$FIRMWARE" \
  --rootfs "$RAW" \
  "${TRANSPORT_ARGS[@]}" \
  --mem "$MEM" --cpus "$CPUS" 2>&1 | tee "$LOG"
status=${PIPESTATUS[0]}
set -e

# timeout(1) exits 124 when it had to kill vmm; that is expected here
# because a cloud image boots to an interactive login and never powers off,
# so we judge success by what reached the serial console, not the exit code.
if [ "$status" -ne 0 ] && [ "$status" -ne 124 ]; then
  echo "[uefi] vmm exited with status $status before reaching login" >&2
  exit "$status"
fi

if grep -qF "$LOGIN_MARKER" "$LOG"; then
  echo "[uefi] PASS: reached serial login ('$LOGIN_MARKER') under UEFI firmware"
  if [ "$LOGIN_MARKER" = "login:" ]; then
    echo "[uefi] note: log in with credentials from a cloud-init seed, or re-run"
    echo "       with VMON_UEFI_CUSTOMIZE=1 (needs libguestfs-tools) for root/ubuntu."
  else
    echo "[uefi] logged in as root (password 'ubuntu') via serial autologin"
  fi
  echo "[uefi] serial log: $LOG"
else
  echo "[uefi] FAIL: serial console never reached login ('$LOGIN_MARKER')" >&2
  echo "       inspect $LOG; common causes: wrong firmware for the host arch," >&2
  echo "       a padded (64 MiB) aarch64 flash image, or a serial-console mismatch." >&2
  exit 1
fi
