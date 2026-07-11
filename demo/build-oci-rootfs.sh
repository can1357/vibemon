#!/bin/bash
# build-oci-rootfs.sh -- turn an OCI/Docker image into an ext4 disk image that
# vmon can serve over virtio-blk. This is the host-side rootfs recipe for the
# Phase-4 warm-boot path.
#
# Pipeline (all host-side, no /dev/kvm needed):
#     docker://<image>  --skopeo copy-->     local OCI image layout
#                       --umoci unpack-->     flattened rootfs directory
#                       --mkfs.ext4 -d -->    ext4 disk image
#
# Usage:
#   build-oci-rootfs.sh [image-ref] [out-img] [size]
#     image-ref  any reference skopeo understands (docker://, oci:, etc.)
#                default: docker://busybox
#     out-img    path of the ext4 image to write
#                default: /tmp/vmon-demo/oci-rootfs.img
#     size       size argument passed to mkfs.ext4 (e.g. 256M, 1G)
#                default: 256M
#
# Examples:
#   build-oci-rootfs.sh                                    # busybox -> default
#   build-oci-rootfs.sh docker://alpine:3.20 /tmp/alpine.img 512M
#
# The produced image is meant to be handed to the low-level VMM subcommand:
#   vmon vmm --kernel <Image> --rootfs <out-img> \
#            --cmdline "console=ttyS0 root=/dev/vda rw rdinit=/init"
#
# Warm-boot fast path (Phase 4): boot a microVM on this image to
# "userspace up, guest-agent waiting", `snapshot` that VM as the template, then
# per container start `--restore` the template and feed the OCI entrypoint to
# the in-guest agent over virtio-console. The /init stub synthesized below is a
# standalone fallback so the image is also directly bootable WITHOUT the agent.
#
# Requires: skopeo, umoci, mkfs.ext4/mke2fs (e2fsprogs); jq is optional (used
# only to auto-detect the image entrypoint). This runs on Linux and macOS hosts
# with the container tooling installed; on macOS those tools are available via
# Homebrew.
set -euo pipefail

# mkfs.ext4/mke2fs commonly live in sbin directories missing from a non-root
# user's PATH. Homebrew's e2fsprogs is keg-only on macOS, so include its sbin
# locations as well.
export PATH="$PATH:/usr/sbin:/sbin:/opt/homebrew/opt/e2fsprogs/sbin:/usr/local/opt/e2fsprogs/sbin:/opt/homebrew/sbin:/usr/local/sbin"

IMAGE_REF=${1:-docker://busybox}
OUT_IMG=${2:-/tmp/vmon-demo/oci-rootfs.img}
SIZE=${3:-256M}

# --- preconditions: required host tooling -----------------------------------
missing=0
need() {  # need <command> <install-hint>
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required tool '$1' not found on PATH" >&2
    echo "       hint: $2" >&2
    missing=1
  fi
}
need skopeo "apt-get install skopeo  |  dnf install skopeo  |  brew install skopeo"
need umoci  "apt-get install umoci  |  brew install umoci  |  or fetch a static binary from https://github.com/opencontainers/umoci/releases"
if command -v mkfs.ext4 >/dev/null 2>&1; then
  MKFS_EXT4=(mkfs.ext4 -F -q)
elif command -v mke2fs >/dev/null 2>&1; then
  MKFS_EXT4=(mke2fs -t ext4 -F -q)
else
  echo "error: required tool 'mkfs.ext4' (or 'mke2fs') not found on PATH" >&2
  echo "       hint: apt-get install e2fsprogs  |  dnf install e2fsprogs  |  brew install e2fsprogs" >&2
  missing=1
fi
if [ "$missing" -ne 0 ]; then
  echo "error: install the missing tool(s) listed above, then re-run" >&2
  exit 1
fi
command -v jq >/dev/null 2>&1 || \
  echo "[oci] note: jq not found -> entrypoint auto-detection disabled; the /init stub will exec /bin/sh" >&2

# --- scratch space (cleaned up on exit) -------------------------------------
TMP=$(mktemp -d "${TMPDIR:-/tmp}/vmon-oci.XXXXXX")
trap 'rm -rf "$TMP"' EXIT

# 1) pull the image into a local OCI layout
echo "[oci] skopeo copy: $IMAGE_REF -> oci:$TMP/oci:latest"
skopeo copy "$IMAGE_REF" "oci:$TMP/oci:latest"

# 2) flatten all layers into a rootfs at $TMP/bundle/rootfs (+ config.json).
#    NOTE: if the image carries device nodes or ownership umoci cannot reproduce
#    unprivileged, run as root on Linux or add `--rootless`.
echo "[oci] umoci unpack -> $TMP/bundle/rootfs"
umoci unpack --image "$TMP/oci:latest" "$TMP/bundle"
ROOTFS="$TMP/bundle/rootfs"
[ -d "$ROOTFS" ] || { echo "error: umoci did not produce $ROOTFS" >&2; exit 1; }

# 3) best-effort: make the image self-bootable by ensuring an init exists.
#    If the rootfs already ships /sbin/init or /init we leave it alone.
#    Otherwise synthesize a tiny /init that mounts the pseudo-filesystems and
#    execs the image's configured entrypoint (OCI bundle config.json
#    `.process.args`, read via jq when present; falls back to /bin/sh). The real
#    warm-boot path execs the entrypoint from the guest agent instead -- this
#    stub only exists so a plain `--rootfs <img> --cmdline "... rdinit=/init"`
#    also works for manual boots.
if [ ! -e "$ROOTFS/sbin/init" ] && [ ! -e "$ROOTFS/init" ]; then
  ENTRY="/bin/sh"
  CFG="$TMP/bundle/config.json"
  if command -v jq >/dev/null 2>&1 && [ -f "$CFG" ]; then
    args=$(jq -r '.process.args // [] | join(" ")' "$CFG" 2>/dev/null || true)
    if [ -n "$args" ] && [ "$args" != "null" ]; then ENTRY="$args"; fi
  fi
  echo "[oci] no init in image; writing /init stub (exec: $ENTRY)"
  cat > "$ROOTFS/init" <<EOF
#!/bin/sh
# Auto-generated by vmon build-oci-rootfs.sh (best-effort init stub).
# Bring up required pseudo-filesystems, then exec the OCI entrypoint.
PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH
fail() {
  echo "vmon-oci-init: FAIL: \$*" >&2
  sync
  poweroff -f 2>/dev/null || exit 1
  exit 1
}
mount -t proc     proc /proc || fail "mount /proc"
mount -t sysfs    sys  /sys  || fail "mount /sys"
mount -t devtmpfs dev  /dev  2>/dev/null || true
exec $ENTRY
fail "exec failed"
EOF
  chmod 0755 "$ROOTFS/init"
fi

# 4) build the ext4 image directly from the rootfs directory
echo "[oci] ${MKFS_EXT4[0]} -d $ROOTFS -> $OUT_IMG ($SIZE)"
mkdir -p "$(dirname "$OUT_IMG")"
rm -f "$OUT_IMG"
"${MKFS_EXT4[@]}" -d "$ROOTFS" "$OUT_IMG" "$SIZE"

# 5) report the result
echo "[oci] done:"
ls -lh "$OUT_IMG"
echo "[oci] serve it with: vmon vmm --rootfs $OUT_IMG --cmdline \"console=ttyS0 root=/dev/vda rw rdinit=/init\""
