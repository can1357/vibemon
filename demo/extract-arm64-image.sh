#!/bin/bash
# Extract an uncompressed arm64 `Image` (what vmon's loader expects) from a
# distro EFI-zboot `vmlinuz`. Modern Ubuntu/Alpine arm64 kernels ship as a PE
# EFI app whose payload is a gzip/zstd-compressed Image; this unwraps it.
#
# Usage: extract-arm64-image.sh [vmlinuz] [out-Image]
#   defaults: vmlinuz = /boot/vmlinuz-$(uname -r), out = /tmp/vmon-demo/Image
set -euo pipefail
IN=${1:-/boot/vmlinuz-$(uname -r)}
OUT=${2:-/tmp/vmon-demo/Image}
mkdir -p "$(dirname "$OUT")"

# /boot kernels are often root-only; copy to a readable temp if needed.
if [ ! -r "$IN" ]; then
  echo "[extract] $IN not readable, copying with sudo" >&2
  sudo cp "$IN" "$(dirname "$OUT")/vmlinuz.src"
  sudo chmod a+r "$(dirname "$OUT")/vmlinuz.src"
  IN="$(dirname "$OUT")/vmlinuz.src"
fi

python3 - "$IN" "$OUT" <<'PY'
import sys, struct, gzip, subprocess, os
data = open(sys.argv[1], "rb").read()
# Already a raw arm64 Image?
if data[56:60] == b"ARM\x64":
    open(sys.argv[2], "wb").write(data)
    print(f"[extract] {sys.argv[1]} is already a raw Image; copied")
    sys.exit(0)
z = data.find(b"zimg")
if z < 0:
    sys.exit("not an EFI-zboot kernel (no zimg magic) and not a raw Image")
base = z - 4
poff, psize = struct.unpack_from("<II", data, base + 8)
comp = data[base + 0x18: base + 0x18 + 16].split(b"\0")[0].decode()
payload = data[base + poff: base + poff + psize]
print(f"[extract] zboot payload off={hex(base+poff)} size={hex(psize)} comp={comp}")
if comp == "gzip":
    open(sys.argv[2], "wb").write(gzip.decompress(payload))
elif comp == "zstd":
    open("/tmp/.zb.payload", "wb").write(payload)
    subprocess.run(["unzstd", "-f", "-q", "-o", sys.argv[2], "/tmp/.zb.payload"], check=True)
    os.remove("/tmp/.zb.payload")
else:
    sys.exit(f"unsupported compression: {comp}")
img = open(sys.argv[2], "rb").read()
assert img[56:60] == b"ARM\x64", f"bad arm64 Image magic: {img[56:60].hex()}"
print(f"[extract] wrote {sys.argv[2]} ({len(img)} bytes), valid arm64 Image")
PY
