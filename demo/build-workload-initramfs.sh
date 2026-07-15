#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ARCH=${VMON_WORKLOAD_ARCH:-$(uname -m)}
case "$ARCH" in
  arm64 | aarch64)
    VMON_ARCH=aarch64
    DOCKER_ARCH=arm64
    ;;
  x86_64 | amd64)
    VMON_ARCH=x86_64
    DOCKER_ARCH=amd64
    ;;
  *)
    echo "unsupported workload guest architecture: $ARCH" >&2
    exit 1
    ;;
esac

OUTPUT=${1:-"$ROOT/target/test-assets/workload-initramfs-$VMON_ARCH.cpio.gz"}
AGENT="$ROOT/target/test-assets/vmon-agent-$VMON_ARCH"
IMAGE="vmon-workload-initramfs:$VMON_ARCH"

command -v docker >/dev/null 2>&1 || {
  echo "missing required tool: docker" >&2
  exit 1
}
[ -x "$AGENT" ] || {
  echo "missing guest agent $AGENT; run 'just agent-musl' first" >&2
  exit 1
}


docker build \
  --platform "linux/$DOCKER_ARCH" \
  --build-arg "VMON_ARCH=$VMON_ARCH" \
  --file "$ROOT/demo/guest-workloads/Dockerfile" \
  --tag "$IMAGE" \
  "$ROOT"

mkdir -p "$(dirname "$OUTPUT")"
docker run --rm \
  --platform "linux/$DOCKER_ARCH" \
  --entrypoint /bin/sh \
  "$IMAGE" \
  -c 'mknod -m 600 /dev/console c 5 1 && cd / && { find . -xdev -print; find ./dev -print; } | sort -u | cpio -o -H newc -R 0:0 2>/dev/null | gzip -n -1' \
  >"$OUTPUT.tmp"
mv "$OUTPUT.tmp" "$OUTPUT"
printf '%s\n' "$OUTPUT"
