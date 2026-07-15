#!/bin/sh
set -eu

export PATH=/usr/bin:/bin:/usr/sbin:/sbin

daemon_pid=
cleanup() {
  if [ -n "$daemon_pid" ]; then
    kill "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT HUP INT TERM

skip() {
  printf '%s\n' "VMON_WORKLOAD_SKIP: docker: $*"
  exit 77
}

fail() {
  printf '%s\n' "VMON_WORKLOAD_FAIL: docker: $*" >&2
  exit 1
}

for tool in docker dockerd tar timeout; do
  command -v "$tool" >/dev/null 2>&1 || skip "Docker requires $tool"
done
[ "$(id -u)" -eq 0 ] || skip "Docker daemon requires guest root"
[ -e /sys/fs/cgroup/cgroup.controllers ] || skip "Docker requires cgroup v2"
[ -x /usr/local/bin/runc-no-pivot ] || skip "ramdisk-compatible runc wrapper not found"

base="/tmp/vmon-docker-$$"
export DOCKER_HOST="unix://$base/docker.sock"
mkdir -p "$base/container-root/bin" "$base/container-root/lib"

dockerd \
  --host "$DOCKER_HOST" \
  --data-root "$base/data" \
  --exec-root "$base/exec" \
  --pidfile "$base/docker.pid" \
  --storage-driver=vfs \
  --add-runtime no-pivot=/usr/local/bin/runc-no-pivot \
  --default-runtime=no-pivot \
  --iptables=false \
  --bridge=none \
  --userland-proxy=false \
  >"$base/dockerd.log" 2>&1 &
daemon_pid=$!

ready=0
i=0
while [ "$i" -lt 30 ]; do
  if timeout 2 docker info >/dev/null 2>&1; then
    ready=1
    break
  fi
  i=$((i + 1))
  sleep 1
done
[ "$ready" -eq 1 ] || fail "dockerd failed to start: $(cat "$base/dockerd.log")"

cp /bin/busybox "$base/container-root/bin/busybox"
cp /lib/ld-musl-*.so.1 "$base/container-root/lib/"
(cd "$base/container-root" && tar -cf "$base/container.tar" .)
docker import "$base/container.tar" vmon-workload:latest >/dev/null \
  || fail "docker import failed"
output=$(timeout 30 docker run --rm vmon-workload:latest \
  /bin/busybox sh -c 'echo VMON_DOCKER_CONTAINER_OK' 2>&1) \
  || fail "container run failed: $output"
printf '%s\n' "$output" | grep -Fq 'VMON_DOCKER_CONTAINER_OK' \
  || fail "container output marker missing"

printf '%s\n' 'VMON_DOCKER_OK'
exit 0
