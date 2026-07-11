#!/bin/sh
# Run the real Linux/KVM verification suites on xeon.internal.
set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
host=${VMON_TEST_HOST:-xeon.internal}

if [ -z "${VMON_TEST_REPO:-}" ]; then
    remote_user=$(ssh -G "$host" 2>/dev/null | awk '/^user / {print $2}' || true)
    remote_user=${remote_user:-$USER}
    if [ "$remote_user" = "root" ]; then
        remote_home="/root"
    else
        remote_home="/home/$remote_user"
    fi
else
    remote_home=""
fi

remote_repo=${VMON_TEST_REPO:-$remote_home/vibevmm-e2e}
mode=${1:-all}
skip_prepare=${VMON_TEST_SKIP_PREPARE:-0}
tap=${VMON_TEST_TAP:-vmon0}
host_ip=${VMON_TEST_HOST_IP:-192.168.249.1}
port=${VMON_TEST_PORT:-18085}
alpine_image=${VMON_TEST_ALPINE_IMAGE:-quay.io/libpod/alpine:latest}
python_image=${VMON_TEST_PYTHON_IMAGE:-public.ecr.aws/docker/library/python:3.14-slim}
node_image=${VMON_TEST_NODE_IMAGE:-public.ecr.aws/docker/library/node:24-alpine}

case "$mode" in
    prepare | rust | python | sdk | all) ;;
    *)
        echo "usage: $0 [prepare|rust|python|sdk|all]" >&2
        exit 2
        ;;
esac

if [ "${VMON_TEST_NO_SYNC:-0}" != "1" ]; then
    "$script_dir/xeon-sync.sh"
fi

exec ssh "$host" bash -s -- \
    "$remote_repo" "$mode" "$skip_prepare" "$tap" "$host_ip" "$port" \
    "$alpine_image" "$python_image" "$node_image" <<'REMOTE'
set -euo pipefail

repo=$1
mode=$2
skip_prepare=$3
tap=$4
host_ip=$5
port=$6
alpine_image=$7
python_image=$8
node_image=$9

cd "$repo"

prepare_host() {
    echo '==> preparing guest agent, assets, and vmon'
    just agent-musl
    # fetch-test-assets caches the initramfs independently of the embedded
    # agent. Remove only that generated archive so it receives the new agent.
    rm -f target/test-assets/busybox-initramfs-x86_64.cpio.gz
    just fetch-assets
    cargo build --locked
}

rust_e2e() {
    echo '==> Rust Linux/KVM integration suite'
    VMON_TAP="$tap" \
    VMON_HOST_IP="$host_ip" \
    VMON_KERNEL="$repo/target/test-assets/bzImage-x86_64" \
    VMON_E2E=1 \
        cargo test --tests -- --test-threads=1
}

python_e2e() {
    echo '==> Python SDK real-VM suite'
    uv sync
    if sudo -n true >/dev/null 2>&1; then
        sudo -n env \
            HOME=/root \
            PATH="$PATH" \
            VMON_BIN="$repo/target/debug/vmon" \
            VMON_E2E=1 \
            VMON_KERNEL="$repo/target/test-assets/bzImage-x86_64" \
            VMON_E2E_IMAGE="$alpine_image" \
            VMON_E2E_PYTHON_IMAGE="$python_image" \
            "$repo/.venv/bin/python" sdk/py/e2e.py
    else
        echo 'xeon-test: sudo -n unavailable; root-only network cases will skip' >&2
        env \
            VMON_BIN="$repo/target/debug/vmon" \
            VMON_E2E=1 \
            VMON_KERNEL="$repo/target/test-assets/bzImage-x86_64" \
            VMON_E2E_IMAGE="$alpine_image" \
            VMON_E2E_PYTHON_IMAGE="$python_image" \
            "$repo/.venv/bin/python" sdk/py/e2e.py
    fi
}

sdk_e2e() {
    echo '==> Go and TypeScript SDK suites with real remote functions'
    if ss -ltn "sport = :$port" | grep -q LISTEN; then
        echo "xeon-test: TCP port $port is already in use; set VMON_TEST_PORT" >&2
        return 1
    fi

    server_home=$(mktemp -d /tmp/vmon-sdk-e2e.XXXXXX)
    chmod 700 "$server_home"
    server_log=$server_home/server.log
    server_pid=

    cleanup_server() {
        rc=$?
        trap - EXIT INT TERM
        if [ -n "$server_pid" ] && kill -0 "$server_pid" 2>/dev/null; then
            kill -TERM "$server_pid" 2>/dev/null || true
            for _ in $(seq 1 100); do
                kill -0 "$server_pid" 2>/dev/null || break
                sleep 0.1
            done
            kill -KILL "$server_pid" 2>/dev/null || true
            wait "$server_pid" 2>/dev/null || true
        fi
        if [ "$rc" -ne 0 ] && [ -f "$server_log" ]; then
            echo '--- vmon serve log ---' >&2
            tail -200 "$server_log" >&2
        fi
        rm -rf "$server_home"
        exit "$rc"
    }
    trap cleanup_server EXIT INT TERM

    VMON_KERNEL="$repo/target/test-assets/bzImage-x86_64" \
        target/debug/vmon serve \
        --home "$server_home" \
        --host 127.0.0.1 \
        --port "$port" \
        --token xeon-sdk-e2e \
        >"$server_log" 2>&1 &
    server_pid=$!

    ready=0
    for _ in $(seq 1 200); do
        if curl -fsS "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then
            ready=1
            break
        fi
        kill -0 "$server_pid" 2>/dev/null || break
        sleep 0.1
    done
    if [ "$ready" -ne 1 ]; then
        echo 'xeon-test: vmon serve did not become healthy' >&2
        return 1
    fi

    (
        cd sdk/go
        VMON_GO_REMOTE_SMOKE=1 \
        VMON_SERVER_URL="http://127.0.0.1:$port" \
        VMON_API_TOKEN=xeon-sdk-e2e \
        VMON_GO_REMOTE_IMAGE="$node_image" \
            go test -v ./...
    )
    (
        cd sdk/ts
        bun install
        VMON_TS_SMOKE=1 \
        VMON_TS_REMOTE_SMOKE=1 \
        VMON_SERVER_URL="http://127.0.0.1:$port" \
        VMON_API_TOKEN=xeon-sdk-e2e \
        VMON_TS_REMOTE_IMAGE="$node_image" \
            bun test
    )

    cleanup_server
}

if [ "$mode" = prepare ]; then
    prepare_host
    exit 0
fi
if [ "$skip_prepare" != 1 ]; then
    prepare_host
fi

case "$mode" in
    rust)
        rust_e2e
        ;;
    python)
        python_e2e
        ;;
    sdk)
        sdk_e2e
        ;;
    all)
        rust_e2e
        python_e2e
        sdk_e2e
        ;;
esac
REMOTE
