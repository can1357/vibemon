#!/usr/bin/env bash
set -euo pipefail

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

echo "vmon cluster e2e: requires a built vmon binary, guest assets, and KVM/HVF; Rust tests will SKIP otherwise."

if [[ "$(uname -s)" == "Darwin" ]]; then
  export CARGO_TARGET_AARCH64_APPLE_DARWIN_RUNNER="${CARGO_TARGET_AARCH64_APPLE_DARWIN_RUNNER:-$repo_root/demo/hvf-test-runner.sh}"
  export VMON_ENTITLEMENTS="${VMON_ENTITLEMENTS:-$repo_root/hvf.entitlements}"
fi

cd "$repo_root"
exec env VMON_CLUSTER_E2E=1 VMON_E2E=1 cargo test --test cluster_e2e -- --test-threads=1
