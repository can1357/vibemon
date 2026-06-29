#!/usr/bin/env bash
set -euo pipefail

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

echo "vmon cluster e2e: requires a working vmm binary, guest agent, and KVM/HVF; pytest will SKIP otherwise."

export VMON_CLUSTER_E2E=1
export VMON_E2E=1

cd "$repo_root/python"
exec uv run --extra server pytest tests/test_cluster_e2e.py -q -s
