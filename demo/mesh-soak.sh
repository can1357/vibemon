#!/usr/bin/env bash
set -euo pipefail

repo_root="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

SOAK_MINUTES="${SOAK_MINUTES:-30}"
SOAK_NETEM_DEV="${SOAK_NETEM_DEV:-lo}"
SOAK_NETEM_DELAY="${SOAK_NETEM_DELAY:-20ms 10ms}"
SOAK_NETEM_LOSS="${SOAK_NETEM_LOSS:-1%}"
SOAK_PYTEST_TARGET="${SOAK_PYTEST_TARGET:-tests/test_cluster_e2e.py::test_three_node_writable_volume_quorum_ha}"

skip() {
  echo "mesh-soak: SKIP: $*"
  exit 0
}

fail() {
  echo "mesh-soak: ERROR: $*" >&2
  exit 1
}

if ! command -v tc >/dev/null 2>&1; then
  skip "tc command not found; Linux iproute2/netem is required to shape loopback."
fi

if [[ "$(uname -s)" != "Linux" ]]; then
  skip "KVM mesh soak requires Linux /dev/kvm; found $(uname -s)."
fi

if [[ ! -e /dev/kvm ]]; then
  skip "/dev/kvm is unavailable; real cluster e2e requires KVM."
fi

if [[ ! -r /dev/kvm || ! -w /dev/kvm ]]; then
  skip "/dev/kvm is not readable and writable by this runner."
fi

if [[ ! "$SOAK_MINUTES" =~ ^[0-9]+$ ]]; then
  fail "SOAK_MINUTES must be a non-negative integer number of minutes; got '$SOAK_MINUTES'."
fi

tc_cmd=(tc)
if (( EUID != 0 )); then
  if ! command -v sudo >/dev/null 2>&1; then
    fail "tc netem requires root; run as root or install passwordless sudo."
  fi
  if ! sudo -n true 2>/dev/null; then
    fail "tc netem requires root; passwordless sudo is not available."
  fi
  tc_cmd=(sudo -n tc)
fi

cleanup_qdisc() {
  "${tc_cmd[@]}" qdisc del dev "$SOAK_NETEM_DEV" root >/dev/null 2>&1 || true
}

apply_netem() {
  local -a delay_args

  cleanup_qdisc
  read -r -a delay_args <<< "$SOAK_NETEM_DELAY"
  if (( ${#delay_args[@]} == 0 )); then
    fail "SOAK_NETEM_DELAY must provide at least one tc delay argument."
  fi

  echo "mesh-soak: applying netem on $SOAK_NETEM_DEV: delay $SOAK_NETEM_DELAY loss $SOAK_NETEM_LOSS"
  "${tc_cmd[@]}" qdisc add dev "$SOAK_NETEM_DEV" root netem \
    delay "${delay_args[@]}" loss "$SOAK_NETEM_LOSS"
}

run_iteration() (
  cd "$repo_root/python"
  VMON_CLUSTER_E2E=1 VMON_E2E=1 uv run --extra server pytest "$SOAK_PYTEST_TARGET" -q -s
)

trap cleanup_qdisc EXIT

duration_seconds=$((SOAK_MINUTES * 60))
if (( duration_seconds == 0 )); then
  echo "mesh-soak: prerequisites available; SOAK_MINUTES=0 so no iterations requested."
  exit 0
fi

deadline=$((SECONDS + duration_seconds))
iterations=0
failures=0

echo "mesh-soak: starting for ${SOAK_MINUTES}m; target=$SOAK_PYTEST_TARGET; netem_dev=$SOAK_NETEM_DEV"

while (( SECONDS < deadline )); do
  iterations=$((iterations + 1))
  echo "::group::mesh-soak iteration $iterations"
  echo "mesh-soak: iteration $iterations begin"

  apply_netem
  if run_iteration; then
    echo "mesh-soak: iteration $iterations passed"
  else
    rc=$?
    failures=$((failures + 1))
    echo "::error::mesh-soak iteration $iterations failed with exit $rc" >&2
  fi
  cleanup_qdisc

  echo "mesh-soak: iteration $iterations end; failures=$failures"
  echo "::endgroup::"
done

echo "mesh-soak: completed iterations=$iterations failures=$failures elapsed=${SECONDS}s"
if (( failures > 0 )); then
  exit 1
fi
