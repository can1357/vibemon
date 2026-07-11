#!/usr/bin/env bash
set -euo pipefail

repo_root="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

SOAK_ITERATIONS="${SOAK_ITERATIONS:-10}"
SOAK_NETEM_DEV="${SOAK_NETEM_DEV:-lo}"
SOAK_NETEM_DELAY="${SOAK_NETEM_DELAY:-20ms 10ms}"
SOAK_NETEM_LOSS="${SOAK_NETEM_LOSS:-1%}"

skip() {
  echo "mesh-soak: SKIP: $*"
  exit 0
}

fail() {
  echo "mesh-soak: ERROR: $*" >&2
  exit 1
}

if ! command -v tc >/dev/null 2>&1; then
  skip "tc command not found; Linux iproute2/netem is required to shape the host interface."
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

if [[ ! "$SOAK_ITERATIONS" =~ ^[0-9]+$ ]]; then
  fail "SOAK_ITERATIONS must be a non-negative integer; got '$SOAK_ITERATIONS'."
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

  # Netem remains host-level: we shape the selected host interface (loopback by
  # default), independent of the Rust test driver, so local cluster peer traffic
  # still runs through the injected delay/loss.
  echo "mesh-soak: applying netem on $SOAK_NETEM_DEV: delay $SOAK_NETEM_DELAY loss $SOAK_NETEM_LOSS"
  "${tc_cmd[@]}" qdisc add dev "$SOAK_NETEM_DEV" root netem \
    delay "${delay_args[@]}" loss "$SOAK_NETEM_LOSS"
}

run_iteration() (
  cd "$repo_root"
  VMON_CLUSTER_E2E=1 VMON_E2E=1 cargo test --test cluster_e2e -- --test-threads=1
)

trap cleanup_qdisc EXIT

if (( SOAK_ITERATIONS == 0 )); then
  echo "mesh-soak: prerequisites available; SOAK_ITERATIONS=0 so no iterations requested."
  exit 0
fi

failures=0

echo "mesh-soak: starting iterations=${SOAK_ITERATIONS}; netem_dev=$SOAK_NETEM_DEV"

for (( iteration = 1; iteration <= SOAK_ITERATIONS; iteration++ )); do
  echo "::group::mesh-soak iteration $iteration"
  echo "mesh-soak: iteration $iteration begin"

  apply_netem
  if run_iteration; then
    echo "mesh-soak: iteration $iteration passed"
  else
    rc=$?
    failures=$((failures + 1))
    echo "::error::mesh-soak iteration $iteration failed with exit $rc" >&2
  fi
  cleanup_qdisc

  echo "mesh-soak: iteration $iteration end; failures=$failures"
  echo "::endgroup::"
done

echo "mesh-soak: completed iterations=$SOAK_ITERATIONS failures=$failures"
if (( failures > 0 )); then
  exit 1
fi
