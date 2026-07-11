#!/bin/sh
# Cargo test runner for the macOS/HVF e2e suite.
#
# Hypervisor.framework refuses to launch a binary that is not codesigned with
# `com.apple.security.hypervisor`, but `cargo test` re-copies the freshly built
# (unsigned) `vmon` over any signature we apply beforehand. This runner is
# invoked by cargo *after* that copy, once per test binary, so it ad-hoc signs
# the exact `vmon` the tests spawn and then execs the test.
#
# Wire it up only for e2e runs, e.g.:
#   CARGO_TARGET_AARCH64_APPLE_DARWIN_RUNNER=demo/hvf-test-runner.sh \
#   VMON_E2E=1 cargo test --tests -- --test-threads=1
set -eu

test_bin=$1
# binary it spawns (CARGO_BIN_EXE_vmon) is one directory up.
vmm="$(dirname "$test_bin")/../vmon"
entitlements="${VMON_ENTITLEMENTS:-hvf.entitlements}"

if [ -x "$vmm" ]; then
    codesign --sign - --entitlements "$entitlements" --force "$vmm" >/dev/null 2>&1 \
        || echo "hvf-test-runner: warning: failed to codesign $vmm" >&2
fi

exec "$@"
