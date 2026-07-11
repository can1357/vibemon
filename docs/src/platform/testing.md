# Testing

Use the focused `just` recipe that matches the behavior under change. The repository separates hermetic tests from guest-boot tests: a plain `cargo test` does not require a hypervisor, while end-to-end VM boots require both the opt-in environment and a usable backend.

## Test matrix

| Goal | Command | Hypervisor requirement | Expected skip behavior |
| --- | --- | --- | --- |
| Hermetic Rust unit/integration and CLI capability checks | `just test` | None | Runs without `/dev/kvm` or HVF. Guest-boot tests early-return because `VMON_E2E` is not set. |
| Host end-to-end suite | `just integration` | Linux/KVM or macOS/HVF | The recipe sets `VMON_E2E=1`; boot tests skip when the host backend is unavailable. Capability-specific rows skip outside their supported platform. |
| VMM smoke, including guest agent | `just smoke` | Linux/KVM or macOS/HVF | The recipe sets `VMON_E2E=1` and includes the musl guest agent. Linux TAP rows run only when `VMON_TAP=<iface>` is also supplied; the recipe does not set `VMON_JAIL`, so it does not establish jail coverage. |
| Linux jail smoke | `just smoke-jail` | Linux/KVM **and root** | Linux-only. It uses `sudo`, `VMON_E2E=1`, and `VMON_JAIL=1`; jail rows skip without a usable hypervisor, Linux isolation support, or root. macOS prints that jail is Linux-only. |
| Linux seccomp allowlist audit | `just seccomp-audit` | Linux/KVM | Linux runs the suite with `VMON_SECCOMP_ACTION=log`; macOS relays to the Lima recipe. |
| Long-running backend smoke | `just soak` | Linux/KVM or macOS/HVF | Requires the recipe's `VMON_E2E=1` and `VMON_SOAK=1`; backend-unavailable boot rows skip. |
| Gated real-VM cluster end-to-end | `just cluster-e2e` | Linux/KVM or macOS/HVF | Requires a real usable backend; it sets `VMON_CLUSTER_E2E=1` and `VMON_E2E=1`. The cluster suite is gated and skips otherwise. |
| Linux/KVM path from macOS | `just lima-integration` | Nested Linux/KVM inside Lima | Runs the Linux path in the configured Lima guest rather than on macOS/HVF. |
| Ordinary Linux/KVM smoke from macOS | `just lima-smoke` | Nested Linux/KVM inside Lima | Runs the ordinary `VMON_E2E=1` Linux/KVM suite in the configured Lima guest. It does not set `VMON_TAP` or `VMON_JAIL`; TAP rows require `VMON_TAP=<iface>`, and jail coverage requires root through `just smoke-jail` in the guest. |
| Long-running Linux/KVM smoke from macOS | `just lima-soak` | Nested Linux/KVM inside Lima | Runs the soak suite in the Linux guest with `VMON_E2E=1` and `VMON_SOAK=1`. |
| Linux seccomp audit from macOS | `just lima-seccomp-audit` | Nested Linux/KVM inside Lima | Runs the Linux log-mode audit in the guest. |

`just integration`, `just smoke`, `just soak`, and `just cluster-e2e` fetch the pinned architecture-appropriate test assets first. On macOS, their test runner ad-hoc signs the spawned binary immediately before each test with the Hypervisor entitlement. macOS test assets require `libslirp`, `pkg-config`, `e2fsprogs`, and `cpio`.

## What the platform-specific rows cover

The shared suite runs on Linux/KVM, Apple Silicon macOS/HVF, and Linux/KVM in Lima. The common capability helper enables guest boots only when `VMON_E2E=1` **and** a backend is usable: `/dev/kvm` exists on Linux, or `sysctl -n kern.hv_support` begins with `1` on macOS. This is why ordinary `just test` remains hermetic.

| Capability | Where it runs |
| --- | --- |
| Boot, virtio block and filesystem, JSON control (pause/snapshot/resume/quit), metrics, timeout, snapshot/restore/fork, and delta snapshots | Every supported backend |
| TAP networking and throughput | Linux/KVM only; requires a host TAP via `VMON_TAP=<iface>` and optionally `VMON_HOST_IP` |
| User-mode NAT, DHCP lease, and outbound TCP through slirp | macOS/HVF only |
| PCI virtio transport | x86_64 only |
| userfaultfd paging and seccomp audit | Linux only |
| Namespace jail | Linux only; requires `VMON_JAIL=1` and effective root, as provided by `just smoke-jail` |
| CLI capability matrix | No hypervisor; runs under plain `cargo test` and checks unsupported flag combinations |

The CLI matrix is specifically useful for flag validation without guest hardware: it covers PCI on aarch64, `--net user` outside macOS, `--net user` combined with `--tap`, and UEFI without firmware.

## Focused recipes

Use these commands from the repository root:

```sh
# No hypervisor required: narrow Rust target or all normal Rust tests.
just test --test cli_matrix
just test

# Guest boot and general integration on the current supported host.
just integration

# Linux TAP coverage needs an existing host TAP; `just smoke` does not set it.
VMON_TAP=<iface> just smoke

# Root-gated Linux jail coverage; `just smoke` and `just lima-smoke` do not enable it.
just smoke-jail

# Audit the Linux seccomp allowlist in log mode.
just seccomp-audit

# Real-VM mesh ownership, durability, and failure-mode coverage.
just cluster-e2e
```

After `just seccomp-audit`, inspect kernel audit output for denied syscalls:

```sh
journalctl -k | grep -i SECCOMP
# or
dmesg | grep -i seccomp
```

A denial indicates that the exercised syscall is outside the current allowlist; preserve the command, host, and kernel context when investigating it. The audit recipe is Linux-native; on macOS it is delegated to the Lima path.

For manual environments, do not set `VMON_E2E=1` unless guest assets and a usable hypervisor are available. Test helpers require `VMON_E2E=1` and a host backend before booting; otherwise those tests intentionally return early. Jail helpers additionally require `VMON_JAIL=1`, Linux, and effective root, and emit `SKIP jail tests: VMON_JAIL=1 but not running as root` when jail coverage was requested without privilege.

Cluster testing is not a substitute for the hermetic mesh invariant tests. The real-VM `cluster-e2e` recipe exercises the gated hardware path, while failure-mode contracts are also covered without booting guests. Run the narrowest recipe that can observe the behavior being changed, then use a supported KVM/HVF environment for guest and mesh paths.
