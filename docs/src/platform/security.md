# Security

Vibemon is not a production isolation boundary and has not received a security audit. Treat the mechanisms below as defense in depth, not as a promise that untrusted workloads can safely share a host. Security fixes target the current `main` branch and any maintained release branch.

## Threat boundary and trusted inputs

Treat guests, guest-controlled virtqueue data, and restored snapshot files as untrusted. The trusted computing base includes the `vmon` process, KVM or HVF, the host kernel, guest kernel and image inputs, disk images, snapshots, and every host path supplied at launch. Operator-supplied kernels, initrds, rootfs images, firmware, and host paths are trusted configuration, so protect their provenance and write access.

Do not expose control sockets, agent sockets, host filesystem shares, TAP devices, vmnet attachments, or user-mode forwarded ports across trust boundaries. Control and agent sockets are operator-owned, mode `0600`, must have private parent directories, and on Linux accept only root or the launch UID. Apply external host network policy around the gateway and exposed guest ports.

## Gateway authentication and TLS

The mesh operator bearer token is shared by every node and grants full control. Set it using `--token` or `VMON_API_TOKEN`, keep it out of logs and shell history, and limit access to its configuration file. A non-loopback gateway must have an operator token; `vmon doctor --serve --config <path>` reports a missing token as a failure.

Use a separate scoped client token for workloads that need ordinary sandbox operations but not mesh administration:

```sh
# server configuration environment
export VMON_API_TOKEN=<operator-token>
export VMON_CLIENT_TOKEN=<client-token>

# client receives only its scoped value through the usual variable
VMON_API_TOKEN=<client-token> vmon ps
```

The client token allows normal sandbox operations such as `run`, `exec`, and `ps`. It cannot use `vmon mesh ...`, migrate, or mesh-admin HTTP routes; those return `403`. During rotation, each operator or client token value may be a comma-separated list such as `old,new`; any listed value authorizes only within its own tier. Remove the old value after clients and gateways have changed over.

TLS is optional in the server configuration, but use it for networked gateways when the network is not already protected by an appropriate trusted transport. Certificate and key must be configured together:

```sh
vmon serve --tls-cert /path/to/cert.pem --tls-key /path/to/key.pem
```

For a TLS mesh, advertise `https://` URLs. Peers then use WSS for inter-node exec proxying. TLS protects transport; it does not reduce the authority of a bearer token, isolate guests, or supply NAT traversal.

Contexts do not persist tokens by default. `vmon context create ... --save-token` opts in to a private credential file under `$VMON_HOME/credentials/`; otherwise clients obtain a full or scoped token from `VMON_API_TOKEN` at connection time.

## Host-process filters

The default Stage-B sandbox runs after VM backends are opened and before VCPU or device-worker threads start. It applies:

- `no_new_privs` and an optional root-to-specified-UID/GID drop;
- tightened resource limits, including no core dumps and a bounded file-descriptor limit;
- Landlock path rules for configured read-only and read/write files and directory trees; and
- a seccomp allowlist. Its default deny action is `errno`; `--seccomp-action kill` maps to a diagnostic SIGSYS trap, while `log` supports auditing.

These filters are default-on. `--no-sandbox` disables the Stage-B filters for local development and cannot be combined with `--jail`; do not use it for a deployment that relies on host-process restriction.

Landlock applies the configured path policy on a best-effort compatibility level. It is not a substitute for choosing dedicated host directories with correct ownership and permissions. A writable named volume or writable disk image intentionally remains writable where the configuration grants it.

`--remote-fs` grants the VMM access to an operator-selected Unix-socket proxy,
not to an arbitrary guest-selected path. Keep its socket and parent directory
private. With `--jail`, the jailer first tries to bind only that socket; if the
kernel cannot bind-mount a Unix-socket inode, it bind-mounts the socket's
parent directory as a fallback. Therefore, dedicate that parent directory to
the proxy and do not place credentials, control sockets, or unrelated files
there. The configured absolute socket path and each traversed parent must also
remain accessible under the VMM's Landlock policy and ordinary permissions.

Daemon-managed S3 mounts keep their per-VM proxy socket and S3 credentials on
the host. The remote filesystem exposed to the guest is read-only; a
non-read-only S3 request adds only a guest-local volatile overlay. Do not infer
that proxy access, a guest overlay, or a snapshot gives the guest S3
credentials or permission to write S3 objects.

## Linux jail

`--jail` is the stronger Linux production path. It requires root and an `--id`; it creates a private jail tree, cgroup v2 placement, mount/PID/IPC/UTS namespaces, makes mounts private, pivots into the jail root, pre-binds operator-owned sockets, then applies the same Stage-B filters and drops to the sandbox identity.

```sh
sudo vmon vmm \
  --jail --id job-42 --jail-root /srv/jailer/job-42 \
  --sandbox-uid <uid> --sandbox-gid <gid> \
  --kernel <kernel-image> --initrd <initramfs.cpio.gz> \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"
```

The jail requires cgroup v2 mounted at `/sys/fs/cgroup` unless `--cgroup-mode off` is selected. With cgroup v2, it enables and configures CPU, memory, and PID controllers beneath `/sys/fs/cgroup/vmon`; default memory is derived from the guest memory setting and the default PID limit is 1024. Only explicitly configured paths and essential devices are bound into the jail. The isolation boundary still depends on the host kernel, hypervisor, device backends, and supplied images.

Jail and Stage-B filtering do not turn host shares or host networking into untrusted interfaces. Keep `--fs-dir` shares dedicated and read-only; explicitly consider the host access granted by writable volumes, TAP, network namespaces, and port forwarding.

## Firmware and image supply

Vibemon does not vendor UEFI firmware. Supply it explicitly with `--firmware`; pin the firmware build, record its source URL, verify a SHA-256 digest before use, and keep rollback artifacts. Treat firmware upgrades like hypervisor upgrades. Do not fetch unsigned firmware at VM launch time.

## Reporting a vulnerability

Do not open public issues for suspected vulnerabilities. Use this repository's GitHub **Security → Report a vulnerability** flow. If private reporting is unavailable, contact maintainers out of band before publication. Include the affected commit or version, host architecture and Linux/KVM version, reproducing guest inputs, whether `/dev/kvm`, TAP, virtio-fs, snapshots, or the control socket are involved, and expected versus observed impact.
