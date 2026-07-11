# Troubleshooting

Start with the Rust CLI's local diagnosis rather than assuming a Python or Uvicorn service exists: the deployed server is `vmon serve` from the Rust `vmond` crate (axum + tonic). The gRPC API is the primary control plane; HTTP is used for health, metrics, and port proxying.

## First pass: local prerequisites and resolved server settings

Run the local prerequisite checklist:

```sh
vmon doctor
```

Expected signals include a usable `vmon` binary, a usable hypervisor (`/dev/kvm` present and writable on Linux, or `kern.hv_support=1` on macOS), image tooling, filesystem tooling, a guest kernel/agent status, and daemon status. A missing local `vmond.sock` is a warning with the hint to run `vmon serve`; a socket that exists but does not answer `/healthz` is also a warning. A hard failure means the relevant prerequisite is not usable.

Validate the server configuration that a gateway will resolve, including source precedence and mesh-sensitive checks:

```sh
vmon doctor --serve --config /path/to/serve.toml
```

Expected signals:

- a non-loopback bind without an operator token is a **fail**; loopback without a token is a warning;
- TLS is **ok** only when certificate and key are both set or both unset, and is a **fail** when only one is set;
- `replicas > 0` with `replicate_sec = 0` is a **fail**;
- automatic cadence reports `auto (60s when mesh is enabled)`;
- two expected members produce a restore-quorum warning because they cannot form a post-failure majority; and
- the advertised URL row reflects the configured host and port. It verifies configuration, not network reachability from other nodes.

On Linux, correct `/dev/kvm` access by enabling KVM and granting the intended user access to the `kvm` group, then starting a new login session. On Apple Silicon macOS, build through the supplied recipe so the binary receives the Hypervisor entitlement; `vmon doctor` reports a missing entitlement as a failure.

## Gateway or context cannot connect

Check the local server socket and a selected network gateway separately:

```sh
vmon doctor
curl -fsS http://<gateway>:8000/healthz
```

A healthy network endpoint returns success from `/healthz`; connection refusal, timeout, or a non-success HTTP result indicates that the URL is not a usable gateway. For TLS endpoints, use `https://` and ensure the certificate/key pair was configured together.

List saved contexts and select the one to use:

```sh
vmon context ls
vmon context use prod
```

The list shows each saved context's ordered endpoint roster and whether its token is saved or expected from the environment. A context can fail over only if at least one saved advertised endpoint is reachable. It retries replay-safe work only on connection-establishment failure. Do not retry an interactive operation merely because the client lost contact after sending it: attached run, exec, shell, snapshot, fork, and extend are deliberately executed once after a single health probe.

If a context was created with `--save-token`, its credential is under `$VMON_HOME/credentials/`; otherwise provide the appropriate token through `VMON_API_TOKEN`. A `403` from mesh administration while ordinary sandbox operations work is the expected result for a scoped `VMON_CLIENT_TOKEN`; use the full operator token for administration.

## Mesh membership, placement, and recovery

Use the mesh view before declaring a node failed:

```sh
VMON_API_TOKEN=<operator-token> vmon mesh status
```

The output reports members, health, capacity, local sandboxes, durability tiers, checkpoint age/RPO, replicas held, warnings, and per-node replication, restore, and fence counters. A two-node warning about quorum restore is expected: two expected members cannot confirm a failed peer by post-failure majority.

When a join appears successful but traffic fails, compare each node's advertised URL with the route available from every peer and client. There is no NAT relay. Rebuild the mesh advertisement with a LAN/VPC address or an overlay address that is actually reachable:

```sh
VMON_API_TOKEN=<operator-token> vmon mesh setup --advertise http://<reachable-ip>:8000
```

For HTTPS, advertise the matching `https://` URL. An address reachable only from the node that announced it is an invalid mesh advertisement.

Placement errors are intentional protection, not a fallback request:

- `arch_required`: a mixed live-architecture mesh could not derive the image architecture. Retry with `--arch x86_64` or `--arch aarch64`.
- `unplaceable`: no compatible live capacity exists. Inspect live members and image/backend compatibility.
- `invalid` for `fs_dir`: host-local shares cannot be safely placed in a mesh; use a named volume.
- `unsupported` for a writable volume: the mesh has fewer than three nodes, so no quorum lease is possible. Read-only volumes remain available.

A sandbox that does not restore immediately after owner loss may be correctly deferred. Automatic quorum restore at three or more expected members requires a strict majority to confirm the former owner unreachable. It retries when quorum is short, the old owner remains reachable, the elected owner is wrong, a replica or secret is missing, or restore cannot safely proceed. Epoch fencing is best effort and converges after gossip; it is not evidence that two partitioned writers could never run. Writable volumes rely on their quorum lease and self-fence on missed renewal.

For planned node removal, drain the node before leaving the mesh:

```sh
VMON_API_TOKEN=<operator-token> vmon mesh leave --drain
```

## Sandbox launch, networking, and isolation failures

Check the host/backend combination before treating a guest boot failure as an image failure:

- Linux requires a writable `/dev/kvm`; TAP networking is Linux/KVM only.
- macOS requires Apple Silicon, macOS 15+, HVF support, and a binary signed with `com.apple.security.hypervisor`. Use `--net user` for entitlement-free user-mode NAT. `--tap` is expected to fail for the ad-hoc-signed binary because vmnet-style networking requires unavailable entitlements.
- `--net user` is not LAN bridging or inbound host port forwarding. After a user-net restore, guest-visible NAT state is restored but existing host-side TCP flows reset.

If a jailed launch fails immediately, run it as root, supply a valid `--id`, and check that cgroup v2 is mounted at `/sys/fs/cgroup`. `--jail` is Linux-only and cannot be combined with `--no-sandbox`. The default Stage-B seccomp, Landlock, `no_new_privs`, and resource-limit filters are active unless `--no-sandbox` is explicitly selected for local development.

For an unexpected denied syscall on Linux, use the focused audit recipe from [Testing](testing.md). Its expected next signal is a kernel audit record, inspectable with `journalctl -k | grep -i SECCOMP` or `dmesg | grep -i seccomp`. Do not disable sandboxing as a production workaround; determine whether the requested backend, host capability, or launch configuration is supported.

## Snapshot and volume compatibility

Restore and fork need a matching architecture, hypervisor backend, and supported snapshot version. A KVM snapshot cannot restore on an HVF build, and an arm64 snapshot cannot restore on x86_64. Named volume data is not copied into a snapshot; it is reattached by name. In a mesh, writable volume mounts additionally need the three-node quorum lease described above.
