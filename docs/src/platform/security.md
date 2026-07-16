# Security

Vibemon is not a production isolation boundary and has not received a security audit. Treat the mechanisms below as defense in depth, not as a promise that untrusted workloads can safely share a host. Security fixes target the current `main` branch and any maintained release branch.

## Threat boundary and trusted inputs

Treat guests, guest-controlled virtqueue data, and restored snapshot files as untrusted. The trusted computing base includes `vmon`, KVM/HVF/WHP, the host kernel, guest kernel and image inputs, disk images, snapshots, and every host path supplied at launch.

Do not expose control endpoints, agent endpoints, host filesystem shares, TAP devices, vmnet attachments, or forwarded ports across trust boundaries. Unix control and agent sockets are operator-owned and mode `0600`. Windows named pipes use a local-only owner/SYSTEM ACL and reject remote clients. Apply external host network policy around gateways and exposed guest ports.

## Tenant boundary and encryption

Each authenticated tenant token resolves to one tenant ID. Tenant requests are confined to resources owned by that ID, including sandboxes, snapshots, volumes, recovery history, and credentials. A tenant cannot select another tenant in an RPC; the optional tenant fields on credential RPCs are for administrators only. Administrators and local Unix-socket callers can cross this boundary.

Configure the tenant-token and customer-key mappings as JSON maps. Tenant IDs must match `[A-Za-z0-9_-]{1,64}`. A tenant without an entry in `tenant_keys` uses the host `default` key.

```toml
[serve]
tenant_tokens = { "tenant-token-from-secret-store" = "acme" }
tenant_keys = { acme = "acme-kms-2026-07" }
```

The key ID identifies a 32-byte hex key file at `$VMON_HOME/security/keys/<key-id>.key`. The file must be a regular file, mode `0600` or stricter, and not group- or world-readable. The daemon rejects a request that needs an unavailable or malformed key rather than writing unencrypted data.

New snapshot archives, credential records, and persistent volume archives are authenticated and encrypted with the owning tenant's key ID. Existing encrypted data retains its recorded key ID: keep that key available for restore, rollback, volume attachment, or credential resolution. Deleting or replacing a key before its encrypted data is deleted makes that data unavailable; key-ID assignment is not a key-management service or a data re-encryption operation.

## Credential broker

Credentials are tenant-local names. A sandbox creation request may reference a name through `credentials`, but it never carries credential values. The host-only gateway resolves the encrypted record and injects its configured HTTP headers only for a permitted domain, subject to expiry and the configured requests-per-minute limit. The guest receives neither the header values nor a reusable credential.

The `CredentialService` stores a credential's allowed domains, header names, expiry, request limit, and version as non-secret metadata. `Put` creates or atomically rotates a record; `Delete` revokes it immediately. The service rejects empty names, records without both an allowed domain and an injected header, invalid domains, and cross-tenant access. Gateway requests with an invalid capability, unknown credential name, expired record, target domain outside the allowlist, or exhausted rate limit fail closed.

Use a generated gRPC client to administer credentials. The convenience SDKs expose credential *references* on sandbox creation; they do not expose a second secret store. The wire request is `CredentialService.Put(PutCredentialRequest)`, with `name`, `allowed_domains`, `headers`, optional `expires_at_unix_millis`, and `requests_per_minute`. Its response and `List` contain metadata only; header values are never returned.

For example, an operator can create a record with a reflection-enabled gRPC
client. `value` is protobuf JSON bytes, so it is base64-encoded:

```sh
grpcurl -plaintext \
  -H "authorization: Bearer $VMON_API_TOKEN" \
  -d '{"name":"github-api","allowedDomains":["api.github.com"],"headers":[{"name":"Authorization","value":"QmVhcmVyIHRva2VuLWZyb20tc2VjdXJlLXN0b3Jl"}],"requestsPerMinute":60}' \
  127.0.0.1:8000 vmon.v1.CredentialService/Put
```

Replace the sample header value with a base64-encoded value from a secure
source. This command's response contains the credential name, header name,
domain, limit, and version, never the value.

A sandbox that attaches credential names receives
`VMON_CREDENTIAL_GATEWAY`, an opaque per-sandbox URL. Send a `POST` to that
exact URL with the credential name and an HTTPS target; do not derive, log, or
persist a replacement URL. The capability in the path authorizes access to
that sandbox's attached names only.

```sh
curl --fail-with-body --request POST "$VMON_CREDENTIAL_GATEWAY" \
  --header 'content-type: application/json' \
  --data '{"credential":"github-api","method":"GET","url":"https://api.github.com/user","headers":{},"body_base64":""}'
```

The response is JSON with `status`, non-secret response `headers`, and
base64-encoded `body_base64`. The gateway accepts HTTPS targets without
embedded credentials, rejects `CONNECT` and `TRACE`, refuses private target
resolution, and limits a request or response body to 16 MiB. On Linux,
credential brokering requires the sandbox TAP path and its broker rule; on
macOS it uses restricted user-mode networking. Gateway, policy, upstream, and
rate-limit failures are returned to the guest request without exposing the
stored header values.

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

Machine snapshots capture arbitrary bytes from guest RAM. A sandbox that has
received secrets can therefore place those values in snapshot and replica
artifacts. Daemon-managed snapshots are encrypted with the owning tenant's key
ID, but encryption does not reduce who can read a key or restore an artifact.
Protect snapshot storage and key files as secret-bearing data. Live mesh
migration also carries the runtime secret environment over the
bearer-authenticated cluster channel so the destination retains the binding.

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
