# Configuration

`vmon serve` resolves one server configuration. It does not merge arbitrary configuration sections or silently accept misspelled keys.

## Precedence and config-file selection

For every server setting, values are applied in this order, with later sources winning:

1. built-in defaults;
2. an optional TOML file;
3. `VMON_*` environment variables;
4. explicit `vmon serve` flags.

Select the TOML file with `vmon serve --config PATH`; if that flag is absent, `VMON_CONFIG` selects it. A non-empty `--config` wins over `VMON_CONFIG`. `~` is expanded in this path and in `home`. The file is read before environment values are overlaid, so environment variables can override a checked-in or provisioned file.

The file must be a TOML table. It may be either a flat table or contain exactly one top-level `[serve]` table. Unknown top-level keys in the latter form, unknown server keys, malformed values, and unreadable files are errors.

```toml
# /etc/vmon/serve.toml
[serve]
host = "127.0.0.1"
port = 8000
home = "/var/lib/vmon"
token = "replace-with-a-secret"
idle_timeout = 300
warm_pool_size = 1
tenant_tokens = { "tenant-token-from-secret-store" = "acme" }
tenant_keys = { acme = "acme-kms-2026-07" }
history_disk_sec = 300
history_checkpoint_sec = 3600
history_retention = 24
history_max_age_sec = 604800
network_broker_socket = "/run/vmon/network-broker.sock"
warm_images = ["alpine:latest=2"]
```

```sh
VMON_SERVE_PORT=9000 vmon serve --config /etc/vmon/serve.toml --port 9443
```

In this example the effective port is `9443`: the flag overrides the environment, which would otherwise override `port = 8000` in the file.

## Settings

The following table gives the TOML key, environment variable, and flag. String values supplied by environment or flags are parsed into the setting's required type. Empty `token`, `client_token`, `tls_cert`, and `tls_key` values disable those optional settings.

| TOML key | Environment | Flag | Meaning |
| --- | --- | --- | --- |
| `home` | `VMON_HOME` | `--home` | Server home directory; default `$HOME/.vmon`. |
| `host` | `VMON_SERVE_HOST` | `--host` | TCP bind host; default `127.0.0.1`. |
| `port` | `VMON_SERVE_PORT` | `--port` | TCP bind port; default `8000`. |
| `token` | `VMON_API_TOKEN` | `--token` | Admin bearer token. |
| `client_token` | `VMON_CLIENT_TOKEN` | `--client-token` | Restricted client bearer token. |
| `tenant_tokens` | `VMON_TENANT_TOKENS` | `--tenant-tokens` | JSON map from bearer token to tenant ID. Tenant IDs are limited to `[A-Za-z0-9_-]{1,64}`. |
| `tenant_keys` | `VMON_TENANT_KEYS` | `--tenant-keys` | JSON map from tenant ID to customer-managed key ID; missing entries use `default`. |
| `network_broker_socket` | `VMON_NETWORK_BROKER_SOCKET` | — | Absolute Linux network-broker socket path. Start the broker separately with `vmon net-broker --socket PATH --owner-uid "$(id -u vmon)"` when it runs as root, or run a dedicated `CAP_NET_ADMIN` broker copy as the daemon service UID. There is no `vmon serve` flag. |
| `history_disk_sec` | `VMON_HISTORY_DISK_SEC` | `--history-disk-sec` | Rolling disk recovery-point cadence in seconds; zero disables it; default 300. |
| `history_checkpoint_sec` | `VMON_HISTORY_CHECKPOINT_SEC` | `--history-checkpoint-sec` | Full-checkpoint recovery-point cadence in seconds; zero disables it; default 3600. |
| `history_retention` | `VMON_HISTORY_RETENTION` | `--history-retention` | Recovery-point count limit; default 24. Production portable history applies it independently to each `disk` and `checkpoint` tier. |
| `history_max_age_sec` | `VMON_HISTORY_MAX_AGE_SEC` | `--history-max-age-sec` | Recovery-point maximum age in seconds; zero disables age pruning; default 604800. |
| `template_ttl_sec` | `VMON_TEMPLATE_TTL_SEC` | `--template-ttl-sec` | Maximum age for unused image templates in seconds; default 2592000. |
| `function_artifact_max_bytes` | `VMON_FUNCTION_ARTIFACT_MAX_BYTES` | `--function-artifact-max-bytes` | Maximum uncompressed streamed function artifact, from 1 through 2^50 bytes; default 4 GiB. |
| `tls_cert` | `VMON_TLS_CERT` | `--tls-cert` | TLS certificate path. |
| `tls_key` | `VMON_TLS_KEY` | `--tls-key` | TLS private-key path. |
| `idle_timeout` | `VMON_IDLE_TIMEOUT` | `--idle-timeout` | Positive idle timeout in seconds; default 300. |
| `replicate_sec` | `VMON_REPLICATE_SEC` | `--replicate-sec` | Non-negative replica checkpoint cadence in seconds. |
| `replicas` | `VMON_REPLICAS` | `--replicas` | Desired asynchronous replica count; default 1. |
| `replicate_concurrency` | `VMON_REPLICATE_CONCURRENCY` | `--replicate-concurrency` | Positive concurrent replica-transfer limit; default 2. |
| `restore_quorum` | `VMON_RESTORE_QUORUM` | `--restore-quorum` / `--no-restore-quorum` | Override automatic restore quorum choice. |
| `warm_pool_size` | `VMON_WARM_POOL_SIZE` | `--warm-pool-size` | Default ready instances per warm image; default 1. |
| `warm_images` | `VMON_WARM_IMAGES` | `--warm-images` | Comma- or whitespace-separated image entries, optionally `reference=count`; TOML also accepts a string array. |
| `mesh_heartbeat_sec` | `VMON_MESH_HEARTBEAT_SEC` | `--mesh-heartbeat-sec` | Positive mesh heartbeat interval. |
| `mesh_reap_sec` | `VMON_MESH_REAP_SEC` | `--mesh-reap-sec` | Positive mesh member-reap age. |
| `mesh_idem_ttl_sec` | `VMON_MESH_IDEM_TTL_SEC` | `--mesh-idem-ttl-sec` | Positive mesh idempotency-cache TTL. |
| `mesh_create_timeout_sec` | `VMON_MESH_CREATE_TIMEOUT_SEC` | `--mesh-create-timeout-sec` | Positive mesh create-dispatch timeout. |
| `mesh_w_warm` | `VMON_MESH_W_WARM` | `--mesh-w-warm` | Positive warm-capacity placement weight. |
| `mesh_w_free` | `VMON_MESH_W_FREE` | `--mesh-w-free` | Positive free-capacity placement weight. |
| `mesh_w_local` | `VMON_MESH_W_LOCAL` | `--mesh-w-local` | Positive local-node placement weight. |
| `mesh_w_region` | `VMON_MESH_W_REGION` | `--mesh-w-region` | Positive region-affinity placement weight. |
| `mesh_w_inflight` | `VMON_MESH_W_INFLIGHT` | `--mesh-w-inflight` | Positive inflight-create placement penalty. |
| `cluster_mode` | `VMON_CLUSTER_MODE` | `--cluster-mode` | `single-node` (default) or `production`. Production makes PostgreSQL authoritative for ownership/lifecycle and S3 authoritative for portable objects. |
| `postgres_url` | `VMON_POSTGRES_URL` | `--postgres-url` | PostgreSQL connection URL; required in production mode. |
| `s3_endpoint` | `VMON_S3_ENDPOINT` | `--s3-endpoint` | S3-compatible endpoint URL; required in production mode. Custom endpoints use path-style requests. |
| `s3_bucket` | `VMON_S3_BUCKET` | `--s3-bucket` | S3 bucket for portable history, replicas, and function artifacts; required in production mode. |
| `s3_region` | `VMON_S3_REGION` | `--s3-region` | S3 region; required in production mode. |
| `s3_access_key` | `VMON_S3_ACCESS_KEY` | `--s3-access-key` | S3 access key; required in production mode. Prefer a protected environment source. |
| `s3_secret_key` | `VMON_S3_SECRET_KEY` | `--s3-secret-key` | S3 secret key; required in production mode. Prefer a protected environment source. |
| `s3_prefix` | `VMON_S3_PREFIX` | `--s3-prefix` | Optional object-key prefix shared by every node in the cluster. |
| `s3_multipart_stale_sec` | `VMON_S3_MULTIPART_STALE_SEC` | `--s3-multipart-stale-sec` | Stale multipart-upload cleanup age; default seven days. Zero disables cleanup; a positive value must be at least 3600 seconds. |
| `portable_history_key_id` | `VMON_PORTABLE_HISTORY_KEY_ID` | `--portable-history-key-id` | Non-`default` shared key ID required in production. Every node must provision identical key material at `$VMON_HOME/security/keys/<id>.key`. |

Boolean environment values accept `1`, `true`, `yes`, or `on` for true and `0`, `false`, `no`, or `off` for false (case-insensitive). TOML accepts booleans and also integer `0` or `1` for `restore_quorum`.

When `replicate_sec` is not set, the effective cadence is 60 seconds with mesh enabled and 0 without it. When `restore_quorum` is not set, it is enabled when at least three members are expected. See [Mesh and High Availability](mesh.md) for the operating model.

## Start patterns

Keep non-secret policy in TOML and inject the bearer token from a protected environment source:

```sh
export VMON_API_TOKEN="$(cat /run/secrets/vmon-api-token)"
vmon serve --config /etc/vmon/serve.toml
```

Production mode is an explicit hard-dependency mode. Every node must resolve
the same PostgreSQL database, S3 namespace, and portable-history key:

```toml
[serve]
cluster_mode = "production"
postgres_url = "postgresql://vmon@postgres/vmon"
s3_endpoint = "https://s3.example.internal"
s3_bucket = "vmon-production"
s3_region = "us-east-1"
s3_prefix = "cluster-a/"
portable_history_key_id = "cluster-recovery"
```

Supply `VMON_S3_ACCESS_KEY` and `VMON_S3_SECRET_KEY` from the deployment's
secret store. Provision the same 32-byte hex key as
`$VMON_HOME/security/keys/cluster-recovery.key` on every node with mode `0600` or
stricter. The server rejects production configuration with any missing
requirement; it does not fall back to node-local ownership or unencrypted
objects. See [Security](security.md) for the key-file contract.

For a TLS listener, configure both certificate and key together, whether through TOML, environment, or flags:

```toml
[serve]
host = "0.0.0.0"
tls_cert = "/etc/vmon/tls.crt"
tls_key = "/etc/vmon/tls.key"
```

Read [Security](security.md) before deciding where credentials and key material live. Use `vmon doctor --serve --config /etc/vmon/serve.toml` to validate the resolved server configuration along with host prerequisites.
