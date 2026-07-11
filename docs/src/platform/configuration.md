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

Boolean environment values accept `1`, `true`, `yes`, or `on` for true and `0`, `false`, `no`, or `off` for false (case-insensitive). TOML accepts booleans and also integer `0` or `1` for `restore_quorum`.

When `replicate_sec` is not set, the effective cadence is 60 seconds with mesh enabled and 0 without it. When `restore_quorum` is not set, it is enabled when at least three members are expected. See [Mesh and High Availability](mesh.md) for the operating model.

## Start patterns

Keep non-secret policy in TOML and inject the bearer token from a protected environment source:

```sh
export VMON_API_TOKEN="$(cat /run/secrets/vmon-api-token)"
vmon serve --config /etc/vmon/serve.toml
```

For a TLS listener, configure both certificate and key together, whether through TOML, environment, or flags:

```toml
[serve]
host = "0.0.0.0"
tls_cert = "/etc/vmon/tls.crt"
tls_key = "/etc/vmon/tls.key"
```

Read [Security](security.md) before deciding where credentials and key material live. Use `vmon doctor --serve --config /etc/vmon/serve.toml` to validate the resolved server configuration along with host prerequisites.
