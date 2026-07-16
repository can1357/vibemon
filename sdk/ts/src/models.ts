import type { JsonValue } from "./function-values";
import type { SecretWire } from "./values";

/** Daemon health response. */
export interface Health {
  ok: boolean;
}

/** S3 bucket or prefix mount accepted by sandbox creation. */
export interface S3MountSpec {
  uri: string;
  endpoint?: string;
  region?: string;
  read_only?: boolean;
  access_key?: string;
  secret_key?: string;
  session_token?: string;
}

/** Sandbox creation request accepted by the gRPC create operation. */
export interface SandboxCreateRequest {
  arch?: string | null;
  block_network?: boolean;
  command?: string[] | null;
  context?: string;
  cpus?: number;
  disk_mb?: number;
  dockerfile?: string | null;
  egress_allow?: string[] | null;
  egress_allow_domains?: string[] | null;
  env?: Record<string, string> | null;
  fs_dir?: string | null;
  ha?: string | null;
  idempotency_key?: string | null;
  image?: string | null;
  inbound_cidr_allowlist?: string[] | null;
  memory?: number;
  name?: string | null;
  pool_size?: number;
  ports?: number[] | null;
  readiness_probe?: number | string | { port: number } | null;
  secrets?: SecretWire[] | null;
  /** Host-brokered credential names; credential values never enter this request. */
  credentials?: string[] | null;
  s3_mounts?: Record<string, S3MountSpec | string> | null;
  tags?: Record<string, string> | null;
  template?: string | null;
  timeout?: number | null;
  timeout_secs?: number | null;
  volumes?: Record<string, string | { name: string; read_only?: boolean }> | null;
  workdir?: string | null;
}

/** Captured or streaming exec request. */
export interface ExecRequest {
  cmd?: string[];
  env?: Record<string, string> | null;
  timeout?: number | null;
  tty?: boolean;
  workdir?: string | null;
}

/** Captured exec result. */
export interface ExecResult {
  exit: number;
  stderr_b64: string;
  stdout_b64: string;
}

/** Sandbox network policy. */
export interface NetworkPolicy {
  block_network?: boolean | null;
  cidr_allow?: string[] | null;
  domain_allow?: string[] | null;
}

/** Full VM snapshot request. */
export interface SnapshotRequest {
  name?: string | null;
  stop?: boolean;
}

/** Filesystem snapshot request. */
export interface SnapshotFilesystemRequest {
  name?: string | null;
}

/** Runtime-only fields that can change without altering captured VM devices. */
export interface SnapshotRuntimeOptions {
  agent?: boolean | null;
  command?: string[] | null;
  env?: Record<string, string> | null;
  readiness_probe?: number | string | { port: number } | null;
  secrets?: SecretWire[] | null;
  s3_mounts?: Record<string, S3MountSpec | string> | null;
  tags?: Record<string, string> | null;
  timeout?: number | null;
  timeout_secs?: number | null;
  workdir?: string | null;
}

/** Snapshot restore request. */
export interface RestoreRequest extends SnapshotRuntimeOptions {
  name?: string | null;
}

/** Atomic snapshot fork request for 1 through 32 clones. */
export interface ForkRequest extends SnapshotRuntimeOptions {
  count: number;
}

/** An immutable recovery point retained for a sandbox. */
export interface RecoveryPoint {
  name: string;
  kind: string;
  created_at_unix_millis: bigint;
  size_bytes: bigint;
}

/** Warm-pool update request. */
export type PoolSetRequest = Partial<SandboxCreateRequest> & {
  size: number;
};

/** Tolerant sandbox view returned by the daemon. */
export interface SandboxInfo {
  id: string;
  state?: string;
  status?: string;
  name?: string | null;
  tags?: Record<string, string> | null;
  node?: string | null;
  [key: string]: unknown;
}

/** Process exit status. */
export interface ExecExit {
  code: number;
  signal: number | null;
}

/** Guest filesystem entry metadata. */
export interface FileInfo {
  ok?: boolean;
  name?: string;
  type?: string;
  size?: number;
  mode?: number;
  mtime?: number;
  [key: string]: unknown;
}

/** One node advertised by mesh status. */
export interface MeshNode {
  node_id: string;
  advertise?: string | null;
  region?: string | null;
  [key: string]: unknown;
}

/** Typed mesh status response. */
export interface MeshStatus {
  self: MeshNode;
  peers: MeshNode[];
  replicas_held: number;
  [key: string]: unknown;
}

/** Warm-pool statistics. */
export interface PoolStats {
  size?: number;
  ready?: number;
  hits?: number;
  misses?: number;
  [key: string]: unknown;
}

/** Open sandbox runtime metrics keyed by subsystem or counter name. */
export interface SandboxMetrics {
  [key: string]: JsonValue;
}

/** Daemon build, host, and capability information. */
export interface ServerInfo {
  version: string;
  platform?: string;
  arch?: string;
  backend?: string;
  capabilities?: Record<string, boolean>;
  [key: string]: unknown;
}

/** Daemon-side target for one exposed guest port. */
export interface TunnelTarget {
  host: string;
  port: number;
  [key: string]: unknown;
}

/** Sandbox tunnels and proxy authorization token. */
export interface TunnelSet {
  connect_token?: string;
  tunnels?: Record<string, TunnelTarget>;
  [key: string]: unknown;
}

/** One daemon event payload. */
export interface EventRecord {
  [key: string]: JsonValue;
}
