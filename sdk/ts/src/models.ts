import type { components } from "./schema";
import type { PortableValue } from "./function-values";

/** Daemon health response. */
export type Health = components["schemas"]["HealthBody"];
/** Sandbox creation request. */
export type SandboxCreateRequest = components["schemas"]["SandboxCreate"];
/** Captured or streaming exec request. */
export type ExecRequest = components["schemas"]["ExecBody"];
/** Captured exec result. */
export type ExecResult = components["schemas"]["ExecCaptureBody"];
/** Sandbox network policy. */
export type NetworkPolicy = components["schemas"]["NetworkBody"];
/** Full VM snapshot request. */
export type SnapshotRequest = components["schemas"]["SnapshotBody"];
/** Filesystem snapshot request. */
export type SnapshotFilesystemRequest = components["schemas"]["SnapshotFsBody"];
/** Snapshot restore request. */
export type RestoreRequest = components["schemas"]["RestoreBody"];
/** Snapshot fork request. */
export type ForkRequest = components["schemas"]["ForkBody"];
/** Warm-pool update request. */
export type PoolSetRequest = components["schemas"]["PoolPutBody"];

/** Tolerant sandbox view returned by the daemon. */
export interface SandboxInfo {
  id: string;
  node?: string | null;
  state?: string;
  status?: string;
  name?: string | null;
  tags?: Record<string, string> | null;
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
  [key: string]: PortableValue;
}

/** Daemon build, host, and capability information. */
export interface ServerInfo {
  version?: string;
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
  [key: string]: PortableValue;
}
