// Same-origin client for the `vmon serve` v1 API: gRPC over the WebSocket
// bridge (GET /grpc, see proto/vmon/v1/bridge.proto) for every RPC.
// In production the UI is served by `vmon serve` itself, so relative URLs are
// correct regardless of host/port/proxy in front of it.

import { Code, ConnectError, createClient } from "@connectrpc/connect";
import { type JsonView, SandboxService } from "./gen/vmon/v1/api_pb.ts";
import { createWsBridgeTransport } from "./grpc-ws.ts";

export type SandboxStatus = "running" | "stopped" | "terminated" | "paused" | "failed";

export interface SandboxView {
  id: string;
  name: string;
  status: SandboxStatus;
  created_at: number;
  last_active: number;
  expires_at: number | null;
  terminated_at: number | null;
  error: string | null;
  tags: Record<string, string>;
  returncode: number | null;
  image?: string | null;
  template?: string | null;
  source?: string | null;
  cpus?: number | null;
  memory?: number | null;
  disk_mb?: number | null;
  [key: string]: unknown;
}

export interface SandboxCreate {
  image?: string | null;
  template?: string | null;
  dockerfile?: string | null;
  context?: string;
  name?: string | null;
  cpus?: number;
  memory?: number;
  disk_mb?: number;
  timeout?: number | null;
  /** Wall-clock sandbox lifetime; 0 disables the deadline. */
  timeout_secs?: number | null;
  workdir?: string | null;
  env?: Record<string, string> | null;
  secrets?: unknown[] | null;
  volumes?: Record<string, unknown> | null;
  tags?: Record<string, string> | null;
  fs_dir?: string | null;
  block_network?: boolean;
  ports?: number[] | null;
  egress_allow?: string[] | null;
  egress_allow_domains?: string[] | null;
  inbound_cidr_allowlist?: string[] | null;
  readiness_probe?: unknown;
  pool_size?: number;
  ha?: string | null;
  arch?: string | null;
  idempotency_key?: string | null;
  command?: string[] | null;
}

export interface FsEntry {
  name: string;
  type: "file" | "dir" | "symlink";
  size: number;
  mode: number;
  mtime: number;
}

export interface FsStat {
  type: "file" | "dir" | "symlink";
  size: number;
  mode: number;
  mtime: number;
}

// Per-sandbox VMM runtime counters from GET /v1/sandboxes/{id}/metrics. Values
// are either scalar counters/gauges or a named group of counters (e.g. vm_exits,
// pager). Kept open so guest/VMM-added fields render without a client change.
export type SandboxMetrics = Record<string, number | Record<string, number>>;

const TOKEN_KEY = "vmon.api_token";

// Minimal pub/sub so hooks can refresh the moment the token changes, instead
// of waiting for the next poll after the user types it into the top bar.
const tokenListeners = new Set<() => void>();

export function getToken(): string {
  return localStorage.getItem(TOKEN_KEY) ?? "";
}
export function setToken(t: string): void {
  if (t) localStorage.setItem(TOKEN_KEY, t);
  else localStorage.removeItem(TOKEN_KEY);
  for (const fn of tokenListeners) fn();
}
export function subscribeToken(fn: () => void): () => void {
  tokenListeners.add(fn);
  return () => tokenListeners.delete(fn);
}

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

// vmond error codes (vmon-code response metadata) → the HTTP statuses the JSON
// REST API used, so ApiError.status keeps its meaning (vmond/src/error.rs).
const VMON_CODE_STATUS: Record<string, number> = {
  not_found: 404,
  not_running: 409,
  busy: 409,
  invalid: 400,
  unsupported: 501,
  unauthorized: 401,
};

// Fallback when the vmon-code trailer is missing: reverse of the server's
// gRPC status mapping (see proto/vmon/v1/api.proto header).
const GRPC_CODE_STATUS: Partial<Record<Code, number>> = {
  [Code.NotFound]: 404,
  [Code.InvalidArgument]: 400,
  [Code.Unauthenticated]: 401,
  [Code.FailedPrecondition]: 409,
  [Code.Aborted]: 409,
  [Code.Unimplemented]: 501,
};

function toApiError(err: unknown): ApiError {
  const ce = ConnectError.from(err);
  const vmonCode = ce.metadata.get("vmon-code") ?? "";
  const status = VMON_CODE_STATUS[vmonCode] ?? GRPC_CODE_STATUS[ce.code] ?? 503;
  const detail = vmonCode ? `${vmonCode}: ${ce.rawMessage}` : ce.rawMessage;
  return new ApiError(status, `${status} ${detail}`);
}

async function rpc<T>(call: Promise<T>): Promise<T> {
  try {
    return await call;
  } catch (err) {
    throw toApiError(err);
  }
}

const transport = createWsBridgeTransport({
  baseUrl: window.location.origin,
  token: getToken,
});

export const sandboxClient = createClient(SandboxService, transport);

function parseView(view: JsonView): SandboxView {
  return JSON.parse(view.json) as SandboxView;
}

export const api = {
  health: async (): Promise<{ ok: boolean }> => {
    const res = await fetch("/healthz");
    if (!res.ok) {
      throw new ApiError(res.status, `${res.status} ${res.statusText || "request failed"}`);
    }
    return (await res.json()) as { ok: boolean };
  },
  sandboxMetrics: (id: string) =>
    rpc(sandboxClient.metrics({ id })).then((r) => JSON.parse(r.json) as SandboxMetrics),

  listSandboxes: () =>
    rpc(sandboxClient.list({})).then((r) =>
      r.sandboxesJson.map((s) => JSON.parse(s) as SandboxView),
    ),
  getSandbox: (id: string) => rpc(sandboxClient.get({ id })).then(parseView),
  createSandbox: (body: SandboxCreate) =>
    rpc(sandboxClient.create({ specJson: JSON.stringify(body) })).then(parseView),
  stopSandbox: (id: string) => rpc(sandboxClient.stop({ id })).then(parseView),
  terminateSandbox: (id: string) => rpc(sandboxClient.terminate({ id })).then(parseView),
  removeSandbox: (id: string) => rpc(sandboxClient.remove({ id })).then(parseView),
  snapshotSandbox: (id: string, name?: string) =>
    rpc(sandboxClient.snapshot({ id, name })).then(
      (r) => JSON.parse(r.json) as { snapshot: string; dir: string },
    ),

  fsList: (id: string, path: string) =>
    rpc(sandboxClient.fileList({ id, path })).then((r) => {
      const parsed = JSON.parse(r.json) as { entries: FsEntry[] } | FsEntry[];
      return Array.isArray(parsed) ? parsed : parsed.entries;
    }),
  fsStat: (id: string, path: string) =>
    rpc(sandboxClient.fileStat({ id, path })).then((r) => JSON.parse(r.json) as FsStat),

  readFile: async (id: string, path: string): Promise<Blob> => {
    const r = await rpc(sandboxClient.fileRead({ id, path }));
    // copy so the Blob sees a plain ArrayBuffer-backed view
    return new Blob([new Uint8Array(r.data)]);
  },
  writeFile: async (id: string, path: string, data: Blob): Promise<void> => {
    await rpc(
      sandboxClient.fileWrite({ id, path, data: new Uint8Array(await data.arrayBuffer()) }),
    );
  },
  deleteFile: async (id: string, path: string, recursive = false): Promise<void> => {
    await rpc(sandboxClient.fileDelete({ id, path, recursive }));
  },
};
