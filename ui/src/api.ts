// Same-origin client for the `vmon serve` HTTP+WebSocket v1 API.
// In dev, vite.config.ts proxies /v1, /healthz, and /metrics to 127.0.0.1:8000.
// In production the UI is served by `vmon serve` itself, so relative URLs are
// correct regardless of host/port/proxy in front of it.

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

function authHeaders(): Record<string, string> {
  const t = getToken();
  return t ? { Authorization: `Bearer ${t}` } : {};
}

async function responseErrorDetail(res: Response): Promise<string> {
  const fallback = res.statusText || "request failed";
  const ct = res.headers.get("content-type") ?? "";
  try {
    if (ct.includes("application/json")) {
      const body = await res.json();
      if (typeof body?.message === "string") {
        return typeof body?.code === "string" ? `${body.code}: ${body.message}` : body.message;
      }
      if (typeof body?.detail === "string") return body.detail;
      return JSON.stringify(body);
    }
    const text = await res.text();
    return text || fallback;
  } catch {
    return fallback;
  }
}

async function throwApiError(res: Response): Promise<never> {
  throw new ApiError(res.status, `${res.status} ${await responseErrorDetail(res)}`);
}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: { ...authHeaders(), ...(init?.headers ?? {}) },
  });
  if (!res.ok) await throwApiError(res);
  if (res.status === 204) return undefined as T;
  const ct = res.headers.get("content-type") ?? "";
  if (ct.includes("application/json")) return res.json() as Promise<T>;
  return (await res.text()) as unknown as T;
}

export const api = {
  health: () => req<{ ok: boolean }>("/healthz"),
  sandboxMetrics: (id: string) =>
    req<SandboxMetrics>(`/v1/sandboxes/${encodeURIComponent(id)}/metrics`),

  listSandboxes: () => req<{ sandboxes: SandboxView[] }>("/v1/sandboxes").then((r) => r.sandboxes),
  getSandbox: (id: string) => req<SandboxView>(`/v1/sandboxes/${encodeURIComponent(id)}`),
  createSandbox: (body: SandboxCreate) =>
    req<SandboxView>("/v1/sandboxes", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
  stopSandbox: (id: string) =>
    req<SandboxView>(`/v1/sandboxes/${encodeURIComponent(id)}/stop`, { method: "POST" }),
  terminateSandbox: (id: string) =>
    req<SandboxView>(`/v1/sandboxes/${encodeURIComponent(id)}/terminate`, { method: "POST" }),
  removeSandbox: (id: string) =>
    req<SandboxView>(`/v1/sandboxes/${encodeURIComponent(id)}`, { method: "DELETE" }),
  snapshotSandbox: (id: string, name?: string) =>
    req<{ snapshot: string; dir: string }>(`/v1/sandboxes/${encodeURIComponent(id)}/snapshots`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: name ?? null }),
    }),

  fsList: (id: string, path: string) =>
    req<{ entries: FsEntry[] } | FsEntry[]>(
      `/v1/sandboxes/${encodeURIComponent(id)}/files/list?path=${encodeURIComponent(path)}`,
    ).then((r) => (Array.isArray(r) ? r : r.entries)),
  fsStat: (id: string, path: string) =>
    req<FsStat>(`/v1/sandboxes/${encodeURIComponent(id)}/files/stat?path=${encodeURIComponent(path)}`),

  // file blob I/O — uses fetch directly to preserve binary.
  readFile: async (id: string, path: string): Promise<Blob> => {
    const res = await fetch(
      `/v1/sandboxes/${encodeURIComponent(id)}/files?path=${encodeURIComponent(path)}`,
      { headers: authHeaders() },
    );
    if (!res.ok) await throwApiError(res);
    return res.blob();
  },
  writeFile: async (id: string, path: string, data: Blob): Promise<void> => {
    const res = await fetch(
      `/v1/sandboxes/${encodeURIComponent(id)}/files?path=${encodeURIComponent(path)}`,
      { method: "PUT", headers: authHeaders(), body: data },
    );
    if (!res.ok) await throwApiError(res);
  },
  deleteFile: async (id: string, path: string, recursive = false): Promise<void> => {
    const res = await fetch(
      `/v1/sandboxes/${encodeURIComponent(id)}/files?path=${encodeURIComponent(path)}&recursive=${recursive}`,
      { method: "DELETE", headers: authHeaders() },
    );
    if (!res.ok) await throwApiError(res);
  },
};

// Build an exec WebSocket URL from the current page origin (so wss:// is used
// automatically when the page is served over HTTPS), appending the token as a
// query param because browsers cannot attach Authorization headers to WebSocket
// constructors. The first message carries the exec request body.
export function execWsUrl(id: string): string {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  const base = `${proto}//${window.location.host}/v1/sandboxes/${encodeURIComponent(id)}/exec`;
  const params = new URLSearchParams();
  const t = getToken();
  if (t) params.append("token", t);
  const query = params.toString();
  return query ? `${base}?${query}` : base;
}

export function execStartFrame(cmd: string[], tty = true): string {
  return JSON.stringify({ cmd, tty });
}

export function stdinFrame(data: string): string {
  return JSON.stringify({ stdin_b64: bytesToB64(new TextEncoder().encode(data)) });
}

export function eofFrame(): string {
  return JSON.stringify({ eof: true });
}

export function resizeFrame(rows: number, cols: number): string {
  return JSON.stringify({ resize: [rows, cols] });
}

export function b64ToBytes(b64: string): Uint8Array {
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

function bytesToB64(bytes: Uint8Array): string {
  let binary = "";
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
  }
  return btoa(binary);
}
