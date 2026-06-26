// Same-origin client for the `vmon serve` HTTP+WS API.
// In dev, vite.config.ts proxies /v1, /healthz, /metrics to 127.0.0.1:8000.
// In production the UI is served by `vmon serve` itself, so relative URLs are
// correct regardless of host/port/proxy in front of it.

export type SandboxStatus = "running" | "terminated" | "paused";

export interface SandboxView {
  id: string;
  name: string;
  status: SandboxStatus;
  created_at: number;
  last_active: number;
  expires_at: number | null;
  terminated_at: number | null;
  error: string | null;
  image: string | null;
  cpus: number;
  memory: number;
  disk_mb: number;
}

export interface SandboxCreate {
  image?: string | null;
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
  fs_dir?: string | null;
  block_network?: boolean;
  ports?: number[] | null;
  egress_allow?: string[] | null;
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
      return typeof body?.detail === "string" ? body.detail : JSON.stringify(body);
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
  metrics: () => req<string>("/metrics"),

  listSandboxes: () =>
    req<{ sandboxes: SandboxView[] }>("/v1/sandboxes").then((r) => r.sandboxes),
  getSandbox: (id: string) => req<SandboxView>(`/v1/sandboxes/${encodeURIComponent(id)}`),
  createSandbox: (body: SandboxCreate) =>
    req<SandboxView>("/v1/sandboxes", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
  terminateSandbox: (id: string) =>
    req<SandboxView>(`/v1/sandboxes/${encodeURIComponent(id)}`, { method: "DELETE" }),
  snapshotSandbox: (id: string, name?: string) =>
    req<{ name: string }>(`/v1/sandboxes/${encodeURIComponent(id)}/snapshot`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: name ?? null }),
    }),

  fsList: (id: string, path: string) =>
    req<{ entries: FsEntry[] }>(
      `/v1/sandboxes/${encodeURIComponent(id)}/fs/list?path=${encodeURIComponent(path)}`,
    ).then((r) => r.entries),
  fsStat: (id: string, path: string) =>
    req<FsStat>(`/v1/sandboxes/${encodeURIComponent(id)}/fs/stat?path=${encodeURIComponent(path)}`),

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
// query param when set.
export function execWsUrl(id: string, cmd: string[], tty = true): string {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  const base = `${proto}//${window.location.host}/v1/sandboxes/${encodeURIComponent(id)}/exec/ws`;
  const params = new URLSearchParams();
  for (const c of cmd) params.append("cmd", c);
  if (tty) params.append("tty", "true");
  const t = getToken();
  if (t) params.append("token", t);
  return `${base}?${params.toString()}`;
}
