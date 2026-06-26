import { useCallback, useEffect, useRef, useState } from "react";
import { api, subscribeToken, type SandboxView } from "./api.ts";

// Polling interval for the sandbox list. 3s matches the supervisor's cadence
// without hammering a local dev VMM.
const LIST_INTERVAL_MS = 3000;

export function useSandboxes(selectedId: string | null): {
  sandboxes: SandboxView[];
  loading: boolean;
  error: string | null;
  authError: boolean;
  refresh: () => Promise<void>;
} {
  const [sandboxes, setSandboxes] = useState<SandboxView[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [authError, setAuthError] = useState(false);
  const alive = useRef(true);

  const refresh = useCallback(async () => {
    try {
      const list = await api.listSandboxes();
      if (!alive.current) return;
      setSandboxes(list);
      setError(null);
      setAuthError(false);
    } catch (e) {
      if (!alive.current) return;
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
      setAuthError(typeof (e as { status?: number }).status === "number" && (e as { status: number }).status === 401);
    } finally {
      if (alive.current) setLoading(false);
    }
  }, []);

  useEffect(() => {
    alive.current = true;
    void refresh();
    const t = setInterval(refresh, LIST_INTERVAL_MS);
    return () => {
      alive.current = false;
      clearInterval(t);
    };
  }, [refresh]);

  // Re-fetch immediately when the token changes (e.g. the user pastes it into
  // the top bar after a 401), instead of waiting up to LIST_INTERVAL_MS.
  useEffect(() => subscribeToken(() => { void refresh(); }), [refresh]);

  // Keep the selected view fresh between list polls.
  useEffect(() => {
    if (!selectedId) return;
    let stop = false;
    const t = setInterval(async () => {
      try {
        const v = await api.getSandbox(selectedId);
        if (!stop) setSandboxes((prev) => prev.map((s) => (s.id === selectedId ? v : s)));
      } catch {
        /* 404 means terminated; the next list poll reconciles. */
      }
    }, 2000);
    return () => { stop = true; clearInterval(t); };
  }, [selectedId]);

  return { sandboxes, loading, error, authError, refresh };
}

export interface Toast {
  id: number;
  message: string;
  kind: "info" | "err";
}

export function useToasts(): {
  toasts: Toast[];
  push: (message: string, kind?: Toast["kind"]) => void;
  dismiss: (id: number) => void;
} {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const seq = useRef(0);

  const dismiss = useCallback((id: number) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, []);

  const push = useCallback((message: string, kind: Toast["kind"] = "info") => {
    const id = ++seq.current;
    setToasts((prev) => [...prev, { id, message, kind }]);
    setTimeout(() => setToasts((prev) => prev.filter((t) => t.id !== id)), 5000);
  }, []);

  return { toasts, push, dismiss };
}

// format helpers — kept here, not as tiny exported funcs, but as module fns.
export function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MiB`;
  return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GiB`;
}

export function fmtTime(unix: number | null): string {
  if (unix === null || unix === undefined) return "—";
  const d = new Date(unix * 1000);
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

export function fmtAgo(unix: number): string {
  const s = Math.max(0, Math.floor(Date.now() / 1000 - unix));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}
