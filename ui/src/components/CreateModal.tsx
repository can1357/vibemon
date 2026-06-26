import { useState } from "react";
import { api, type SandboxCreate } from "../api.ts";

const DEFAULTS: SandboxCreate = {
  image: "alpine",
  cpus: 1,
  memory: 512,
  disk_mb: 1024,
  timeout: 300,
  block_network: true,
};

export function CreateModal({
  onClose,
  onCreated,
}: {
  onClose: () => void;
  onCreated: (name: string) => void;
}): React.ReactElement {
  const [form, setForm] = useState<SandboxCreate>(DEFAULTS);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const set = <K extends keyof SandboxCreate>(k: K, v: SandboxCreate[K]) =>
    setForm((f) => ({ ...f, [k]: v }));

  async function submit(): Promise<void> {
    setBusy(true);
    setError(null);
    try {
      const v = await api.createSandbox(form);
      onCreated(v.name);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="overlay" onMouseDown={onClose}>
      <div className="modal" onMouseDown={(e) => e.stopPropagation()}>
        <div className="modal__head">
          <h3>New sandbox</h3>
          <button className="btn btn--ghost btn--sm" onClick={onClose} aria-label="Close">✕</button>
        </div>
        <div className="modal__body">
          <label className="field">
            <span className="label">Image</span>
            <input
              className="input mono"
              value={form.image ?? ""}
              placeholder="alpine, python:3.12, …"
              onChange={(e) => set("image", e.currentTarget.value || null)}
            />
          </label>
          <div className="form-row">
            <label className="field">
              <span className="label">vCPUs</span>
              <input
                className="input mono"
                type="number" min={1} max={64}
                value={form.cpus ?? 1}
                onChange={(e) => set("cpus", Number(e.currentTarget.value))}
              />
            </label>
            <label className="field">
              <span className="label">Memory (MiB)</span>
              <input
                className="input mono"
                type="number" min={32}
                value={form.memory ?? 512}
                onChange={(e) => set("memory", Number(e.currentTarget.value))}
              />
            </label>
            <label className="field">
              <span className="label">Disk (MiB)</span>
              <input
                className="input mono"
                type="number" min={256}
                value={form.disk_mb ?? 1024}
                onChange={(e) => set("disk_mb", Number(e.currentTarget.value))}
              />
            </label>
            <label className="field">
              <span className="label">Name (optional)</span>
              <input
                className="input mono"
                value={form.name ?? ""}
                placeholder="auto"
                onChange={(e) => set("name", e.currentTarget.value || null)}
              />
            </label>
          </div>
          <label className="field">
            <span className="label">Timeout (s, 0 = never)</span>
            <input
              className="input mono"
              type="number" min={0}
              value={form.timeout ?? 0}
              onChange={(e) => set("timeout", Number(e.currentTarget.value))}
            />
          </label>
          <label className="form-check">
            <input
              type="checkbox"
              checked={form.block_network ?? false}
              onChange={(e) => set("block_network", e.currentTarget.checked)}
            />
            Block network (no TAP)
          </label>
          {error && <p className="mono" style={{ color: "var(--err)", fontSize: "var(--fs-sm)" }}>{error}</p>}
        </div>
        <div className="modal__foot">
          <button className="btn btn--ghost" onClick={onClose} disabled={busy}>Cancel</button>
          <button className="btn btn--primary" onClick={submit} disabled={busy || !form.image}>
            {busy ? <span className="spinner" /> : "Create"}
          </button>
        </div>
      </div>
    </div>
  );
}
