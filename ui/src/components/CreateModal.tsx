import { useEffect, useRef, useState } from "react";
import { api, type SandboxCreate } from "../api.ts";

const DEFAULTS: SandboxCreate = {
  image: "alpine",
  cpus: 1,
  memory: 512,
  disk_mb: 1024,
  timeout: 300,
  block_network: true,
};

function trimOrNull(value: string | null | undefined): string | null {
  const trimmed = value?.trim() ?? "";
  return trimmed ? trimmed : null;
}

function requiredInt(value: number | null | undefined, label: string, min: number, max?: number): number {
  if (typeof value !== "number" || !Number.isFinite(value) || !Number.isInteger(value)) {
    throw new Error(`${label} must be a whole number.`);
  }
  const n = Number(value);
  if (n < min || (max !== undefined && n > max)) {
    throw new Error(max === undefined ? `${label} must be at least ${min}.` : `${label} must be between ${min} and ${max}.`);
  }
  return n;
}

/** Build the REST create body from form state, rejecting invalid values. */
export function buildCreatePayload(form: SandboxCreate): SandboxCreate {
  const image = trimOrNull(form.image);
  if (!image && !trimOrNull(form.dockerfile)) throw new Error("Image is required.");

  const timeout = requiredInt(form.timeout ?? 0, "Timeout", 0);
  return {
    ...form,
    image,
    name: trimOrNull(form.name),
    cpus: requiredInt(form.cpus, "vCPUs", 1, 64),
    memory: requiredInt(form.memory, "Memory", 32),
    disk_mb: requiredInt(form.disk_mb, "Disk", 256),
    timeout: timeout === 0 ? null : timeout,
    timeout_secs: timeout,
    block_network: Boolean(form.block_network),
  };
}

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
  const mounted = useRef(true);

  useEffect(() => () => { mounted.current = false; }, []);

  const set = <K extends keyof SandboxCreate>(k: K, v: SandboxCreate[K]) => {
    setForm((f) => ({ ...f, [k]: v }));
    if (error) setError(null);
  };

  function requestClose(): void {
    if (!busy) onClose();
  }

  async function submit(): Promise<void> {
    if (busy) return;
    let payload: SandboxCreate;
    try {
      payload = buildCreatePayload(form);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      return;
    }

    setBusy(true);
    setError(null);
    try {
      const v = await api.createSandbox(payload);
      onCreated(v.name);
    } catch (e) {
      if (mounted.current) setError(e instanceof Error ? e.message : String(e));
    } finally {
      if (mounted.current) setBusy(false);
    }
  }

  return (
    <div className="overlay" onMouseDown={requestClose}>
      <div className="modal" onMouseDown={(e) => e.stopPropagation()}>
        <div className="modal__head">
          <h3>New sandbox</h3>
          <button className="btn btn--ghost btn--sm" onClick={requestClose} disabled={busy} aria-label="Close">✕</button>
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
          <button className="btn btn--ghost" onClick={requestClose} disabled={busy}>Cancel</button>
          <button className="btn btn--primary" onClick={submit} disabled={busy || !trimOrNull(form.image)}>
            {busy ? <span className="spinner" /> : "Create"}
          </button>
        </div>
      </div>
    </div>
  );
}
