import { useCallback, useEffect, useState } from "react";
import { api, type FsEntry } from "../api.ts";
import { fmtBytes, fmtTime } from "../hooks.ts";

const ROOT = "/";

export function FilesPanel({
  sandboxId,
  notify,
}: {
  sandboxId: string;
  notify: (msg: string, kind?: "info" | "err") => void;
}): React.ReactElement {
  const [cwd, setCwd] = useState(ROOT);
  const [entries, setEntries] = useState<FsEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [preview, setPreview] = useState<{ path: string; text: string; binary: boolean } | null>(null);

  const load = useCallback(async (dir: string) => {
    setLoading(true);
    setError(null);
    setPreview(null);
    try {
      const list = await api.fsList(sandboxId, dir);
      list.sort((a, b) => (a.type === b.type ? a.name.localeCompare(b.name) : a.type === "dir" ? -1 : 1));
      setEntries(list);
      setCwd(dir);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setEntries([]);
    } finally {
      setLoading(false);
    }
  }, [sandboxId]);

  useEffect(() => { void load(ROOT); }, [load]);

  function joinPath(base: string, name: string): string {
    if (base === ROOT) return ROOT + name;
    return `${base}/${name}`;
  }
  function parentOf(p: string): string {
    if (p === ROOT) return ROOT;
    const idx = p.slice(0, -1).lastIndexOf("/");
    return idx <= 0 ? ROOT : p.slice(0, idx);
  }

  async function openEntry(e: FsEntry): Promise<void> {
    const p = joinPath(cwd, e.name);
    if (e.type === "dir") { void load(p); return; }
    if (e.type === "symlink") {
      // resolve via stat: if it's a dir, descend; else read.
      try {
        const st = await api.fsStat(sandboxId, p);
        if (st.type === "dir") { void load(p); return; }
      } catch { /* fall through to read */ }
    }
    try {
      const blob = await api.readFile(sandboxId, p);
      const isText = blob.size < 512 * 1024 && looksText(await blob.slice(0, 4096).text());
      setPreview({ path: p, text: isText ? await blob.text() : "", binary: !isText });
    } catch (err) {
      notify(err instanceof Error ? err.message : String(err), "err");
    }
  }

  async function delEntry(e: FsEntry, recursive: boolean): Promise<void> {
    const p = joinPath(cwd, e.name);
    if (!confirm(`Delete ${p}${recursive ? " (recursive)" : ""}?`)) return;
    try {
      await api.deleteFile(sandboxId, p, recursive);
      notify(`deleted ${p}`);
      void load(cwd);
    } catch (err) {
      notify(err instanceof Error ? err.message : String(err), "err");
    }
  }

  async function onUpload(files: FileList | null): Promise<void> {
    if (!files || files.length === 0) return;
    for (const f of Array.from(files)) {
      const dest = joinPath(cwd, f.name);
      try {
        await api.writeFile(sandboxId, dest, f);
        notify(`uploaded ${dest} (${fmtBytes(f.size)})`);
      } catch (err) {
        notify(err instanceof Error ? err.message : String(err), "err");
      }
    }
    void load(cwd);
  }

  return (
    <div className="panel">
      <div className="fs-toolbar">
        <button className="btn btn--sm" onClick={() => void load(parentOf(cwd))} disabled={cwd === ROOT}>↑ Up</button>
        <span className="fs-path muted">{cwd}</span>
        <label className="btn btn--sm" style={{ cursor: "pointer" }}>
          ↑ Upload
          <input type="file" multiple hidden onChange={(e) => void onUpload(e.currentTarget.files)} />
        </label>
        <button className="btn btn--sm" onClick={() => void load(cwd)}>↻</button>
      </div>

      {error && <p className="mono" style={{ color: "var(--err)", fontSize: "var(--fs-sm)" }}>{error}</p>}

      <table className="fs-table">
        <thead>
          <tr><th>Name</th><th>Type</th><th>Size</th><th>Modified</th><th></th></tr>
        </thead>
        <tbody>
          {loading ? (
            <tr><td colSpan={5} className="muted">loading…</td></tr>
          ) : entries.length === 0 ? (
            <tr><td colSpan={5} className="faint">empty directory</td></tr>
          ) : entries.map((e) => (
            <tr key={e.name} className="fs-row" onClick={() => void openEntry(e)}>
              <td className={`fs-row__name${e.type === "dir" ? " fs-row__name--dir" : ""}`}>
                {e.type === "dir" ? "▸ " : e.type === "symlink" ? "↪ " : ""}{e.name}
              </td>
              <td className="muted">{e.type}</td>
              <td className="mono muted">{e.type === "dir" ? "—" : fmtBytes(e.size)}</td>
              <td className="mono muted">{fmtTime(e.mtime)}</td>
              <td>
                <button
                  className="btn btn--ghost btn--sm fs-row__del"
                  onClick={(ev) => { ev.stopPropagation(); void delEntry(e, e.type === "dir"); }}
                  aria-label={`delete ${e.name}`}
                >✕</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {preview && (
        <div className="card" style={{ marginTop: "var(--gap)" }}>
          <div className="modal__head">
            <h3 className="mono" style={{ fontSize: "var(--fs-sm)" }}>{preview.path}</h3>
            <button className="btn btn--ghost btn--sm" onClick={() => setPreview(null)}>✕</button>
          </div>
          {preview.binary ? (
            <p className="muted" style={{ padding: "var(--pad)" }}>Binary file — download not supported in this view.</p>
          ) : (
            <pre className="mono" style={{ margin: 0, padding: "var(--pad)", maxHeight: 360, overflow: "auto", whiteSpace: "pre-wrap", wordBreak: "break-all" }}>
              {preview.text}
            </pre>
          )}
        </div>
      )}
    </div>
  );
}

// cheap heuristic: reject if it contains a NUL byte in the prefix.
function looksText(prefix: string): boolean {
  return !prefix.includes("\u0000");
}
