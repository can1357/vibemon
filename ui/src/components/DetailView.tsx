import { useState } from "react";
import { api, type SandboxView } from "../api.ts";
import { fmtBytes, fmtTime } from "../hooks.ts";
import { StatusBadge } from "./Sidebar.tsx";
import { TerminalPanel } from "./TerminalPanel.tsx";
import { FilesPanel } from "./FilesPanel.tsx";
import { MetricsPanel } from "./MetricsPanel.tsx";

type Tab = "terminal" | "files" | "metrics";

export function DetailView({
  sandbox,
  notify,
}: {
  sandbox: SandboxView;
  notify: (msg: string, kind?: "info" | "err") => void;
}): React.ReactElement {
  const [tab, setTab] = useState<Tab>("terminal");
  const [busy, setBusy] = useState(false);

  const running = sandbox.status === "running";

  async function act(label: string, fn: () => Promise<unknown>): Promise<void> {
    setBusy(true);
    try {
      await fn();
      notify(`${label} — done`);
    } catch (e) {
      notify(`${label} failed: ${e instanceof Error ? e.message : String(e)}`, "err");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="detail">
      <div className="detail__head">
        <div>
          <h1 className="detail__title">{sandbox.name}</h1>
          <div className="detail__sub">
            {sandbox.image ?? "no image"} · {sandbox.cpus} vCPU ·{" "}
            {fmtBytes(sandbox.memory * 1024 * 1024)} · {fmtBytes(sandbox.disk_mb * 1024 * 1024)}{" "}
            disk
          </div>
        </div>
        <div className="detail__actions">
          <StatusBadge status={sandbox.status} />
          <button
            className="btn btn--sm"
            disabled={!running || busy}
            onClick={() => {
              const name = prompt("Snapshot name", `${sandbox.name}-snap`);
              if (name) void act("snapshot", () => api.snapshotSandbox(sandbox.id, name));
            }}
          >
            Snapshot
          </button>
          <button
            className="btn btn--sm btn--danger"
            disabled={!running || busy}
            onClick={() => void act("terminate", () => api.terminateSandbox(sandbox.id))}
          >
            Terminate
          </button>
        </div>
      </div>

      <div className="kvgrid">
        <div className="kv">
          <div className="kv__k">id</div>
          <div className="kv__v" style={{ fontSize: "var(--fs-xs)" }}>
            {sandbox.id}
          </div>
        </div>
        <div className="kv">
          <div className="kv__k">created</div>
          <div className="kv__v">{fmtTime(sandbox.created_at)}</div>
        </div>
        <div className="kv">
          <div className="kv__k">last active</div>
          <div className="kv__v">{fmtTime(sandbox.last_active)}</div>
        </div>
        <div className="kv">
          <div className="kv__k">expires</div>
          <div className="kv__v">{fmtTime(sandbox.expires_at)}</div>
        </div>
        <div className="kv">
          <div className="kv__k">terminated</div>
          <div className="kv__v">{fmtTime(sandbox.terminated_at)}</div>
        </div>
        {sandbox.error && (
          <div className="kv">
            <div className="kv__k">error</div>
            <div className="kv__v" style={{ color: "var(--err)", fontSize: "var(--fs-xs)" }}>
              {sandbox.error}
            </div>
          </div>
        )}
      </div>

      <div className="tabs">
        <button
          className={`tab${tab === "terminal" ? " tab--active" : ""}`}
          onClick={() => setTab("terminal")}
        >
          Terminal
        </button>
        <button
          className={`tab${tab === "files" ? " tab--active" : ""}`}
          onClick={() => setTab("files")}
        >
          Files
        </button>
        <button
          className={`tab${tab === "metrics" ? " tab--active" : ""}`}
          onClick={() => setTab("metrics")}
        >
          Metrics
        </button>
      </div>

      {tab === "terminal" && <TerminalPanel sandboxId={sandbox.id} />}
      {tab === "files" && <FilesPanel sandboxId={sandbox.id} notify={notify} />}
      {tab === "metrics" && <MetricsPanel />}
    </div>
  );
}
