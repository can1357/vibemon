import { fmtAgo } from "../hooks.ts";
import type { SandboxView } from "../api.ts";

export function Sidebar({
  sandboxes,
  selectedId,
  onSelect,
  onNew,
}: {
  sandboxes: SandboxView[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onNew: () => void;
}): React.ReactElement {
  return (
    <aside className="sidebar">
      <div className="sidebar__head">
        <h2>Sandboxes</h2>
        <button className="btn btn--primary btn--sm" onClick={onNew}>
          + New
        </button>
      </div>
      <div className="sidebar__list">
        {sandboxes.length === 0 ? (
          <p className="sidebar__empty">No sandboxes yet. Create one to get started.</p>
        ) : (
          sandboxes.map((s) => (
            <div
              key={s.id}
              className={`vm-item${s.id === selectedId ? " vm-item--active" : ""}`}
              onClick={() => onSelect(s.id)}
              role="button"
              tabIndex={0}
              onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && onSelect(s.id)}
            >
              <span className="vm-item__name">{s.name}</span>
              <span className="vm-item__meta">
                <StatusBadge status={s.status} />
                <span>{fmtAgo(s.created_at)}</span>
              </span>
            </div>
          ))
        )}
      </div>
    </aside>
  );
}

export function StatusBadge({ status }: { status: SandboxView["status"] }): React.ReactElement {
  const cls = `badge badge--${status}`;
  return <span className={cls}>{status}</span>;
}
