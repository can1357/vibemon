import { useMemo, useState } from "react";
import { useSandboxes, useToasts, type Toast } from "./hooks.ts";
import { TopBar } from "./components/TopBar.tsx";
import { Sidebar } from "./components/Sidebar.tsx";
import { CreateModal } from "./components/CreateModal.tsx";
import { DetailView } from "./components/DetailView.tsx";

export function App(): React.ReactElement {
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const { sandboxes, loading, error, authError, refresh } = useSandboxes(selectedId);
  const { toasts, push, dismiss } = useToasts();

  const selected = useMemo(
    () => sandboxes.find((s) => s.id === selectedId) ?? null,
    [sandboxes, selectedId],
  );

  return (
    <div className="app">
      <TopBar />
      <div className="body">
        <Sidebar
          sandboxes={sandboxes}
          selectedId={selectedId}
          onSelect={setSelectedId}
          onNew={() => setShowCreate(true)}
        />
        <main className="main">
          {error && !loading ? (
            authError ? (
              <div className="main__empty">
                <div style={{ textAlign: "center", maxWidth: 360 }}>
                  <p className="muted" style={{ marginBottom: "var(--pad-sm)" }}>Authentication required.</p>
                  <p className="faint" style={{ fontSize: "var(--fs-sm)", marginBottom: "var(--pad)" }}>
                    Enter the <code className="mono">vmon serve</code> bearer token in the top-right token field.
                    Set it with <code className="mono">--token</code> or <code className="mono">VMON_API_TOKEN</code>.
                  </p>
                </div>
              </div>
            ) : (
              <div className="main__empty">
                <div style={{ textAlign: "center" }}>
                  <p className="mono" style={{ color: "var(--err)" }}>{error}</p>
                  <button className="btn" onClick={() => void refresh()} style={{ marginTop: "var(--pad)" }}>Retry</button>
                </div>
              </div>
            )
          ) : !selected ? (
            <div className="main__empty">
              {loading ? <span className="spinner" /> : <span className="faint">Select a sandbox, or create a new one.</span>}
            </div>
          ) : (
            <DetailView sandbox={selected} notify={push} />
          )}
        </main>
      </div>

      {showCreate && (
        <CreateModal
          onClose={() => setShowCreate(false)}
          onCreated={(name) => {
            setShowCreate(false);
            void refresh();
            push(`sandbox ${name} created`);
          }}
        />
      )}

      <div className="toast-host">
        {toasts.map((t: Toast) => (
          <div
            key={t.id}
            className={`toast${t.kind === "err" ? " toast--err" : ""}`}
            onClick={() => dismiss(t.id)}
            role="alert"
          >{t.message}</div>
        ))}
      </div>
    </div>
  );
}
