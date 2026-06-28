import { useEffect, useState } from "react";
import { api, type SandboxMetrics } from "../api.ts";

const numberFmt = new Intl.NumberFormat();

interface MetricGroup {
  name: string;
  rows: [string, number][];
}

// Split the per-sandbox metrics object into named groups of scalar counters:
// top-level scalars collapse into one "runtime" group, while nested objects
// (vm_exits, snapshot, pager, …) each become their own group in server order.
function groupMetrics(metrics: SandboxMetrics): MetricGroup[] {
  const groups: MetricGroup[] = [];
  const scalars: [string, number][] = [];
  for (const key in metrics) {
    const value = metrics[key];
    if (typeof value === "number") {
      scalars.push([key, value]);
    } else if (value && typeof value === "object") {
      const rows: [string, number][] = [];
      for (const field in value) rows.push([field, value[field]]);
      groups.push({ name: key, rows });
    }
  }
  if (scalars.length > 0) groups.unshift({ name: "runtime", rows: scalars });
  return groups;
}

// Live VMM runtime counters for the selected sandbox. The endpoint is
// running-only, so polling is gated on status to avoid a 5s error loop against
// a stopped/terminated VM.
export function MetricsPanel({
  sandboxId,
  running,
}: {
  sandboxId: string;
  running: boolean;
}): React.ReactElement {
  const [metrics, setMetrics] = useState<SandboxMetrics | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setMetrics(null);
    setError(null);
    if (!running) return;
    let stop = false;
    async function pull(): Promise<void> {
      try {
        const m = await api.sandboxMetrics(sandboxId);
        if (!stop) {
          setMetrics(m);
          setError(null);
        }
      } catch (e) {
        if (!stop) setError(e instanceof Error ? e.message : String(e));
      }
    }
    void pull();
    const t = setInterval(pull, 5000);
    return () => {
      stop = true;
      clearInterval(t);
    };
  }, [sandboxId, running]);

  if (!running)
    return (
      <p className="muted" style={{ padding: "var(--pad)" }} aria-live="polite">
        Metrics are available while the sandbox is running.
      </p>
    );
  if (error)
    return (
      <p className="mono" style={{ color: "var(--err)", padding: "var(--pad)" }} aria-live="polite">
        {error}
      </p>
    );
  if (!metrics)
    return (
      <p className="muted" style={{ padding: "var(--pad)" }}>
        Loading…
      </p>
    );

  const groups = groupMetrics(metrics);
  if (groups.length === 0)
    return (
      <p className="muted" style={{ padding: "var(--pad)" }}>
        No metrics reported.
      </p>
    );

  return (
    <div className="panel">
      {groups.map((group) => (
        <div key={group.name} className="card" style={{ marginBottom: "var(--gap)" }}>
          <div className="modal__head" style={{ padding: "var(--pad-sm) var(--pad)" }}>
            <h3 className="mono" style={{ fontSize: "var(--fs-sm)", margin: 0 }}>
              {group.name}
            </h3>
          </div>
          <table className="fs-table">
            <tbody>
              {group.rows.map(([key, value]) => (
                <tr key={key}>
                  <td className="mono muted" style={{ fontSize: "var(--fs-xs)" }}>
                    {key}
                  </td>
                  <td
                    className="mono"
                    style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}
                  >
                    {numberFmt.format(value)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  );
}
