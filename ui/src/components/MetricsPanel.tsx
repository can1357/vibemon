import { useEffect, useState } from "react";
import { api } from "../api.ts";

interface MetricLine {
  name: string;
  labels: Record<string, string>;
  value: number;
}

// Parse the Prometheus exposition format served at /metrics into rows.
function parseMetrics(text: string): { help: Record<string, string>; lines: MetricLine[] } {
  const help: Record<string, string> = {};
  const lines: MetricLine[] = [];
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (!line) continue;
    if (line.startsWith("# HELP")) {
      const [, , name, ...rest] = line.split(/\s+/);
      help[name] = rest.join(" ");
      continue;
    }
    if (line.startsWith("#")) continue;
    const m = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+(\S+)/);
    if (!m) continue;
    const labels: Record<string, string> = {};
    if (m[2]) {
      for (const pair of m[2].split(",")) {
        const eq = pair.indexOf("=");
        if (eq > 0) labels[pair.slice(0, eq).trim()] = pair.slice(eq + 2, -1);
      }
    }
    lines.push({ name: m[1], labels, value: Number(m[3]) });
  }
  return { help, lines };
}

export function MetricsPanel(): React.ReactElement {
  const [text, setText] = useState<string>("");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let stop = false;
    async function pull(): Promise<void> {
      try {
        const t = await api.metrics();
        if (!stop) {
          setText(t);
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
  }, []);

  if (error)
    return (
      <p className="mono" style={{ color: "var(--err)", padding: "var(--pad)" }}>
        {error}
      </p>
    );
  if (!text)
    return (
      <p className="muted" style={{ padding: "var(--pad)" }}>
        loading…
      </p>
    );

  const { help, lines } = parseMetrics(text);

  return (
    <div className="panel">
      {Object.keys(help).map((family) => {
        const rows = lines.filter((l) => l.name === family);
        if (rows.length === 0) return null;
        return (
          <div key={family} className="card" style={{ marginBottom: "var(--gap)" }}>
            <div className="modal__head" style={{ padding: "var(--pad-sm) var(--pad)" }}>
              <h3 className="mono" style={{ fontSize: "var(--fs-sm)", margin: 0 }}>
                {family}
              </h3>
              <span className="muted" style={{ fontSize: "var(--fs-xs)" }}>
                {help[family]}
              </span>
            </div>
            <table className="fs-table">
              <tbody>
                {rows.map((r, i) => (
                  <tr key={i}>
                    <td className="mono muted" style={{ fontSize: "var(--fs-xs)" }}>
                      {Object.entries(r.labels)
                        .map(([k, v]) => `${k}="${v}"`)
                        .join(" ") || "—"}
                    </td>
                    <td className="mono" style={{ textAlign: "right" }}>
                      {r.value}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        );
      })}
    </div>
  );
}
