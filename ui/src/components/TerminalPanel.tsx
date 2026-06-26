import { useEffect, useRef, useState } from "react";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import "@xterm/xterm/css/xterm.css";
import { execWsUrl } from "../api.ts";

// An interactive terminal over the exec WS. A fresh shell per connect; the
// session is torn down (process killed) when the component unmounts or the
// user clicks Disconnect.
export function TerminalPanel({ sandboxId }: { sandboxId: string }): React.ReactElement {
  const hostRef = useRef<HTMLDivElement | null>(null);
  const termRef = useRef<Terminal | null>(null);
  const fitRef = useRef<FitAddon | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const [cmd, setCmd] = useState("/bin/sh");
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // one-time terminal setup
  useEffect(() => {
    if (!hostRef.current) return;
    const term = new Terminal({
      fontFamily: "var(--font-mono)",
      fontSize: 13,
      lineHeight: 1.3,
      theme: {
        background: "#0d1117",
        foreground: "#c9d1d9",
        cursor: "#58a6ff",
        selectionBackground: "#264f78aa",
      },
      cursorBlink: true,
      convertEol: true,
    });
    const fit = new FitAddon();
    term.loadAddon(fit);
    term.open(hostRef.current);
    fit.fit();
    termRef.current = term;
    fitRef.current = fit;

    const onResize = () => {
      fit.fit();
      sendResize(term);
    };
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
      term.dispose();
      termRef.current = null;
    };
  }, []);

  function writeStdin(data: string): void {
    const ws = wsRef.current;
    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ stdin: data }));
  }

  function sendResize(term: Terminal): void {
    const ws = wsRef.current;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ resize: { rows: term.rows, cols: term.cols } }));
    }
  }

  function connect(): void {
    const term = termRef.current;
    if (!term) return;
    const argv = cmd.trim().split(/\s+/).filter(Boolean);
    if (argv.length === 0) {
      setError("command must not be empty");
      return;
    }
    setError(null);
    term.reset();
    term.writeln(`\x1b[36m[vmon] connecting to ${sandboxId} …\x1b[0m`);

    const ws = new WebSocket(execWsUrl(sandboxId, argv));
    ws.binaryType = "arraybuffer";
    wsRef.current = ws;

    ws.onopen = () => {
      setConnected(true);
      term.writeln("\x1b[36m[vmon] connected\x1b[0m");
      term.focus();
      fitRef.current?.fit();
      sendResize(term);
      // forward keystrokes to stdin
      const disp = term.onData((d) => writeStdin(d));
      ws.addEventListener("close", () => disp.dispose(), { once: true });
    };
    ws.onmessage = (ev) => {
      if (ev.data instanceof ArrayBuffer) {
        term.write(new TextDecoder().decode(new Uint8Array(ev.data)));
        return;
      }
      let payload: Record<string, unknown>;
      try { payload = JSON.parse(ev.data as string); } catch { return; }
      if (typeof payload.stream === "string") term.write(String(payload.data ?? ""));
      else if (typeof payload.exit === "number") {
        term.writeln(`\x1b[36m[vmon] process exited with code ${payload.exit}\x1b[0m`);
        setConnected(false);
      } else if (typeof payload.error === "string") {
        term.writeln(`\x1b[31m[vmon] ${payload.error}\x1b[0m`);
        setError(payload.error);
      }
    };
    ws.onerror = () => {
      setError("websocket error");
      term.writeln("\x1b[31m[vmon] connection error\x1b[0m");
    };
    ws.onclose = () => {
      setConnected(false);
      wsRef.current = null;
    };
  }

  function disconnect(): void {
    const ws = wsRef.current;
    if (ws) {
      try { ws.send(JSON.stringify({ close_stdin: true })); } catch { /* closing */ }
      ws.close();
    }
    wsRef.current = null;
    setConnected(false);
  }

  // tear down on unmount / sandbox switch
  useEffect(() => () => { wsRef.current?.close(); wsRef.current = null; }, [sandboxId]);

  return (
    <div className="term-wrap">
      <div className="term-host" ref={hostRef} />
      <div className="term-cmd">
        <input
          className="input mono"
          value={cmd}
          disabled={connected}
          onChange={(e) => setCmd(e.currentTarget.value)}
          placeholder="/bin/sh"
          aria-label="command to exec"
        />
        {connected ? (
          <button className="btn btn--danger" onClick={disconnect}>Disconnect</button>
        ) : (
          <button className="btn btn--primary" onClick={connect}>Connect</button>
        )}
        {error && <span className="mono" style={{ color: "var(--err)", fontSize: "var(--fs-sm)" }}>{error}</span>}
      </div>
    </div>
  );
}
