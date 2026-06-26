import { useEffect, useRef, useState } from "react";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import "@xterm/xterm/css/xterm.css";
import { execWsUrl } from "../api.ts";

type XtermDisposable = { dispose: () => void };

// An interactive terminal over the exec WS. A fresh shell per connect; the
// session is torn down (process killed) when the component unmounts or the
// user clicks Disconnect.
export function TerminalPanel({ sandboxId }: { sandboxId: string }): React.ReactElement {
  const hostRef = useRef<HTMLDivElement | null>(null);
  const termRef = useRef<Terminal | null>(null);
  const fitRef = useRef<FitAddon | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const inputDisposableRef = useRef<XtermDisposable | null>(null);
  const mountedRef = useRef(false);
  const [cmd, setCmd] = useState("/bin/sh");
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // one-time terminal setup
  useEffect(() => {
    mountedRef.current = true;
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
      mountedRef.current = false;
      closeSocket(false);
      window.removeEventListener("resize", onResize);
      fitRef.current = null;
      termRef.current = null;
      term.dispose();
    };
  }, []);

  function disposeInput(): void {
    inputDisposableRef.current?.dispose();
    inputDisposableRef.current = null;
  }

  function closeSocket(sendCloseStdin: boolean): void {
    const ws = wsRef.current;
    disposeInput();
    if (ws) {
      ws.onopen = null;
      ws.onmessage = null;
      ws.onerror = null;
      ws.onclose = null;
      if (sendCloseStdin && ws.readyState === WebSocket.OPEN) {
        try {
          ws.send(JSON.stringify({ close_stdin: true }));
        } catch {
          /* closing */
        }
      }
      if (ws.readyState === WebSocket.CONNECTING || ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
    }
    wsRef.current = null;
  }

  function writeStdin(ws: WebSocket, data: string): void {
    if (wsRef.current === ws && ws.readyState === WebSocket.OPEN)
      ws.send(JSON.stringify({ stdin: data }));
  }

  function sendResize(term: Terminal, ws = wsRef.current): void {
    if (ws && wsRef.current === ws && ws.readyState === WebSocket.OPEN) {
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
    closeSocket(false);
    setConnected(false);
    setError(null);
    term.reset();
    term.writeln(`\x1b[36m[vmon] connecting to ${sandboxId} …\x1b[0m`);

    const ws = new WebSocket(execWsUrl(sandboxId, argv));
    let reportedError = false;
    ws.binaryType = "arraybuffer";
    wsRef.current = ws;

    ws.onopen = () => {
      if (wsRef.current !== ws || !mountedRef.current) {
        ws.close();
        return;
      }
      setConnected(true);
      term.writeln("\x1b[36m[vmon] connected\x1b[0m");
      term.focus();
      fitRef.current?.fit();
      sendResize(term, ws);
      disposeInput();
      inputDisposableRef.current = term.onData((d) => writeStdin(ws, d));
    };
    ws.onmessage = (ev) => {
      if (wsRef.current !== ws || !mountedRef.current) return;
      if (ev.data instanceof ArrayBuffer) {
        term.write(new TextDecoder().decode(new Uint8Array(ev.data)));
        return;
      }
      let payload: Record<string, unknown>;
      try {
        payload = JSON.parse(ev.data as string);
      } catch {
        return;
      }
      if (typeof payload.stream === "string") term.write(String(payload.data ?? ""));
      else if (typeof payload.exit === "number") {
        term.writeln(`\x1b[36m[vmon] process exited with code ${payload.exit}\x1b[0m`);
        setConnected(false);
      } else if (typeof payload.error === "string") {
        reportedError = true;
        term.writeln(`\x1b[31m[vmon] ${payload.error}\x1b[0m`);
        setError(payload.error);
      }
    };
    ws.onerror = () => {
      if (wsRef.current !== ws || !mountedRef.current) return;
      reportedError = true;
      setError("websocket error");
      term.writeln("\x1b[31m[vmon] connection error\x1b[0m");
    };
    ws.onclose = (ev) => {
      if (wsRef.current !== ws) return;
      disposeInput();
      wsRef.current = null;
      if (!mountedRef.current) return;
      setConnected(false);
      if (!reportedError && ev.code !== 1000 && ev.code !== 1005) {
        setError(ev.reason || `websocket closed (${ev.code})`);
      }
    };
  }

  function disconnect(): void {
    closeSocket(true);
    setConnected(false);
  }

  // tear down on unmount / sandbox switch
  useEffect(() => {
    closeSocket(false);
    setConnected(false);
    setError(null);
  }, [sandboxId]);

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
          <button className="btn btn--danger" onClick={disconnect}>
            Disconnect
          </button>
        ) : (
          <button className="btn btn--primary" onClick={connect}>
            Connect
          </button>
        )}
        {error && (
          <span className="mono" style={{ color: "var(--err)", fontSize: "var(--fs-sm)" }}>
            {error}
          </span>
        )}
      </div>
    </div>
  );
}
