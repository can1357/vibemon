import { useEffect, useRef, useState } from "react";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import "@xterm/xterm/css/xterm.css";
import type { MessageInitShape } from "@bufbuild/protobuf";
import { ConnectError } from "@connectrpc/connect";
import { sandboxClient } from "../api.ts";
import type { ExecInputSchema } from "../gen/vmon/v1/api_pb.ts";
import { PushQueue } from "../grpc-ws.ts";

type XtermDisposable = { dispose: () => void };

// One interactive exec RPC (SandboxService.Exec bidi stream over the WS
// bridge). Client inputs flow through a push queue; aborting the controller
// tears the RPC (and the guest process) down.
type ExecSession = {
  queue: PushQueue<MessageInitShape<typeof ExecInputSchema>>;
  abort: AbortController;
};

// An interactive terminal over the exec stream. A fresh shell per connect;
// the session is torn down (process killed) when the component unmounts or
// the user clicks Disconnect.
export function TerminalPanel({ sandboxId }: { sandboxId: string }): React.ReactElement {
  const hostRef = useRef<HTMLDivElement | null>(null);
  const termRef = useRef<Terminal | null>(null);
  const fitRef = useRef<FitAddon | null>(null);
  const sessionRef = useRef<ExecSession | null>(null);
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
      closeSession(false);
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

  function closeSession(sendEof: boolean): void {
    const session = sessionRef.current;
    disposeInput();
    if (session) {
      if (sendEof) session.queue.push({ input: { case: "eof", value: {} } });
      session.queue.finish();
      session.abort.abort();
    }
    sessionRef.current = null;
  }

  function writeStdin(session: ExecSession, data: string): void {
    if (sessionRef.current === session) {
      session.queue.push({ input: { case: "stdin", value: new TextEncoder().encode(data) } });
    }
  }

  function sendResize(term: Terminal, session = sessionRef.current): void {
    if (session && sessionRef.current === session) {
      session.queue.push({
        input: { case: "resize", value: { rows: term.rows, cols: term.cols } },
      });
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
    closeSession(false);
    setConnected(false);
    setError(null);
    term.reset();
    term.writeln(`\x1b[36m[vmon] connecting to ${sandboxId} …\x1b[0m`);

    const session: ExecSession = {
      queue: new PushQueue(),
      abort: new AbortController(),
    };
    sessionRef.current = session;
    session.queue.push({
      input: { case: "start", value: { sandboxId, cmd: argv, tty: true } },
    });
    setConnected(true);
    term.writeln("\x1b[36m[vmon] connected\x1b[0m");
    term.focus();
    fitRef.current?.fit();
    sendResize(term, session);
    disposeInput();
    inputDisposableRef.current = term.onData((d) => writeStdin(session, d));

    void (async () => {
      try {
        for await (const out of sandboxClient.exec(session.queue, {
          signal: session.abort.signal,
        })) {
          if (sessionRef.current !== session || !mountedRef.current) break;
          switch (out.output.case) {
            case "chunk":
              term.write(out.output.value.data);
              break;
            case "exit":
              term.writeln(
                `\x1b[36m[vmon] process exited with code ${Number(out.output.value.code)}\x1b[0m`,
              );
              setConnected(false);
              break;
            default:
              break; // ready / unknown → ignore
          }
        }
      } catch (err) {
        if (sessionRef.current !== session || !mountedRef.current) return;
        const ce = ConnectError.from(err);
        const vmonCode = ce.metadata.get("vmon-code");
        const text = vmonCode ? `${vmonCode}: ${ce.rawMessage}` : ce.rawMessage;
        term.writeln(`\x1b[31m[vmon] ${text}\x1b[0m`);
        setError(text);
      } finally {
        if (sessionRef.current === session) {
          disposeInput();
          sessionRef.current = null;
          if (mountedRef.current) setConnected(false);
        }
      }
    })();
  }

  function disconnect(): void {
    closeSession(true);
    setConnected(false);
  }

  // tear down on unmount / sandbox switch
  useEffect(() => {
    closeSession(false);
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
