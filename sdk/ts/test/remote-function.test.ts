import { afterAll, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  create,
  type DescMessage,
  fromBinary,
  type MessageInitShape,
  type MessageShape,
  toBinary,
} from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import type { Subprocess } from "bun";
import type { Client } from "../src";
import {
  APIError,
  connect,
  DEFAULT_REMOTE_FUNCTION_IMAGE,
  FunctionCall,
  MISSING_NODE_HINT,
  RemoteFunctionError,
  Retries,
  SESSION_RUNNER_PATH,
} from "../src";
import {
  CreateSandboxRequestSchema,
  ExecInputSchema,
  ExecOutputSchema,
  type ExecStart,
  FileDeleteRequestSchema,
  FileWriteRequestSchema,
  JsonViewSchema,
  OkSchema,
  SandboxRefSchema,
  Stream,
} from "../src/gen/vmon/v1/api_pb";
import { BridgeFrameSchema } from "../src/gen/vmon/v1/bridge_pb";

const nodeBin = Bun.which("node");
if (nodeBin === null) {
  throw new Error("local Node.js is required to bridge fake exec sessions");
}

const temporaryDirectories: string[] = [];
const fakeServers: Bun.Server<BridgeWsState>[] = [];

afterAll(async () => {
  for (const server of fakeServers.splice(0)) server.stop(true);
  await Promise.allSettled(
    temporaryDirectories.map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

async function scratchDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "vmon-ts-remote-"));
  temporaryDirectories.push(directory);
  return directory;
}

interface RecordedExec {
  sandboxId: string;
  cmd: string[];
  env: Record<string, string> | undefined;
}

interface FakeApiFailure {
  status: number;
  code: string;
  message: string;
}

/** One live fake exec session backed by a local `node <runner>` subprocess. */
interface ExecSession {
  sandboxId: string;
  kill(): void;
}

type ExecOutputInit = MessageInitShape<typeof ExecOutputSchema>;

function chunkOut(name: "stdout" | "stderr", data: Uint8Array): ExecOutputInit {
  return {
    output: {
      case: "chunk",
      value: { stream: name === "stderr" ? Stream.STDERR : Stream.STDOUT, data },
    },
  };
}
function exitOut(code: number): ExecOutputInit {
  return { output: { case: "exit", value: { code: BigInt(code) } } };
}

/** Server side of one bridged RPC socket. */
interface BridgeConn {
  message(payload: Uint8Array): void;
  end(status?: number, text?: string, trailers?: Record<string, string>): void;
  close(): void;
}
interface RpcHandler {
  onMessage(payload: Uint8Array): void;
  onHalfClose?(): void;
  onClose?(): void;
}
interface BridgeWsState {
  handler: RpcHandler | null;
}

function bridgeFrameBytes(frame: MessageInitShape<typeof BridgeFrameSchema>["frame"]): Uint8Array {
  return toBinary(BridgeFrameSchema, create(BridgeFrameSchema, { frame }));
}

/** A unary RPC over the bridge: one request message in, one response + end out. */
function unaryHandler<I extends DescMessage, O extends DescMessage>(
  conn: BridgeConn,
  input: I,
  output: O,
  impl: (request: MessageShape<I>) => MessageInitShape<O>,
): RpcHandler {
  return {
    onMessage: (payload) => {
      try {
        const response = impl(fromBinary(input, payload));
        conn.message(toBinary(output, create(output, response)));
        conn.end();
      } catch (error) {
        const failure = ConnectError.from(error);
        const trailers: Record<string, string> = {};
        failure.metadata.forEach((value, key) => {
          trailers[key] = value;
        });
        conn.end(failure.code, failure.rawMessage, trailers);
      }
    },
  };
}

class FakeVmonServer {
  readonly createBodies: Record<string, unknown>[] = [];
  readonly execs: RecordedExec[] = [];
  readonly terminatedSandboxIds: string[] = [];
  createFailure: FakeApiFailure | null = null;
  nodeMissing = false;
  readonly #files = new Map<string, Map<string, string>>();
  readonly #statuses = new Map<string, string>();
  readonly #sessions = new Map<string, Set<ExecSession>>();
  #nextSandbox = 1;

  readonly #server: Bun.Server<BridgeWsState>;

  constructor() {
    this.#server = Bun.serve<BridgeWsState>({
      port: 0,
      fetch(request, server) {
        const url = new URL(request.url);
        if (url.pathname !== "/grpc") return new Response("not found", { status: 404 });
        return server.upgrade(request, { data: { handler: null } })
          ? undefined
          : new Response("upgrade required", { status: 426 });
      },
      websocket: {
        open() {},
        message: (ws, message) => this.#onFrame(ws, message),
        close: (ws) => ws.data.handler?.onClose?.(),
      },
    });
    fakeServers.push(this.#server);
  }

  /** Base URL of the fake vmond node. */
  get url(): string {
    return `http://127.0.0.1:${this.#server.port}`;
  }

  #onFrame(ws: Bun.ServerWebSocket<BridgeWsState>, message: string | Buffer): void {
    const conn: BridgeConn = {
      message: (payload) => void ws.send(bridgeFrameBytes({ case: "message", value: payload })),
      end: (status = 0, text = "", trailers = {}) => {
        ws.send(bridgeFrameBytes({ case: "end", value: { status, message: text, trailers } }));
        ws.close();
      },
      close: () => ws.close(),
    };
    if (typeof message === "string") {
      conn.end(Code.Internal, "expected binary bridge frame");
      return;
    }
    const frame = fromBinary(BridgeFrameSchema, new Uint8Array(message)).frame;
    switch (frame.case) {
      case "call": {
        if (frame.value.metadata.authorization !== "Bearer test-token") {
          conn.end(Code.Unauthenticated, "bad token", { "vmon-code": "unauthorized" });
          return;
        }
        ws.data.handler = this.#route(frame.value.method, conn);
        if (ws.data.handler === null)
          conn.end(Code.Unimplemented, `unknown method ${frame.value.method}`, {
            "vmon-code": "unsupported",
          });
        return;
      }
      case "message":
        ws.data.handler?.onMessage(frame.value);
        return;
      case "halfClose":
        ws.data.handler?.onHalfClose?.();
        return;
      default:
        conn.end(Code.Internal, "unexpected bridge frame");
    }
  }

  #route(method: string, conn: BridgeConn): RpcHandler | null {
    switch (method) {
      case "/vmon.v1.SandboxService/Create":
        return unaryHandler(conn, CreateSandboxRequestSchema, JsonViewSchema, (req) => {
          const createBody: unknown = JSON.parse(req.specJson);
          if (!isRecord(createBody))
            throw new ConnectError("bad body", Code.InvalidArgument, { "vmon-code": "invalid" });
          this.createBodies.push(createBody);
          if (this.createFailure !== null) {
            throw new ConnectError(
              this.createFailure.message,
              STATUS_GRPC_CODE[this.createFailure.status] ?? Code.Unknown,
              { "vmon-code": this.createFailure.code },
            );
          }
          const id = `sb-${this.#nextSandbox}`;
          this.#nextSandbox += 1;
          this.#files.set(id, new Map());
          this.#statuses.set(id, "running");
          return { json: JSON.stringify({ id, name: id }) };
        });
      case "/vmon.v1.SandboxService/Get":
        return unaryHandler(conn, SandboxRefSchema, JsonViewSchema, (req) => {
          const status = this.#statuses.get(req.id);
          if (status === undefined) {
            throw new ConnectError(`unknown sandbox ${req.id}`, Code.NotFound, {
              "vmon-code": "not_found",
            });
          }
          return { json: JSON.stringify({ id: req.id, name: req.id, status }) };
        });
      case "/vmon.v1.SandboxService/FileWrite":
        return unaryHandler(conn, FileWriteRequestSchema, OkSchema, (req) => {
          const files = this.#files.get(req.id);
          if (files === undefined) {
            throw new ConnectError("missing sandbox or path", Code.NotFound, {
              "vmon-code": "not_found",
            });
          }
          files.set(req.path, new TextDecoder().decode(req.data));
          return {};
        });
      case "/vmon.v1.SandboxService/FileDelete":
        return unaryHandler(conn, FileDeleteRequestSchema, OkSchema, (req) => {
          this.#files.get(req.id)?.delete(req.path);
          return {};
        });
      case "/vmon.v1.SandboxService/Terminate":
        return unaryHandler(conn, SandboxRefSchema, JsonViewSchema, (req) => {
          this.terminatedSandboxIds.push(req.id);
          if (!this.#statuses.has(req.id)) {
            throw new ConnectError(`unknown sandbox ${req.id}`, Code.NotFound, {
              "vmon-code": "not_found",
            });
          }
          this.#statuses.set(req.id, "terminated");
          this.#killSessions(req.id);
          return { json: "{}" };
        });
      case "/vmon.v1.SandboxService/Exec":
        return this.#execHandler(conn);
      default:
        return null;
    }
  }

  /**
   * Bridges one Exec bidi stream to a real local `node <runner>` subprocess,
   * mirroring vmond: ExecInput (start, stdin, eof) in, ExecOutput (chunk,
   * exit) out, and a killed guest process when the socket goes away.
   */
  #execHandler(conn: BridgeConn): RpcHandler {
    let child: Subprocess<"pipe", "pipe", "pipe"> | null = null;
    let session: ExecSession | null = null;
    let started = false;
    let killed = false;
    const pendingStdin: (Uint8Array | "eof")[] = [];
    const deliverStdin = (
      target: Subprocess<"pipe", "pipe", "pipe">,
      data: Uint8Array | "eof",
    ): void => {
      if (data === "eof") {
        target.stdin.end();
        return;
      }
      target.stdin.write(data);
      target.stdin.flush();
    };
    const sendOutput = (output: ExecOutputInit): void =>
      conn.message(toBinary(ExecOutputSchema, create(ExecOutputSchema, output)));
    const run = async (start: ExecStart): Promise<void> => {
      const env = Object.keys(start.env).length > 0 ? { ...start.env } : undefined;
      this.execs.push({ sandboxId: start.sandboxId, cmd: [...start.cmd], env });
      if (this.nodeMissing) {
        sendOutput(chunkOut("stderr", new TextEncoder().encode("exec: node: not found\n")));
        sendOutput(exitOut(127));
        conn.end();
        return;
      }
      const source = this.fileFor(start.sandboxId, SESSION_RUNNER_PATH);
      if (start.cmd[0] !== "node" || start.cmd[1] !== SESSION_RUNNER_PATH || source === undefined) {
        sendOutput(exitOut(127));
        conn.end();
        return;
      }
      const directory = await scratchDirectory();
      const runnerPath = join(directory, "runner.js");
      await writeFile(runnerPath, source);
      const spawned = Bun.spawn([nodeBin as string, runnerPath], {
        stdin: "pipe",
        stdout: "pipe",
        stderr: "pipe",
        env: { ...process.env, ...(env ?? {}) },
      });
      child = spawned;
      const live: ExecSession = {
        sandboxId: start.sandboxId,
        kill: () => {
          this.#releaseSession(live);
          killed = true;
          spawned.kill();
          // Close without an end frame: the client surfaces a transport
          // failure, matching a vmond node dying mid-session.
          conn.close();
        },
      };
      session = live;
      this.#registerSession(live);
      for (const queued of pendingStdin.splice(0)) deliverStdin(spawned, queued);
      const pump = async (
        stream: ReadableStream<Uint8Array>,
        name: "stdout" | "stderr",
      ): Promise<void> => {
        const reader = stream.getReader();
        try {
          while (true) {
            const { value, done } = await reader.read();
            if (done) return;
            sendOutput(chunkOut(name, value));
          }
        } finally {
          reader.releaseLock();
        }
      };
      const stdoutDone = pump(spawned.stdout, "stdout");
      const stderrDone = pump(spawned.stderr, "stderr");
      const code = await spawned.exited;
      await stdoutDone;
      await stderrDone;
      this.#releaseSession(live);
      if (!killed) {
        sendOutput(exitOut(code));
        conn.end();
      }
    };
    return {
      onMessage: (payload) => {
        const input = fromBinary(ExecInputSchema, payload).input;
        if (!started) {
          started = true;
          if (input.case !== "start") {
            conn.end(Code.InvalidArgument, "expected start frame", { "vmon-code": "invalid" });
            return;
          }
          void run(input.value);
          return;
        }
        if (input.case === "stdin") {
          if (child) deliverStdin(child, input.value);
          else pendingStdin.push(input.value);
        }
        if (input.case === "eof") {
          if (child) deliverStdin(child, "eof");
          else pendingStdin.push("eof");
        }
      },
      /** Client went away: SIGTERM the guest process, mirroring vmond. */
      onClose: () => {
        session?.kill();
      },
    };
  }

  fileFor(sandboxId: string, path: string): string | undefined {
    return this.#files.get(sandboxId)?.get(path);
  }

  #registerSession(session: ExecSession): void {
    const sessions = this.#sessions.get(session.sandboxId) ?? new Set();
    sessions.add(session);
    this.#sessions.set(session.sandboxId, sessions);
  }

  #releaseSession(session: ExecSession): void {
    this.#sessions.get(session.sandboxId)?.delete(session);
  }

  /** Simulate a sandbox dying out from under the SDK. */
  killSandbox(sandboxId: string): void {
    this.#statuses.set(sandboxId, "terminated");
    this.#killSessions(sandboxId);
  }

  liveSocketCount(): number {
    let count = 0;
    for (const sessions of this.#sessions.values()) count += sessions.size;
    return count;
  }

  #killSessions(sandboxId: string): void {
    for (const session of [...(this.#sessions.get(sandboxId) ?? [])]) session.kill();
  }
}

function clientFor(server: FakeVmonServer): Client {
  return connect(server.url, { token: "test-token", discover: false });
}

async function collect<T>(iterator: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const value of iterator) values.push(value);
  return values;
}

function waitForFileSource(): string {
  // This body is serialized and runs in a separate guest process, so the
  // import target only resolves at runtime there; a static import in this
  // test module cannot serve the shipped source.
  return `
export async function waitForFile(path, value) {
  const fs = await import("node:fs/promises");
  while (true) {
    try {
      await fs.access(path);
      break;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }
  return value;
}
`;
}

function counterSource(): string {
  // Serialized guest-side body: the module graph exists only in the runner
  // process, so the import must be dynamic there.
  return `
export async function flaky(counterPath, succeedAt) {
  const fs = await import("node:fs/promises");
  let count = 0;
  try {
    count = Number.parseInt(await fs.readFile(counterPath, "utf8"), 10) || 0;
  } catch {}
  count += 1;
  await fs.writeFile(counterPath, String(count));
  if (count < succeedAt) throw new RangeError("attempt " + count + " failed");
  return count;
}
`;
}

test("remote reuses one warm sandbox and session across calls", async () => {
  const server = new FakeVmonServer();
  const client = clientFor(server);
  const stdout: string[] = [];
  const add = client.remoteFunction(
    function add(left: number, right: number) {
      console.log("adding", left, right);
      return { sum: left + right };
    },
    { onStdout: (output) => stdout.push(output) },
  );

  expect(server.createBodies).toHaveLength(0);
  await expect(add.remote(2, 5)).resolves.toEqual({ sum: 7 });
  await expect(add.remote(10, 4)).resolves.toEqual({ sum: 14 });

  expect(server.createBodies).toHaveLength(1);
  expect(server.createBodies[0]?.image).toBe(DEFAULT_REMOTE_FUNCTION_IMAGE);
  expect(server.execs).toHaveLength(1);
  expect(server.execs[0]?.cmd).toEqual(["node", SESSION_RUNNER_PATH]);
  expect(server.execs[0]?.env?.VMON_REMOTE).toBe("1");
  expect(stdout).toEqual(["adding 2 5\n", "adding 10 4\n"]);

  await Promise.all([add.terminate(), add.terminate()]);
  await add.terminate();
  expect(server.terminatedSandboxIds).toEqual(["sb-1"]);
  expect(server.liveSocketCount()).toBe(0);
});

test("remote transparently replaces dead sandboxes and sessions", async () => {
  const server = new FakeVmonServer();
  const increment = clientFor(server).remoteFunction((value: number) => value + 1);

  await expect(increment.remote(1)).resolves.toBe(2);
  server.killSandbox("sb-1");
  await expect(increment.remote(2)).resolves.toBe(3);
  await increment.terminate();

  expect(server.createBodies).toHaveLength(2);
  expect(server.execs).toHaveLength(2);
  expect(server.terminatedSandboxIds).toContain("sb-2");
});

test("options forward pool_size, validate ranges, and compute backoff delays", async () => {
  const server = new FakeVmonServer();
  const client = clientFor(server);
  const pooled = client.remoteFunction((value: number) => value, { pool: 3 });
  await expect(pooled.remote(1)).resolves.toBe(1);
  expect(server.createBodies[0]?.pool_size).toBe(3);
  await pooled.terminate();

  expect(() => client.remoteFunction((value: number) => value, { pool: -1 })).toThrow(RangeError);
  expect(() => client.remoteFunction((value: number) => value, { timeout: 0 })).toThrow(RangeError);
  expect(() => client.remoteFunction((value: number) => value, { retries: -1 })).toThrow(
    "maxRetries must be an integer >= 0",
  );
  expect(() => new Retries({ maxRetries: 2, backoffCoefficient: 0.5 })).toThrow(RangeError);
  expect(() => new Retries({ maxRetries: 2, maxDelay: 0.5 })).toThrow(RangeError);

  const retries = new Retries({
    maxRetries: 5,
    initialDelay: 1,
    backoffCoefficient: 2,
    maxDelay: 5,
  });
  expect(retries.delayFor(1)).toBe(1);
  expect(retries.delayFor(2)).toBe(2);
  expect(retries.delayFor(3)).toBe(4);
  expect(retries.delayFor(4)).toBe(5);
});

test("arguments are validated before any sandbox exists", async () => {
  const server = new FakeVmonServer();
  const echo = clientFor(server).remoteFunction(function echo(value: unknown) {
    return value;
  });
  const cyclic: Record<string, unknown> = {};
  cyclic.self = cyclic;

  await expect(echo.remote(undefined)).rejects.toThrow(
    "remote function arguments[0] contains a non-JSON value",
  );
  await expect(echo.remote(Number.NaN)).rejects.toThrow(
    "remote function arguments[0] contains a non-finite number",
  );
  await expect(echo.remote(cyclic)).rejects.toThrow(
    "remote function arguments[0].self contains a cycle",
  );
  expect(server.createBodies).toHaveLength(0);

  expect(() => clientFor(server).remoteFunction(Math.max)).toThrow(
    "native functions cannot be serialized",
  );
  const bound = function add(left: number, right: number): number {
    return left + right;
  }.bind(null, 1);
  expect(() => clientFor(server).remoteFunction(bound)).toThrow(
    "bound functions cannot be serialized",
  );
});

test("builtin guest errors are reconstructed with the remote stack attached", async () => {
  const server = new FakeVmonServer();
  const explode = clientFor(server).remoteFunction(function explode(value: number): never {
    throw new RangeError(`bad value ${value}`);
  });

  try {
    await explode.remote(7);
    throw new Error("expected the remote function to fail");
  } catch (error) {
    expect(error).toBeInstanceOf(RangeError);
    expect(error).not.toBeInstanceOf(RemoteFunctionError);
    const failure = error as RangeError & { remoteStack?: string };
    expect(failure.message).toBe("bad value 7");
    expect(failure.remoteStack).toContain("bad value 7");
    expect(failure.stack).toContain("[vmon remote stack]");
  }

  const custom = clientFor(server).remoteFunction(function custom(): never {
    class CustomBoom extends Error {
      constructor(message: string) {
        super(message);
        this.name = "CustomBoom";
      }
    }
    throw new CustomBoom("special failure");
  });
  try {
    await custom.remote();
    throw new Error("expected the remote function to fail");
  } catch (error) {
    expect(error).toBeInstanceOf(RemoteFunctionError);
    const failure = error as RemoteFunctionError;
    expect(failure.remoteType).toBe("CustomBoom");
    expect(failure.message).toBe("special failure");
    expect(failure.code).toBe("remote");
    expect(failure.remoteStack).toContain("CustomBoom");
  }

  await explode.terminate();
  await custom.terminate();
});

test("remoteGen streams yields; early break kills only that session", async () => {
  const server = new FakeVmonServer();
  const client = clientFor(server);
  const squares = client.remoteFunction(function* squares(limit: number) {
    for (let index = 0; index < limit; index += 1) yield index * index;
  });

  await expect(squares.remote(3)).rejects.toThrow(
    "a generator function cannot be called with .remote(); use .remoteGen()",
  );

  await expect(collect(squares.remoteGen(4))).resolves.toEqual([0, 1, 4, 9]);
  await expect(collect(squares.remoteGen(2))).resolves.toEqual([0, 1]);
  expect(server.execs).toHaveLength(1);

  const partial: number[] = [];
  for await (const value of squares.remoteGen(1000)) {
    partial.push(value);
    if (partial.length === 2) break;
  }
  expect(partial).toEqual([0, 1]);
  // The abandoned stream killed its session; the next call gets a fresh one.
  await expect(collect(squares.remoteGen(2))).resolves.toEqual([0, 1]);
  expect(server.execs).toHaveLength(2);

  const scalar = client.remoteFunction((value: number) => value);
  await expect(collect(scalar.remoteGen(1))).rejects.toThrow(
    "a non-generator function cannot be called with .remoteGen(); use .remote()",
  );

  const fromSource = client.remoteFunctionFromSource<[count: number], number>({
    source: "export function* ticks(count) { for (let i = 0; i < count; i += 1) yield i; }",
    exportName: "ticks",
  });
  await expect(collect(fromSource.remoteGen(3))).resolves.toEqual([0, 1, 2]);

  await squares.terminate();
  await fromSource.terminate();
});

test("map is lazy, ordered by default, and reuses at most `concurrency` workers", async () => {
  const server = new FakeVmonServer();
  const directory = await scratchDirectory();
  const gate = join(directory, "map-gate");
  const waiter = clientFor(server).remoteFunctionFromSource<[path: string, value: number], number>({
    source: waitForFileSource(),
    exportName: "waitForFile",
  });
  const pulled: number[] = [];
  function* inputs(): Generator<[path: string, value: number]> {
    for (let index = 0; index < 6; index += 1) {
      pulled.push(index);
      yield [gate, index];
    }
  }

  const stream = waiter.starmap(inputs(), { concurrency: 2 });
  const iterator = stream[Symbol.asyncIterator]();
  const firstResult = iterator.next();
  // Both workers are blocked on the gate holding one item each; the input
  // iterator must not be drained beyond what workers claimed.
  await pollUntil(() => pulled.length >= 2);
  await Bun.sleep(50);
  expect(pulled.length).toBe(2);

  await writeFile(gate, "go");
  const results: number[] = [(await firstResult).value as number];
  for await (const value of { [Symbol.asyncIterator]: () => iterator }) results.push(value);
  expect(results).toEqual([0, 1, 2, 3, 4, 5]);
  expect(server.createBodies).toHaveLength(2);
  expect([...server.terminatedSandboxIds].sort()).toEqual(["sb-1", "sb-2"]);
  expect(server.liveSocketCount()).toBe(0);

  const empty = await collect(waiter.starmap([]));
  expect(empty).toEqual([]);
});

test("map emits completion order when orderOutputs is false", async () => {
  const server = new FakeVmonServer();
  const directory = await scratchDirectory();
  const gateA = join(directory, "gate-a");
  const gateB = join(directory, "gate-b");
  const waiter = clientFor(server).remoteFunctionFromSource<[path: string, value: number], number>({
    source: waitForFileSource(),
    exportName: "waitForFile",
  });

  const stream = waiter.starmap(
    [
      [gateA, 1],
      [gateB, 2],
    ],
    { concurrency: 2, orderOutputs: false },
  );
  const iterator = stream[Symbol.asyncIterator]();
  await writeFile(gateB, "go");
  const first = await iterator.next();
  expect(first.value).toBe(2);
  await writeFile(gateA, "go");
  const second = await iterator.next();
  expect(second.value).toBe(1);
  expect((await iterator.next()).done).toBe(true);
  expect(server.liveSocketCount()).toBe(0);
});

test("map yields errors in place with returnExceptions and supports forEach", async () => {
  const server = new FakeVmonServer();
  const picky = clientFor(server).remoteFunction(function picky(value: number) {
    if (value % 2 === 1) throw new RangeError(`odd ${value}`);
    return value * 10;
  });

  const mixed = await collect(picky.map([1, 2, 3], { returnExceptions: true }));
  expect(mixed).toHaveLength(3);
  expect(mixed[0]).toBeInstanceOf(RangeError);
  expect(mixed[1]).toBe(20);
  expect(mixed[2]).toBeInstanceOf(RangeError);

  await picky.forEach([1, 2, 3], { ignoreExceptions: true });
  await expect(picky.forEach([1])).rejects.toThrow("odd 1");
  expect(server.liveSocketCount()).toBe(0);
});

test("map fails fast and tears down every worker sandbox", async () => {
  const server = new FakeVmonServer();
  const explode = clientFor(server).remoteFunction(function explode(value: number): never {
    throw new RangeError(`boom ${value}`);
  });

  await expect(collect(explode.map([1, 2, 3, 4], { concurrency: 2 }))).rejects.toBeInstanceOf(
    RangeError,
  );
  expect(server.createBodies.length).toBeLessThanOrEqual(2);
  expect([...server.terminatedSandboxIds].sort()).toEqual(
    server.createBodies.map((_, index) => `sb-${index + 1}`).sort(),
  );
  expect(server.liveSocketCount()).toBe(0);

  await expect(collect(explode.map([1], { concurrency: 0 }))).rejects.toThrow(
    "concurrency must be an integer >= 1",
  );

  const doubler = clientFor(server).remoteFunction(function* generate(value: number) {
    yield value;
  });
  expect(() => doubler.map([1])).toThrow("a generator function cannot be mapped");
});

test("starmap spreads argument tuples", async () => {
  const server = new FakeVmonServer();
  const add = clientFor(server).remoteFunction((left: number, right: number) => left + right);
  await expect(
    collect(
      add.starmap([
        [1, 2],
        [3, 4],
      ]),
    ),
  ).resolves.toEqual([3, 7]);
  expect(server.liveSocketCount()).toBe(0);
});

test("spawn runs on dedicated sessions of one shared sandbox", async () => {
  const server = new FakeVmonServer();
  const directory = await scratchDirectory();
  const gateA = join(directory, "spawn-a");
  const gateB = join(directory, "spawn-b");
  const client = clientFor(server);
  const waiter = client.remoteFunctionFromSource<[path: string, value: number], number>({
    source: waitForFileSource(),
    exportName: "waitForFile",
  });

  const first = await waiter.spawn(gateA, 11);
  const second = await waiter.spawn(gateB, 22);
  expect(first.done()).toBe(false);
  await expect(first.get(50)).rejects.toThrow("was not ready within 50ms");

  await writeFile(gateA, "go");
  await writeFile(gateB, "go");
  await expect(FunctionCall.gather(first, second)).resolves.toEqual([11, 22]);
  expect(first.done()).toBe(true);
  // One shared sandbox, one exec session per spawn.
  expect(server.createBodies).toHaveLength(1);
  expect(server.execs).toHaveLength(2);

  const generatorFn = client.remoteFunction(function* nope() {
    yield 1;
  });
  await expect(generatorFn.spawn()).rejects.toThrow("cannot spawn a generator function");

  await waiter.terminate();
});

test("cancel closes the spawned session and get rejects; gather can collect failures", async () => {
  const server = new FakeVmonServer();
  const directory = await scratchDirectory();
  const gate = join(directory, "cancel-gate");
  const client = clientFor(server);
  const waiter = client.remoteFunctionFromSource<[path: string, value: number], number>({
    source: waitForFileSource(),
    exportName: "waitForFile",
  });
  const failer = client.remoteFunction(function failer(): never {
    throw new TypeError("spawned failure");
  });

  const hanging = await waiter.spawn(gate, 1);
  const failing = await failer.spawn();
  hanging.cancel();
  await expect(hanging.get()).rejects.toThrow("remote function call was cancelled");
  expect(hanging.done()).toBe(true);

  const gathered = await FunctionCall.gather(hanging, failing, { returnExceptions: true });
  expect(gathered).toHaveLength(2);
  expect(gathered[0]).toBeInstanceOf(RemoteFunctionError);
  expect(gathered[1]).toBeInstanceOf(TypeError);
  await expect(FunctionCall.gather(failing)).rejects.toThrow("spawned failure");

  await waiter.terminate();
  await failer.terminate();
  expect(server.liveSocketCount()).toBe(0);
});

test("retries rerun guest failures with recorded policy delays", async () => {
  const server = new FakeVmonServer();
  const directory = await scratchDirectory();
  const client = clientFor(server);

  const fixedDelays: number[] = [];
  const flakyFixed = client.remoteFunctionFromSource<[path: string, succeedAt: number], number>(
    { source: counterSource(), exportName: "flaky" },
    {
      retries: 2,
      retrySleep: (seconds) => {
        fixedDelays.push(seconds);
        return Promise.resolve();
      },
    },
  );
  await expect(flakyFixed.remote(join(directory, "count-fixed"), 3)).resolves.toBe(3);
  expect(fixedDelays).toEqual([1, 1]);
  expect(server.execs).toHaveLength(1);
  await flakyFixed.terminate();

  const backoffDelays: number[] = [];
  const flakyBackoff = client.remoteFunctionFromSource<[path: string, succeedAt: number], number>(
    { source: counterSource(), exportName: "flaky" },
    {
      retries: new Retries({
        maxRetries: 3,
        initialDelay: 0.5,
        backoffCoefficient: 2,
        maxDelay: 60,
      }),
      retrySleep: (seconds) => {
        backoffDelays.push(seconds);
        return Promise.resolve();
      },
    },
  );
  await expect(flakyBackoff.remote(join(directory, "count-backoff"), 3)).resolves.toBe(3);
  expect(backoffDelays).toEqual([0.5, 1]);
  await flakyBackoff.terminate();

  const exhaustedDelays: number[] = [];
  const alwaysFails = client.remoteFunction(
    function alwaysFails(): never {
      throw new RangeError("permanent");
    },
    {
      retries: 1,
      retrySleep: (seconds) => {
        exhaustedDelays.push(seconds);
        return Promise.resolve();
      },
    },
  );
  await expect(alwaysFails.remote()).rejects.toThrow("permanent");
  expect(exhaustedDelays).toEqual([1]);
  await alwaysFails.terminate();
});

test("per-call timeouts kill the session and the next call recovers", async () => {
  const server = new FakeVmonServer();
  const slow = clientFor(server).remoteFunction(
    async function slow(value: number) {
      if (value === 0) {
        await new Promise((resolve) => setTimeout(resolve, 60_000));
      }
      return value;
    },
    { timeout: 0.3 },
  );

  try {
    await slow.remote(0);
    throw new Error("expected the call to time out");
  } catch (error) {
    expect(error).toBeInstanceOf(RemoteFunctionError);
    expect((error as RemoteFunctionError).code).toBe("timeout");
    expect((error as RemoteFunctionError).message).toContain("timed out after 0.3s");
  }

  await expect(slow.remote(5)).resolves.toBe(5);
  expect(server.execs).toHaveLength(2);
  await slow.terminate();
  expect(server.liveSocketCount()).toBe(0);
});

test("source specs expose an exported handler without closure serialization", async () => {
  const server = new FakeVmonServer();
  const sourceFunction = clientFor(server).remoteFunctionFromSource<
    [value: number],
    { doubled: number }
  >({
    source: "export function handle(value) { return { doubled: value * 2 }; }",
    exportName: "handle",
  });

  await expect(sourceFunction.remote(9)).resolves.toEqual({ doubled: 18 });
  await sourceFunction.terminate();
});

test("daemon errors retain status and structured codes during provisioning", async () => {
  const server = new FakeVmonServer();
  server.createFailure = { status: 503, code: "capacity_exhausted", message: "no VM slots" };
  const remote = clientFor(server).remoteFunction((value: number) => value);

  try {
    await remote.remote(1);
    throw new Error("expected sandbox provisioning to fail");
  } catch (error) {
    expect(error).toBeInstanceOf(APIError);
    const failure = error as APIError;
    expect(failure.status).toBe(503);
    expect(failure.code).toBe("capacity_exhausted");
    expect(failure.message).toBe("no VM slots");
  }
  // Provisioning failures are not infra-retried.
  expect(server.createBodies).toHaveLength(1);
});

test("images without Node.js produce the guidance error without retries", async () => {
  const server = new FakeVmonServer();
  server.nodeMissing = true;
  const remote = clientFor(server).remoteFunction((value: number) => value, {
    image: "alpine:3",
  });

  await expect(remote.remote(1)).rejects.toThrow(MISSING_NODE_HINT);
  expect(server.createBodies).toHaveLength(1);
  expect(server.execs).toHaveLength(1);
});

const STATUS_GRPC_CODE: Record<number, Code> = {
  400: Code.InvalidArgument,
  401: Code.Unauthenticated,
  404: Code.NotFound,
  409: Code.Aborted,
  503: Code.Unavailable,
};

async function pollUntil(condition: () => boolean, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("pollUntil condition never became true");
    await Bun.sleep(5);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
