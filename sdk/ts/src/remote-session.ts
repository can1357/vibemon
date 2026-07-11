import { APIError, ProtocolError, TransportError } from "./errors";
import type { Process } from "./process";
import type { Sandbox } from "./sandbox";

/** The scalar values representable by JSON. */
export type JsonPrimitive = boolean | number | string | null;

/** A value that survives a strict JSON round trip without coercion. */
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

/** Failure category attached to {@link RemoteFunctionError}. */
export type RemoteFailureCode =
  | "remote"
  | "infra"
  | "protocol"
  | "timeout"
  | "cancelled"
  | "unavailable";

/** Structured metadata attached to a {@link RemoteFunctionError}. */
export interface RemoteFunctionErrorDetails {
  remoteType?: string;
  remoteStack?: string;
  cause?: unknown;
  code?: RemoteFailureCode;
}

/** An error raised for infra failures and non-builtin guest exception types. */
export class RemoteFunctionError extends Error {
  override readonly name = "RemoteFunctionError";
  /** The guest-side error type name, or `RemoteError` for infra failures. */
  readonly remoteType: string;
  /** The guest-side stack trace, when one was captured. */
  readonly remoteStack: string;
  /** Failure category used by the retry policy. */
  readonly code: RemoteFailureCode;

  constructor(message: string, details: RemoteFunctionErrorDetails = {}) {
    super(message, details.cause === undefined ? undefined : { cause: details.cause });
    this.remoteType = details.remoteType ?? "RemoteError";
    this.remoteStack = details.remoteStack ?? "";
    this.code = details.code ?? "infra";
  }
}

/** Guest path of the persistent session runner script. */
export const SESSION_RUNNER_PATH = "/tmp/vmon-fn-runner.js";

const HELLO_TIMEOUT_MS = 30_000;
const STDERR_TAIL_LIMIT = 64;

/** Raised guidance when the guest image has no Node.js interpreter. */
export const MISSING_NODE_HINT =
  "remote function images must provide Node.js; use node:22-slim or another Node-capable image";

/**
 * The persistent in-guest session runner. One long-lived `node` process per
 * worker session, speaking NDJSON over the exec WebSocket: `call`/`shutdown`
 * ops in, `hello`/`out`/`yield`/`result`/`error` events out. Values travel as
 * `{"json": ...}` frames only. Plain CommonJS, no dependencies, Node >= 18.
 */
export const SESSION_RUNNER = `"use strict";
const fs = require("node:fs");

let currentId = null;

// fd 1 is reserved as the protocol channel; user-visible output is rerouted
// through "out" events by patching the stream-level write below.
function send(frame) {
  fs.writeSync(1, JSON.stringify(frame) + "\\n");
}

function makeOutputProxy(stream, target) {
  let chunks = [];
  let size = 0;
  function flush() {
    if (size === 0) return;
    const data = chunks.join("");
    chunks = [];
    size = 0;
    send({ event: "out", id: currentId, stream: stream, data: data });
  }
  target.write = function proxyWrite(chunk, encoding, callback) {
    const text = typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8");
    if (text.length > 0) {
      chunks.push(text);
      size += text.length;
      if (size >= 8192 || text.indexOf("\\n") >= 0) flush();
    }
    const done = typeof encoding === "function" ? encoding : callback;
    if (typeof done === "function") queueMicrotask(done);
    return true;
  };
  return flush;
}

const flushStdout = makeOutputProxy("stdout", process.stdout);
const flushStderr = makeOutputProxy("stderr", process.stderr);

function flushOutput() {
  flushStdout();
  flushStderr();
}

function jsonValue(value, path, active) {
  if (value === null || typeof value === "boolean" || typeof value === "string") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new TypeError(path + " contains a non-finite number");
    }
    return value;
  }
  if (typeof value === "undefined") {
    throw new TypeError(path + " contains a non-JSON value");
  }
  if (typeof value !== "object") {
    throw new TypeError(path + " contains a non-JSON value");
  }
  if (active.has(value)) {
    throw new TypeError(path + " contains a cycle");
  }
  active.add(value);
  try {
    if (Array.isArray(value)) {
      const result = [];
      for (let index = 0; index < value.length; index += 1) {
        if (!(index in value)) {
          throw new TypeError(path + " contains a sparse array");
        }
        result.push(jsonValue(value[index], path + "[" + index + "]", active));
      }
      return result;
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError(path + " contains a non-plain object");
    }
    if (Object.getOwnPropertySymbols(value).length !== 0) {
      throw new TypeError(path + " contains symbol properties");
    }
    const result = {};
    const descriptors = Object.getOwnPropertyDescriptors(value);
    const keys = Object.keys(value).sort();
    for (const key of keys) {
      const descriptor = descriptors[key];
      if (!("value" in descriptor)) {
        throw new TypeError(path + "." + key + " is an accessor property");
      }
      result[key] = jsonValue(descriptor.value, path + "." + key, active);
    }
    return result;
  } finally {
    active.delete(value);
  }
}

function errorDetails(error) {
  if (error !== null && typeof error === "object") {
    const type =
      typeof error.name === "string" && error.name.length > 0
        ? error.name
        : error.constructor && typeof error.constructor.name === "string"
          ? error.constructor.name
          : "RemoteError";
    return {
      etype: type,
      message: typeof error.message === "string" ? error.message : String(error),
      traceback: typeof error.stack === "string" ? error.stack : "",
    };
  }
  return { etype: "RemoteError", message: String(error), traceback: "" };
}

const namespaces = new Map();

function loadModule(op) {
  if (typeof op.hash !== "string" || op.hash.length === 0) {
    return Promise.reject(new TypeError("call op is missing a source hash"));
  }
  const cached = namespaces.get(op.hash);
  if (cached !== undefined) return cached;
  if (typeof op.source !== "string") {
    return Promise.reject(new Error("unknown source hash " + op.hash));
  }
  const moduleSource =
    op.source + "\\n//# sourceURL=vmon-remote-function-" + op.hash.slice(0, 12) + ".mjs\\n";
  const moduleUrl =
    "data:text/javascript;base64," + Buffer.from(moduleSource).toString("base64");
  // The user module is selected from the call op at runtime, so a static
  // import cannot name it.
  const loading = import(moduleUrl);
  namespaces.set(op.hash, loading);
  loading.catch(function unregister() {
    namespaces.delete(op.hash);
  });
  return loading;
}

function frameValue(frame) {
  if (frame !== null && typeof frame === "object" && "json" in frame) return frame.json;
  throw new TypeError("unsupported value frame; this runner accepts json values only");
}

function isThenable(value) {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    typeof value.then === "function"
  );
}

function isGeneratorLike(value) {
  if (value === null || typeof value !== "object") return false;
  if (typeof value.next !== "function" || typeof value.throw !== "function") return false;
  return (
    typeof value[Symbol.asyncIterator] === "function" ||
    typeof value[Symbol.iterator] === "function"
  );
}

async function handleCall(op) {
  const namespace = await loadModule(op);
  const handler = namespace[op.exportName];
  if (typeof handler !== "function") {
    throw new TypeError("remote function module does not export callable " + op.exportName);
  }
  const args = frameValue(op.args);
  if (!Array.isArray(args)) {
    throw new TypeError("remote function args must be an array");
  }
  const mode = op.mode === "iter" ? "iter" : "value";
  let result = handler.apply(null, args);
  if (isThenable(result)) result = await result;
  if (mode === "iter") {
    if (!isGeneratorLike(result)) {
      throw new TypeError("remote function did not return a generator; use .remote()");
    }
    for await (const item of result) {
      flushOutput();
      send({ event: "yield", id: op.id, json: jsonValue(item, "remote function yield", new Set()) });
    }
    flushOutput();
    send({ event: "result", id: op.id, json: null });
    return;
  }
  if (isGeneratorLike(result)) {
    throw new TypeError("remote function returned a generator; call it with .remoteGen()");
  }
  flushOutput();
  send({
    event: "result",
    id: op.id,
    json: jsonValue(result, "remote function result", new Set()),
  });
}

async function dispatch(line) {
  let op = null;
  try {
    op = JSON.parse(line);
    if (op === null || typeof op !== "object") {
      throw new TypeError("op must be a JSON object");
    }
    currentId = op.id === undefined ? null : op.id;
    if (op.op === "shutdown") {
      flushOutput();
      send({ event: "result", id: currentId, json: null });
      process.exit(0);
    }
    if (op.op !== "call") {
      throw new TypeError("unknown op " + String(op.op));
    }
    await handleCall(op);
  } catch (error) {
    flushOutput();
    const details = errorDetails(error);
    send({
      event: "error",
      id: op !== null && typeof op === "object" && op.id !== undefined ? op.id : null,
      etype: details.etype,
      message: details.message,
      traceback: details.traceback,
    });
  } finally {
    currentId = null;
  }
}

let pending = "";
let chain = Promise.resolve();
process.stdin.setEncoding("utf8");
process.stdin.on("data", function onData(piece) {
  pending += piece;
  let newline = pending.indexOf("\\n");
  while (newline >= 0) {
    const line = pending.slice(0, newline).trim();
    pending = pending.slice(newline + 1);
    if (line.length > 0) {
      chain = chain.then(function run() {
        return dispatch(line);
      });
    }
    newline = pending.indexOf("\\n");
  }
});
process.stdin.on("end", function onEnd() {
  chain.then(function exit() {
    process.exit(0);
  });
});

send({
  event: "hello",
  node: process.versions.node.split(".").map(function parse(part) {
    return Number.parseInt(part, 10) || 0;
  }),
});
`;

/** Failure classification consumed by the retry policy. */
export type FailureKind = "user" | "timeout" | "infra" | "cancelled" | "fatal";

/** Receives one chunk of live remote output. */
export type SessionOutputSink = (stream: "stdout" | "stderr", text: string) => void;

/** The wire identity of one source bundle: content hash plus module source. */
export interface WireSourceSpec {
  hash: string;
  source: string;
  exportName: string;
}

/** One session call: encoded args plus deadline and output routing. */
export interface SessionCallRequest {
  spec: WireSourceSpec;
  args: JsonValue[];
  /** Client-side deadline in seconds; `null` disables the deadline. */
  timeout: number | null;
  output: SessionOutputSink | null;
}

const REMOTE_USER_FAILURE = Symbol("vmon.remoteUserFailure");
const DEADLINE = Symbol("vmon.deadline");

interface FrameWaiter {
  resolve: (frame: Record<string, unknown>) => void;
  reject: (reason: unknown) => void;
}

class FrameQueue {
  readonly #frames: Record<string, unknown>[] = [];
  readonly #waiters: FrameWaiter[] = [];
  #failure: Error | null = null;

  push(frame: Record<string, unknown>): void {
    const waiter = this.#waiters.shift();
    if (waiter) waiter.resolve(frame);
    else this.#frames.push(frame);
  }

  fail(error: Error): void {
    if (this.#failure !== null) return;
    this.#failure = error;
    for (const waiter of this.#waiters.splice(0)) waiter.reject(error);
  }

  get failed(): boolean {
    return this.#failure !== null;
  }

  /** Next frame; rejects with the {@link DEADLINE} sentinel past the deadline. */
  next(deadline: number | null): Promise<Record<string, unknown>> {
    const buffered = this.#frames.shift();
    if (buffered !== undefined) return Promise.resolve(buffered);
    if (this.#failure !== null) return Promise.reject(this.#failure);
    const { promise, resolve, reject } = Promise.withResolvers<Record<string, unknown>>();
    const waiter: FrameWaiter = { resolve, reject };
    this.#waiters.push(waiter);
    if (deadline !== null) {
      const timer = setTimeout(
        () => {
          const index = this.#waiters.indexOf(waiter);
          if (index >= 0) this.#waiters.splice(index, 1);
          reject(DEADLINE);
        },
        Math.max(0, deadline - Date.now()),
      );
      const clear = () => clearTimeout(timer);
      promise.then(clear, clear);
    }
    return promise;
  }
}

const installedRunners = new WeakSet<Sandbox>();

/**
 * Client half of one persistent runner session: exec, hello handshake, calls.
 * One call in flight per session (internal lock); parallelism comes from
 * multiple sessions per sandbox or multiple sandboxes.
 */
export class WorkerSession {
  readonly #process: Process;
  readonly #frames = new FrameQueue();
  readonly #stderrTail: string[] = [];
  readonly #stdoutDecoder = new TextDecoder();
  readonly #stderrDecoder = new TextDecoder();
  readonly #sentHashes = new Set<string>();
  #lineBuffer = "";
  #lock: Promise<void> = Promise.resolve();
  #dead = false;
  #deliberateClose = false;
  #exit: { code: number; signal: number | null } | null = null;
  #opId = 0;
  #outputSink: SessionOutputSink | null = null;

  private constructor(process: Process) {
    this.#process = process;
    void this.#pump();
  }

  /** Install the runner (once per sandbox), exec it, and await its hello. */
  static async start(sandbox: Sandbox): Promise<WorkerSession> {
    if (!installedRunners.has(sandbox)) {
      await sandbox.files.writeText(SESSION_RUNNER_PATH, SESSION_RUNNER);
      installedRunners.add(sandbox);
    }
    const process = await sandbox.exec(["node", SESSION_RUNNER_PATH], {
      env: { VMON_REMOTE: "1" },
    });
    const session = new WorkerSession(process);
    try {
      const hello = await session.#frames.next(Date.now() + HELLO_TIMEOUT_MS);
      if (hello.event !== "hello") {
        throw session.#violation("expected a hello frame");
      }
    } catch (error) {
      session.close();
      if (error === DEADLINE) {
        throw new RemoteFunctionError(
          `remote function runner sent no hello within ${HELLO_TIMEOUT_MS / 1000}s`,
          { code: "infra" },
        );
      }
      throw error;
    }
    return session;
  }

  /** True while the runner process is believed to be running. */
  get alive(): boolean {
    return !this.#dead && this.#exit === null && !this.#frames.failed;
  }

  /** Kill the runner; the server SIGTERMs it when the exec WebSocket closes. */
  close(): void {
    if (!this.#dead) this.#deliberateClose = true;
    this.#dead = true;
    try {
      this.#process.close();
    } catch {
      // The socket may already be closed.
    }
  }

  /** Run one value-mode call to completion. */
  async callValue(request: SessionCallRequest): Promise<JsonValue> {
    const release = await this.#acquire();
    this.#outputSink = request.output;
    try {
      this.#sendOp(this.#buildOp(request, "value"));
      return await this.#awaitResult(request);
    } finally {
      this.#outputSink = null;
      release();
    }
  }

  /**
   * Run one iterator-mode call, yielding each decoded `yield` event. The
   * session lock is held until the generator finishes or is closed; a
   * generator abandoned mid-stream kills the session, since the runner is
   * still yielding and cannot serve another call.
   */
  async *callIter(request: SessionCallRequest): AsyncGenerator<JsonValue, void, undefined> {
    const release = await this.#acquire();
    this.#outputSink = request.output;
    let finished = false;
    try {
      this.#sendOp(this.#buildOp(request, "iter"));
      const deadline = deadlineFor(request.timeout);
      while (true) {
        const frame = await this.#nextEvent(deadline, request.timeout);
        const kind = frame.event;
        if (kind === "out") {
          this.#forward(frame, request.output);
        } else if (kind === "yield") {
          yield this.#decodeValue(frame);
        } else if (kind === "result") {
          finished = true;
          return;
        } else if (kind === "error") {
          finished = true;
          throw remoteCallError(frame);
        } else {
          throw this.#violation(`unexpected ${String(kind)} event`);
        }
      }
    } finally {
      this.#outputSink = null;
      release();
      if (!finished && !this.#dead) this.close();
    }
  }

  async #awaitResult(request: SessionCallRequest): Promise<JsonValue> {
    const deadline = deadlineFor(request.timeout);
    while (true) {
      const frame = await this.#nextEvent(deadline, request.timeout);
      const kind = frame.event;
      if (kind === "out") this.#forward(frame, request.output);
      else if (kind === "result") return this.#decodeValue(frame);
      else if (kind === "error") throw remoteCallError(frame);
      else throw this.#violation(`unexpected ${String(kind)} event`);
    }
  }

  async #nextEvent(
    deadline: number | null,
    timeout: number | null,
  ): Promise<Record<string, unknown>> {
    try {
      return await this.#frames.next(deadline);
    } catch (error) {
      if (error === DEADLINE) {
        this.close();
        throw new RemoteFunctionError(`remote function call timed out after ${timeout}s`, {
          code: "timeout",
        });
      }
      throw error;
    }
  }

  #decodeValue(frame: Record<string, unknown>): JsonValue {
    if (!Object.hasOwn(frame, "json")) {
      throw this.#violation("unsupported value frame; expected a json value");
    }
    return frame.json as JsonValue;
  }

  #forward(frame: Record<string, unknown>, output: SessionOutputSink | null): void {
    if (output === null) return;
    const stream = frame.stream === "stderr" ? "stderr" : "stdout";
    output(stream, typeof frame.data === "string" ? frame.data : String(frame.data ?? ""));
  }

  #buildOp(request: SessionCallRequest, mode: "value" | "iter"): Record<string, unknown> {
    this.#opId += 1;
    const op: Record<string, unknown> = {
      op: "call",
      id: this.#opId,
      hash: request.spec.hash,
      exportName: request.spec.exportName,
      args: { json: request.args },
      mode,
    };
    if (!this.#sentHashes.has(request.spec.hash)) {
      op.source = request.spec.source;
      this.#sentHashes.add(request.spec.hash);
    }
    return op;
  }

  #sendOp(op: Record<string, unknown>): void {
    try {
      this.#process.stdin.write(`${JSON.stringify(op)}\n`);
    } catch (error) {
      this.#dead = true;
      throw new RemoteFunctionError("remote function session is not writable", {
        code: "infra",
        cause: error,
      });
    }
  }

  #acquire(): Promise<() => void> {
    const previous = this.#lock;
    const { promise: gate, resolve: release } = Promise.withResolvers<void>();
    this.#lock = gate;
    return previous.then(() => release);
  }

  async #pump(): Promise<void> {
    let cause: unknown = null;
    try {
      for await (const event of this.#process) {
        if (event.stream === "stdout") {
          this.#ingestStdout(this.#stdoutDecoder.decode(event.data, { stream: true }));
        } else {
          this.#ingestStderr(this.#stderrDecoder.decode(event.data, { stream: true }));
        }
      }
    } catch (error) {
      cause = error;
    }
    try {
      this.#exit = await this.#process.wait();
    } catch (error) {
      cause ??= error;
    }
    this.#frames.fail(this.#deathError(cause));
  }

  #ingestStdout(text: string): void {
    this.#lineBuffer += text;
    let newline = this.#lineBuffer.indexOf("\n");
    while (newline >= 0) {
      const line = this.#lineBuffer.slice(0, newline).trim();
      this.#lineBuffer = this.#lineBuffer.slice(newline + 1);
      if (line.length > 0 && !this.#ingestLine(line)) return;
      newline = this.#lineBuffer.indexOf("\n");
    }
  }

  #ingestLine(line: string): boolean {
    let frame: unknown;
    try {
      frame = JSON.parse(line);
    } catch {
      this.#frames.fail(this.#violation("malformed frame"));
      return false;
    }
    if (typeof frame !== "object" || frame === null || Array.isArray(frame)) {
      this.#frames.fail(this.#violation("non-object frame"));
      return false;
    }
    this.#frames.push(frame as Record<string, unknown>);
    return true;
  }

  #ingestStderr(text: string): void {
    if (text.length === 0) return;
    this.#stderrTail.push(text);
    if (this.#stderrTail.length > STDERR_TAIL_LIMIT) this.#stderrTail.shift();
    this.#outputSink?.("stderr", text);
  }

  #violation(detail: string): RemoteFunctionError {
    this.#dead = true;
    try {
      this.#process.close();
    } catch {
      // The socket may already be closed.
    }
    return new RemoteFunctionError(`remote function runner protocol violation: ${detail}`, {
      code: "protocol",
    });
  }

  #deathError(cause: unknown): RemoteFunctionError {
    this.#dead = true;
    if (this.#deliberateClose) {
      return new RemoteFunctionError("remote function call was cancelled", { code: "cancelled" });
    }
    const tail = this.#stderrTail.join("").trim();
    const clues = `${tail}\n${cause instanceof Error ? cause.message : ""}`.toLowerCase();
    if (
      this.#exit?.code === 127 ||
      clues.includes("no such file") ||
      clues.includes("not found") ||
      clues.includes("enoent")
    ) {
      return new RemoteFunctionError(MISSING_NODE_HINT, { code: "unavailable" });
    }
    const detail =
      tail.length > 0
        ? tail
        : this.#exit !== null
          ? `remote function runner exited with ${this.#exit.code}`
          : "remote function session closed unexpectedly";
    return new RemoteFunctionError(detail, {
      code: "infra",
      cause: cause ?? undefined,
    });
  }
}

const BUILTIN_ERRORS: Record<string, new (message?: string) => Error> = {
  Error,
  TypeError,
  RangeError,
  ReferenceError,
  SyntaxError,
  EvalError,
  URIError,
};

/**
 * Reconstruct a guest `error` event as faithfully as possible: a builtin
 * error type by name with the remote stack attached, else a
 * {@link RemoteFunctionError} carrying the guest type name.
 */
export function remoteCallError(frame: Record<string, unknown>): Error {
  const etype =
    typeof frame.etype === "string" && frame.etype.length > 0 ? frame.etype : "RemoteError";
  const message = typeof frame.message === "string" ? frame.message : "remote function failed";
  const traceback = typeof frame.traceback === "string" ? frame.traceback : "";
  const builtin = BUILTIN_ERRORS[etype];
  let error: Error;
  if (builtin === undefined) {
    error = new RemoteFunctionError(message, {
      remoteType: etype,
      remoteStack: traceback,
      code: "remote",
    });
  } else {
    error = new builtin(message);
    if (traceback.length > 0) {
      error.stack = `${error.stack ?? `${etype}: ${message}`}\n[vmon remote stack]\n${traceback}`;
    }
    Object.defineProperty(error, "remoteStack", {
      value: traceback,
      enumerable: false,
      configurable: true,
      writable: true,
    });
  }
  Object.defineProperty(error, REMOTE_USER_FAILURE, {
    value: true,
    enumerable: false,
    configurable: true,
  });
  return error;
}

/** True for errors raised by the user's remote function body. */
export function isRemoteUserFailure(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as Record<PropertyKey, unknown>)[REMOTE_USER_FAILURE] === true
  );
}

/** Categorize a dispatch failure for the retry policy. */
export function classifyFailure(error: unknown): FailureKind {
  if (error instanceof RemoteFunctionError) {
    switch (error.code) {
      case "remote":
        return "user";
      case "timeout":
        return "timeout";
      case "cancelled":
        return "cancelled";
      case "infra":
      case "protocol":
        return "infra";
      default:
        return "fatal";
    }
  }
  if (isRemoteUserFailure(error)) return "user";
  if (error instanceof APIError) return error.code === "not_found" ? "infra" : "fatal";
  if (error instanceof TransportError || error instanceof ProtocolError) return "infra";
  return "fatal";
}

function deadlineFor(timeout: number | null): number | null {
  return timeout === null ? null : Date.now() + timeout * 1000;
}
