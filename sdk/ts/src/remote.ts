import type { SandboxCreateRequestWithSecrets, VmonClient } from "./client";

/** The scalar values representable by JSON. */
export type JsonPrimitive = boolean | number | string | null;

/** A value that survives a strict JSON round trip without coercion. */
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

/** A JavaScript function whose argument tuple and result type are retained remotely. */
export type RemoteCallable<Arguments extends unknown[], Result> = (
  ...args: Arguments
) => Result | Promise<Result>;

/** JavaScript module source plus the name of its exported remote handler. */
export interface RemoteFunctionSourceSpec {
  source: string;
  exportName: string;
}

/** JSON invocation written for the guest runner. */
export interface RemoteFunctionInvocation {
  source: string;
  exportName: string;
  args: JsonValue[];
}

/** A successful guest runner response. */
export interface RemoteFunctionSuccess {
  ok: true;
  result: JsonValue;
  stdout: string;
}

/** Structured failure details produced by the guest runner. */
export interface RemoteFunctionFailureDetails {
  type: string;
  message: string;
  stack: string;
}

/** A failed guest runner response. */
export interface RemoteFunctionFailure {
  ok: false;
  error: RemoteFunctionFailureDetails;
  stdout: string;
}

/** The language-neutral response envelope emitted by the guest runner. */
export type RemoteFunctionResponse = RemoteFunctionSuccess | RemoteFunctionFailure;

/** Receives stdout captured while a remote handler runs. */
export type RemoteStdoutHandler = (output: string) => void;

/** Sandbox and invocation settings for a remote function. */
export interface RemoteFunctionOptions extends SandboxCreateRequestWithSecrets {
  invocationTimeout?: number | null;
  onStdout?: RemoteStdoutHandler;
}

/** Controls the bounded worker pool used by {@link RemoteFunction.map}. */
export interface RemoteMapOptions {
  concurrency?: number;
  ordered?: boolean;
}

/** Structured metadata attached to a {@link RemoteFunctionError}. */
export interface RemoteFunctionErrorDetails {
  remoteType?: string;
  remoteStack?: string;
  cause?: unknown;
}

/** The default image, chosen for its stable Node.js 22 runtime and small Debian base. */
export const DEFAULT_REMOTE_FUNCTION_IMAGE = "node:22-slim";

const RUNNER_PATH = "/tmp/vmon-remote-function-runner.mjs";
const PAYLOAD_PATH_PREFIX = "/tmp/vmon-remote-function-invocation";
const CONVENIENCE_EXPORT = "__vmon_handler";
const NODE_CHECK_TIMEOUT_SECONDS = 10;
const TERMINAL_SANDBOX_STATUSES = new Set(["stopped", "terminated", "failed"]);

const REMOTE_FUNCTION_RUNNER = String.raw`
import { readFile } from "node:fs/promises";

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
    const type = typeof error.name === "string" && error.name.length > 0
      ? error.name
      : error.constructor && typeof error.constructor.name === "string"
        ? error.constructor.name
        : "RemoteError";
    return {
      type,
      message: typeof error.message === "string" ? error.message : String(error),
      stack: typeof error.stack === "string" ? error.stack : "",
    };
  }
  return { type: "RemoteError", message: String(error), stack: "" };
}

const originalWrite = process.stdout.write;
let capturedStdout = "";
process.stdout.write = function captureStdout(chunk, encoding, callback) {
  capturedStdout += typeof chunk === "string" ? chunk : Buffer.from(chunk).toString();
  const done = typeof encoding === "function" ? encoding : callback;
  if (typeof done === "function") {
    queueMicrotask(done);
  }
  return true;
};

let response;
try {
  const payload = JSON.parse(await readFile(process.argv[2], "utf8"));
  if (payload === null || typeof payload !== "object") {
    throw new TypeError("remote function invocation must be an object");
  }
  if (typeof payload.source !== "string" || typeof payload.exportName !== "string") {
    throw new TypeError("remote function source and exportName must be strings");
  }
  if (!Array.isArray(payload.args)) {
    throw new TypeError("remote function args must be an array");
  }
  const moduleSource = payload.source + "\n//# sourceURL=vmon-remote-function.mjs\n";
  const moduleUrl = "data:text/javascript;base64," + Buffer.from(moduleSource).toString("base64");
  // The user module is selected from the invocation at runtime, so a static import cannot name it.
  const namespace = await import(moduleUrl);
  const handler = namespace[payload.exportName];
  if (typeof handler !== "function") {
    throw new TypeError("remote function module does not export callable " + payload.exportName);
  }
  const result = await handler(...payload.args);
  response = {
    ok: true,
    result: jsonValue(result, "remote function result", new Set()),
    stdout: capturedStdout,
  };
} catch (error) {
  response = {
    ok: false,
    error: errorDetails(error),
    stdout: capturedStdout,
  };
}
process.stdout.write = originalWrite;
originalWrite.call(process.stdout, JSON.stringify(response));
`;

/** An error raised when guest-side function evaluation fails. */
export class RemoteFunctionError extends Error {
  override readonly name = "RemoteFunctionError";
  readonly remoteType: string;
  readonly remoteStack: string;

  constructor(message: string, details: RemoteFunctionErrorDetails = {}) {
    super(message, details.cause === undefined ? undefined : { cause: details.cause });
    this.remoteType = details.remoteType ?? "RemoteError";
    this.remoteStack = details.remoteStack ?? "";
  }
}

/** A reusable source-serialized function backed by vmon sandboxes. */
export class RemoteFunction<Arguments extends unknown[] = JsonValue[], Result = JsonValue> {
  readonly #client: VmonClient;
  readonly #spec: RemoteFunctionSourceSpec;
  readonly #sandboxRequest: SandboxCreateRequestWithSecrets;
  readonly #invocationTimeout: number | null | undefined;
  readonly #onStdout: RemoteStdoutHandler;
  #cachedSandbox: Promise<string> | null = null;
  #termination: Promise<void> | null = null;
  #invocationSequence = 0;

  constructor(
    client: VmonClient,
    spec: RemoteFunctionSourceSpec,
    options: RemoteFunctionOptions = {},
  ) {
    this.#client = client;
    this.#spec = checkedSourceSpec(spec);
    const { invocationTimeout, onStdout, ...sandboxRequest } = options;
    const reusableSecrets =
      sandboxRequest.secrets === undefined || sandboxRequest.secrets === null
        ? sandboxRequest.secrets
        : [...sandboxRequest.secrets];
    this.#sandboxRequest = {
      image: DEFAULT_REMOTE_FUNCTION_IMAGE,
      ...sandboxRequest,
      secrets: reusableSecrets,
    };
    this.#invocationTimeout = invocationTimeout;
    this.#onStdout = onStdout ?? forwardStdout;
  }

  /** Run the handler in one lazily created, reusable sandbox. */
  async remote(...args: Arguments): Promise<Result> {
    const jsonArgs = checkedJsonValue(args, "remote function arguments", new Set());
    if (!Array.isArray(jsonArgs)) {
      throw new TypeError("remote function arguments must be an array");
    }
    const sandboxId = await this.#ensureSandbox();
    return this.#invoke(sandboxId, jsonArgs);
  }

  /** Map unary calls over ephemeral workers, returning input order unless disabled. */
  async map(items: Iterable<Arguments[0]>, options: RemoteMapOptions = {}): Promise<Result[]> {
    const concurrency = options.concurrency ?? 4;
    if (!Number.isInteger(concurrency) || concurrency < 1) {
      throw new RangeError("concurrency must be an integer >= 1");
    }

    const argumentSets: JsonValue[][] = [];
    for (const item of items) {
      const jsonArgs = checkedJsonValue([item], "remote function arguments", new Set());
      if (!Array.isArray(jsonArgs)) {
        throw new TypeError("remote function arguments must be an array");
      }
      argumentSets.push(jsonArgs);
    }
    if (argumentSets.length === 0) {
      return [];
    }

    const sandboxIds: string[] = [];
    const orderedResults: Result[] = [];
    const completionResults: Result[] = [];
    let nextIndex = 0;
    let stopped = false;
    let failed = false;
    let firstError: unknown;

    try {
      const poolSize = Math.min(concurrency, argumentSets.length);
      for (let worker = 0; worker < poolSize; worker += 1) {
        sandboxIds.push(await this.#provisionSandbox());
      }

      const workers = sandboxIds.map(async (sandboxId) => {
        while (!stopped) {
          const index = nextIndex;
          nextIndex += 1;
          if (index >= argumentSets.length) {
            return;
          }
          try {
            const result = await this.#invoke(sandboxId, argumentSets[index]);
            orderedResults[index] = result;
            completionResults.push(result);
          } catch (error) {
            if (!failed) {
              failed = true;
              firstError = error;
              stopped = true;
            }
            return;
          }
        }
      });
      await Promise.all(workers);
      if (failed) {
        throw firstError;
      }
      return options.ordered === false ? completionResults : orderedResults;
    } finally {
      await Promise.allSettled(
        sandboxIds.map((sandboxId) => this.#client.terminateSandbox(sandboxId)),
      );
    }
  }

  /** Terminate the cached sandbox; repeated and concurrent calls are harmless. */
  async terminate(): Promise<void> {
    const sandbox = this.#cachedSandbox;
    this.#cachedSandbox = null;
    const previousTermination = this.#termination;
    if (sandbox === null) {
      if (previousTermination !== null) {
        await previousTermination;
      }
      return;
    }

    const termination = (async () => {
      if (previousTermination !== null) {
        await previousTermination.catch(() => undefined);
      }
      let sandboxId: string;
      try {
        sandboxId = await sandbox;
      } catch {
        return;
      }
      await this.#client.terminateSandbox(sandboxId);
    })();
    this.#termination = termination;
    try {
      await termination;
    } finally {
      if (this.#termination === termination) {
        this.#termination = null;
      }
    }
  }

  async #ensureSandbox(): Promise<string> {
    const cached = this.#cachedSandbox;
    if (cached === null) {
      const creation = this.#provisionSandbox();
      this.#cachedSandbox = creation;
      try {
        return await creation;
      } catch (error) {
        if (this.#cachedSandbox === creation) {
          this.#cachedSandbox = null;
        }
        throw error;
      }
    }

    let sandboxId: string;
    try {
      sandboxId = await cached;
    } catch (error) {
      if (this.#cachedSandbox === cached) {
        this.#cachedSandbox = null;
      }
      throw error;
    }
    const live = await this.#sandboxIsLive(sandboxId);
    if (this.#cachedSandbox !== cached) {
      return this.#ensureSandbox();
    }
    if (live) {
      return sandboxId;
    }

    const replacement = (async () => {
      await this.#client.terminateSandbox(sandboxId).catch(() => undefined);
      return this.#provisionSandbox();
    })();
    this.#cachedSandbox = replacement;
    try {
      return await replacement;
    } catch (error) {
      if (this.#cachedSandbox === replacement) {
        this.#cachedSandbox = null;
      }
      throw error;
    }
  }

  async #sandboxIsLive(sandboxId: string): Promise<boolean> {
    try {
      const view: unknown = await this.#client.getSandbox(sandboxId);
      if (!isRecord(view)) {
        throw new RemoteFunctionError("sandbox lookup returned a non-object response", {
          remoteType: "ProtocolError",
        });
      }
      const status = typeof view.status === "string" ? view.status.toLowerCase() : "";
      return !TERMINAL_SANDBOX_STATUSES.has(status);
    } catch (error) {
      if (isRecord(error) && (error.status === 404 || error.code === "not_found")) {
        return false;
      }
      throw error;
    }
  }

  async #provisionSandbox(): Promise<string> {
    let sandboxId: string | null = null;
    try {
      const view: unknown = await this.#client.createSandbox(this.#sandboxRequest);
      sandboxId = sandboxIdentifier(view);
      const check = await this.#client.execSandbox(sandboxId, {
        cmd: ["node", "--version"],
        timeout: NODE_CHECK_TIMEOUT_SECONDS,
      });
      if (check.exit !== 0) {
        const detail = decodeBase64(check.stderr_b64).trim();
        throw new RemoteFunctionError(
          detail ||
            `remote function images must provide Node.js; use ${DEFAULT_REMOTE_FUNCTION_IMAGE} or another Node-capable image`,
          { remoteType: "RuntimeUnavailable" },
        );
      }
      await this.#client.writeSandboxFile(sandboxId, RUNNER_PATH, REMOTE_FUNCTION_RUNNER);
      return sandboxId;
    } catch (error) {
      if (sandboxId !== null) {
        await this.#client.terminateSandbox(sandboxId).catch(() => undefined);
      }
      throw error;
    }
  }

  async #invoke(sandboxId: string, args: JsonValue[]): Promise<Result> {
    this.#invocationSequence += 1;
    const payloadPath = `${PAYLOAD_PATH_PREFIX}-${this.#invocationSequence}.json`;
    const invocation: RemoteFunctionInvocation = {
      source: this.#spec.source,
      exportName: this.#spec.exportName,
      args,
    };
    let uploaded = false;
    try {
      await this.#client.writeSandboxFile(sandboxId, payloadPath, JSON.stringify(invocation));
      uploaded = true;
      const capture = await this.#client.execSandbox(sandboxId, {
        cmd: ["node", RUNNER_PATH, payloadPath],
        timeout: this.#invocationTimeout,
      });
      if (capture.exit !== 0) {
        const detail = decodeBase64(capture.stderr_b64).trim();
        throw new RemoteFunctionError(
          detail || `remote function runner exited with ${capture.exit}`,
          { remoteType: "RunnerProcessError" },
        );
      }
      const response = remoteResponse<Result>(decodeBase64(capture.stdout_b64));
      if (response.stdout.length > 0) {
        this.#onStdout(response.stdout);
      }
      if (!response.ok) {
        throw new RemoteFunctionError(response.error.message, {
          remoteType: response.error.type,
          remoteStack: response.error.stack,
        });
      }
      return response.result;
    } finally {
      if (uploaded) {
        await this.#client.deleteSandboxFile(sandboxId, payloadPath).catch(() => undefined);
      }
    }
  }
}

/** Create a typed remote function from a source-serializable JavaScript closure. */
export function remoteFunction<Arguments extends unknown[], Result>(
  client: VmonClient,
  fn: RemoteCallable<Arguments, Result>,
  options: RemoteFunctionOptions = {},
): RemoteFunction<Arguments, Result> {
  const source = Function.prototype.toString.call(fn);
  if (fn.name.startsWith("bound ")) {
    throw new TypeError("bound functions cannot be serialized for remote execution");
  }
  if (source.includes("[native code]")) {
    throw new TypeError("native functions cannot be serialized for remote execution");
  }
  if (source.trimStart().startsWith("class ")) {
    throw new TypeError("class constructors cannot be serialized for remote execution");
  }
  try {
    const recreated: unknown = Function(`"use strict"; return (${source});`)();
    if (typeof recreated !== "function") {
      throw new TypeError("serialized source did not produce a function");
    }
  } catch (error) {
    throw new TypeError("remote functions must have standalone JavaScript expression source", {
      cause: error,
    });
  }
  return new RemoteFunction<Arguments, Result>(
    client,
    {
      source: `export const ${CONVENIENCE_EXPORT} = (${source});\n`,
      exportName: CONVENIENCE_EXPORT,
    },
    options,
  );
}

/** Create a typed remote function from JavaScript module source and an exported handler. */
export function remoteFunctionFromSource<
  Arguments extends unknown[] = JsonValue[],
  Result = JsonValue,
>(
  client: VmonClient,
  spec: RemoteFunctionSourceSpec,
  options: RemoteFunctionOptions = {},
): RemoteFunction<Arguments, Result> {
  return new RemoteFunction<Arguments, Result>(client, spec, options);
}

function checkedSourceSpec(spec: RemoteFunctionSourceSpec): RemoteFunctionSourceSpec {
  if (spec.source.trim().length === 0) {
    throw new TypeError("remote function source must not be empty");
  }
  if (!/^(?:default|[$A-Z_a-z][$\w]*)$/.test(spec.exportName)) {
    throw new TypeError("remote function exportName must be a JavaScript identifier or 'default'");
  }
  return { source: spec.source, exportName: spec.exportName };
}

function checkedJsonValue(value: unknown, path: string, active: Set<object>): JsonValue {
  if (value === null || typeof value === "boolean" || typeof value === "string") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new TypeError(`${path} contains a non-finite number`);
    }
    return value;
  }
  if (typeof value !== "object") {
    throw new TypeError(`${path} contains a non-JSON value`);
  }
  if (active.has(value)) {
    throw new TypeError(`${path} contains a cycle`);
  }
  active.add(value);
  try {
    if (Array.isArray(value)) {
      const result: JsonValue[] = [];
      for (let index = 0; index < value.length; index += 1) {
        if (!(index in value)) {
          throw new TypeError(`${path} contains a sparse array`);
        }
        result.push(checkedJsonValue(value[index], `${path}[${index}]`, active));
      }
      return result;
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError(`${path} contains a non-plain object`);
    }
    if (Object.getOwnPropertySymbols(value).length !== 0) {
      throw new TypeError(`${path} contains symbol properties`);
    }
    const result: Record<string, JsonValue> = {};
    const descriptors = Object.getOwnPropertyDescriptors(value);
    const keys = Object.keys(value);
    keys.sort();
    for (const key of keys) {
      const descriptor = descriptors[key];
      if (!("value" in descriptor)) {
        throw new TypeError(`${path}.${key} is an accessor property`);
      }
      result[key] = checkedJsonValue(descriptor.value, `${path}.${key}`, active);
    }
    return result;
  } finally {
    active.delete(value);
  }
}

interface DecodedRemoteFunctionSuccess<Result> {
  ok: true;
  result: Result;
  stdout: string;
}

interface DecodedRemoteFunctionFailure {
  ok: false;
  error: RemoteFunctionFailureDetails;
  stdout: string;
}

type DecodedRemoteFunctionResponse<Result> =
  | DecodedRemoteFunctionSuccess<Result>
  | DecodedRemoteFunctionFailure;

function remoteResponse<Result>(raw: string): DecodedRemoteFunctionResponse<Result> {
  let response: DecodedRemoteFunctionResponse<Result>;
  try {
    response = JSON.parse(raw);
  } catch (error) {
    throw new RemoteFunctionError("remote function returned invalid JSON", {
      remoteType: "ProtocolError",
      cause: error,
    });
  }
  if (
    !isRecord(response) ||
    typeof response.ok !== "boolean" ||
    typeof response.stdout !== "string"
  ) {
    throw new RemoteFunctionError("remote function returned an invalid response envelope", {
      remoteType: "ProtocolError",
    });
  }
  if (response.ok) {
    checkedJsonValue(response.result, "remote function result", new Set());
    return response;
  }
  if (
    !isRecord(response.error) ||
    typeof response.error.type !== "string" ||
    typeof response.error.message !== "string" ||
    typeof response.error.stack !== "string"
  ) {
    throw new RemoteFunctionError("remote function returned invalid error details", {
      remoteType: "ProtocolError",
    });
  }
  return response;
}

function sandboxIdentifier(view: unknown): string {
  if (!isRecord(view)) {
    throw new RemoteFunctionError("sandbox creation returned a non-object response", {
      remoteType: "ProtocolError",
    });
  }
  const id = typeof view.id === "string" && view.id.length > 0 ? view.id : view.name;
  if (typeof id !== "string" || id.length === 0) {
    throw new RemoteFunctionError("sandbox creation response did not include an id or name", {
      remoteType: "ProtocolError",
    });
  }
  return id;
}

function decodeBase64(value: string): string {
  try {
    const binary = atob(value);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) {
      bytes[index] = binary.charCodeAt(index);
    }
    return new TextDecoder().decode(bytes);
  } catch (error) {
    throw new RemoteFunctionError("remote function transport returned invalid base64", {
      remoteType: "ProtocolError",
      cause: error,
    });
  }
}

function forwardStdout(output: string): void {
  if (typeof process !== "undefined") {
    process.stdout.write(output);
    return;
  }
  console.log(output);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
