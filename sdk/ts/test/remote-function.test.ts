import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  DEFAULT_REMOTE_FUNCTION_IMAGE,
  RemoteFunctionError,
  VmonApiError,
  VmonClient,
} from "../src";
import type {
  JsonValue,
  RemoteFunctionFailureDetails,
  RemoteFunctionInvocation,
  RemoteFunctionResponse,
  VmonFetch,
} from "../src";

class Gate {
  readonly #promise: Promise<void>;
  readonly #resolve: () => void;

  constructor() {
    const { promise, resolve } = Promise.withResolvers<void>();
    this.#promise = promise;
    this.#resolve = resolve;
  }

  open(): void {
    this.#resolve();
  }

  wait(): Promise<void> {
    return this.#promise;
  }
}

interface CountWaiter {
  count: number;
  resolve: () => void;
}

interface FakeApiFailure {
  status: number;
  code: string;
  message: string;
}

interface RecordedRequest {
  method: string;
  path: string;
  authorization: string | null;
  body: string;
}

class FakeVmonServer {
  readonly requests: RecordedRequest[] = [];
  readonly createBodies: Record<string, unknown>[] = [];
  readonly invocationArguments: JsonValue[][] = [];
  readonly completedArguments: JsonValue[][] = [];
  readonly terminatedSandboxIds: string[] = [];
  readonly terminationResponses: Response[] = [];
  createFailure: FakeApiFailure | null = null;
  runnerSource: string | null = null;
  readonly #files = new Map<string, Map<string, string>>();
  readonly #sandboxStatuses = new Map<string, string>();
  readonly #gates = new Map<string, Gate>();
  readonly #arrivalWaiters: CountWaiter[] = [];
  readonly #completionWaiters: CountWaiter[] = [];
  #nextSandbox = 1;
  #moduleSequence = 0;

  readonly fetch: VmonFetch = async (input, init) => {
    const request = new Request(input, init);
    const url = new URL(request.url);
    const body = request.method === "GET" ? "" : await request.text();
    this.requests.push({
      method: request.method,
      path: `${url.pathname}${url.search}`,
      authorization: request.headers.get("authorization"),
      body,
    });
    if (request.headers.get("authorization") !== "Bearer test-token") {
      return jsonResponse({ code: "unauthorized", message: "bad token" }, 401);
    }

    const segments = url.pathname
      .split("/")
      .filter((segment) => segment.length > 0)
      .map((segment) => decodeURIComponent(segment));
    if (request.method === "POST" && segments.join("/") === "v1/sandboxes") {
      const createBody = recordBody(JSON.parse(body));
      this.createBodies.push(createBody);
      if (this.createFailure !== null) {
        return jsonResponse(
          { code: this.createFailure.code, message: this.createFailure.message },
          this.createFailure.status,
        );
      }
      const id = `sb-${this.#nextSandbox}`;
      this.#nextSandbox += 1;
      this.#files.set(id, new Map());
      this.#sandboxStatuses.set(id, "running");
      return jsonResponse({ id, name: id }, 201);
    }
    if (
      request.method === "GET" &&
      segments.length === 3 &&
      segments[0] === "v1" &&
      segments[1] === "sandboxes"
    ) {
      const id = segments[2];
      const status = this.#sandboxStatuses.get(id);
      if (status === undefined) {
        return jsonResponse({ code: "not_found", message: `unknown sandbox ${id}` }, 404);
      }
      return jsonResponse({ id, name: id, status });
    }
    if (segments.length !== 4 || segments[0] !== "v1" || segments[1] !== "sandboxes") {
      return jsonResponse({ code: "not_found", message: "unknown route" }, 404);
    }

    const sandboxId = segments[2];
    const operation = segments[3];
    if (request.method === "PUT" && operation === "files") {
      const path = url.searchParams.get("path");
      const files = this.#files.get(sandboxId);
      if (path === null || files === undefined) {
        return jsonResponse({ code: "not_found", message: "missing sandbox or path" }, 404);
      }
      files.set(path, body);
      if (path === "/tmp/vmon-remote-function-runner.mjs") {
        this.runnerSource = body;
      }
      return jsonResponse({ ok: true });
    }
    if (request.method === "DELETE" && operation === "files") {
      const path = url.searchParams.get("path");
      const files = this.#files.get(sandboxId);
      if (path === null || files === undefined) {
        return jsonResponse({ code: "not_found", message: "missing sandbox or path" }, 404);
      }
      files.delete(path);
      return jsonResponse({ ok: true });
    }
    if (request.method === "POST" && operation === "terminate") {
      this.terminatedSandboxIds.push(sandboxId);
      const status = this.#sandboxStatuses.get(sandboxId);
      const response =
        status === undefined
          ? jsonResponse({ code: "not_found", message: `unknown sandbox ${sandboxId}` }, 404)
          : jsonResponse({ ok: true });
      this.terminationResponses.push(response);
      if (status !== undefined) {
        this.#sandboxStatuses.set(sandboxId, "terminated");
      }
      return response;
    }
    if (request.method === "POST" && operation === "exec") {
      return this.#exec(sandboxId, JSON.parse(body));
    }
    return jsonResponse({ code: "not_found", message: "unknown operation" }, 404);
  };

  setSandboxStatus(sandboxId: string, status: string): void {
    if (!this.#sandboxStatuses.has(sandboxId)) {
      throw new Error(`unknown fake sandbox ${sandboxId}`);
    }
    this.#sandboxStatuses.set(sandboxId, status);
  }

  forgetSandbox(sandboxId: string): void {
    this.#sandboxStatuses.delete(sandboxId);
    this.#files.delete(sandboxId);
  }

  gateInvocation(value: JsonValue, gate: Gate): void {
    this.#gates.set(JSON.stringify(value), gate);
  }

  waitForInvocations(count: number): Promise<void> {
    if (this.invocationArguments.length >= count) {
      return Promise.resolve();
    }
    const { promise, resolve } = Promise.withResolvers<void>();
    this.#arrivalWaiters.push({ count, resolve });
    return promise;
  }

  waitForCompletions(count: number): Promise<void> {
    if (this.completedArguments.length >= count) {
      return Promise.resolve();
    }
    const { promise, resolve } = Promise.withResolvers<void>();
    this.#completionWaiters.push({ count, resolve });
    return promise;
  }

  async #exec(sandboxId: string, body: unknown): Promise<Response> {
    if (!isRecord(body) || !isStringArray(body.cmd)) {
      return jsonResponse({ code: "invalid_request", message: "cmd must be strings" }, 400);
    }
    if (body.cmd[0] === "node" && body.cmd[1] === "--version") {
      return captureResponse(0, "v22.0.0\n", "");
    }
    if (body.cmd[0] !== "node" || body.cmd.length !== 3) {
      return captureResponse(127, "", "unsupported command");
    }
    const files = this.#files.get(sandboxId);
    const payload = files?.get(body.cmd[2]);
    if (payload === undefined) {
      return captureResponse(1, "", "missing invocation payload");
    }
    const response = await this.#runInvocation(invocationBody(JSON.parse(payload)));
    return captureResponse(0, JSON.stringify(response), "");
  }

  async #runInvocation(invocation: RemoteFunctionInvocation): Promise<RemoteFunctionResponse> {
    this.invocationArguments.push(invocation.args);
    notifyWaiters(this.#arrivalWaiters, this.invocationArguments.length);
    const gate = this.#gates.get(JSON.stringify(invocation.args[0]));
    if (gate !== undefined) {
      await gate.wait();
    }

    let namespace: Record<string, unknown> | null = null;
    try {
      this.#moduleSequence += 1;
      const moduleSource = `
const __vmonFakeStdout = [];
const console = {
  log(...values) {
    __vmonFakeStdout.push(values.map((value) => String(value)).join(" ") + "\\n");
  },
};
${invocation.source}
export { __vmonFakeStdout };
//# sourceURL=vmon-fake-${this.#moduleSequence}.mjs
`;
      const moduleUrl = `data:text/javascript;base64,${Buffer.from(moduleSource).toString("base64")}`;
      // The invocation supplies the module at runtime, so a static import cannot identify it.
      const loaded: unknown = await import(moduleUrl);
      if (!isRecord(loaded)) {
        throw new TypeError("remote module namespace was not an object");
      }
      namespace = loaded;
      const handler = namespace[invocation.exportName];
      if (typeof handler !== "function") {
        throw new TypeError(`missing exported handler ${invocation.exportName}`);
      }
      const result: unknown = await handler(...invocation.args);
      return {
        ok: true,
        result: fakeJsonValue(result, "remote function result", new Set()),
        stdout: namespaceStdout(namespace),
      };
    } catch (error) {
      return {
        ok: false,
        error: failureDetails(error),
        stdout: namespaceStdout(namespace),
      };
    } finally {
      this.completedArguments.push(invocation.args);
      notifyWaiters(this.#completionWaiters, this.completedArguments.length);
    }
  }
}

function clientFor(server: FakeVmonServer): VmonClient {
  return new VmonClient({
    baseUrl: "https://vmon.test/root/..",
    token: "test-token",
    fetch: server.fetch,
  });
}

test("remote lazily reuses one sandbox and terminate is idempotent", async () => {
  const server = new FakeVmonServer();
  const client = clientFor(server);
  const add = client.remoteFunction(function add(left: number, right: number) {
    return { sum: left + right };
  });

  expect(server.createBodies).toHaveLength(0);
  await expect(add.remote(2, 5)).resolves.toEqual({ sum: 7 });
  await expect(add.remote(10, 4)).resolves.toEqual({ sum: 14 });

  expect(server.createBodies).toHaveLength(1);
  expect(server.createBodies[0]?.image).toBe(DEFAULT_REMOTE_FUNCTION_IMAGE);
  expect(server.invocationArguments).toEqual([
    [2, 5],
    [10, 4],
  ]);
  expect(server.requests.every((request) => request.authorization === "Bearer test-token")).toBe(
    true,
  );
  expect(
    server.requests.some(
      (request) =>
        request.method === "PUT" &&
        request.path.includes("path=%2Ftmp%2Fvmon-remote-function-runner.mjs"),
    ),
  ).toBe(true);

  await Promise.all([add.terminate(), add.terminate()]);
  await add.terminate();
  expect(server.terminatedSandboxIds).toEqual(["sb-1"]);
  expect(server.terminationResponses.every((response) => response.bodyUsed)).toBe(true);
});

test("remote reprovisions when its cached sandbox is terminal or missing", async () => {
  const server = new FakeVmonServer();
  const increment = clientFor(server).remoteFunction((value: number) => value + 1);

  await expect(increment.remote(1)).resolves.toBe(2);
  server.setSandboxStatus("sb-1", "stopped");
  await expect(increment.remote(2)).resolves.toBe(3);
  server.forgetSandbox("sb-2");
  await expect(increment.remote(3)).resolves.toBe(4);
  await increment.terminate();

  expect(server.createBodies).toHaveLength(3);
  expect(server.terminatedSandboxIds).toEqual(["sb-1", "sb-2", "sb-3"]);
  expect(server.terminationResponses.every((response) => response.bodyUsed)).toBe(true);
  expect(
    server.requests
      .filter((request) => request.method === "GET")
      .map((request) => request.path),
  ).toEqual(["/v1/sandboxes/sb-1", "/v1/sandboxes/sb-2"]);
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
  expect(server.invocationArguments).toEqual([[9]]);
  await sourceFunction.terminate();
});

test("uploaded guest runner emits the documented stdout and error envelope", async () => {
  const server = new FakeVmonServer();
  const remote = clientFor(server).remoteFunction((value: number) => value);
  let temporaryDirectory: string | null = null;

  try {
    await remote.remote(0);
    const runnerSource = server.runnerSource;
    if (runnerSource === null) {
      throw new Error("runner source was not uploaded");
    }
    temporaryDirectory = await mkdtemp(join(tmpdir(), "vmon-ts-runner-"));
    const runnerPath = join(temporaryDirectory, "runner.mjs");
    const payloadPath = join(temporaryDirectory, "payload.json");
    await Bun.write(runnerPath, runnerSource);
    await Bun.write(
      payloadPath,
      JSON.stringify({
        source:
          "export function handle(value) { console.log('runner value', value); throw new TypeError('runner boom'); }",
        exportName: "handle",
        args: [7],
      }),
    );

    const nodePath = Bun.which("node");
    if (nodePath === null) {
      throw new Error("local Node.js is required to validate the guest runner");
    }
    const child = Bun.spawn([nodePath, runnerPath, payloadPath], {
      stdout: "pipe",
      stderr: "pipe",
    });
    const [exitCode, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    expect(exitCode).toBe(0);
    expect(stderr).toBe("");
    const response: unknown = JSON.parse(stdout);
    if (!isRecord(response) || response.ok !== false || !isRecord(response.error)) {
      throw new Error(`invalid runner response: ${stdout}`);
    }
    expect(response.stdout).toBe("runner value 7\n");
    expect(response.error.type).toBe("TypeError");
    expect(response.error.message).toBe("runner boom");
    expect(response.error.stack).toContain("runner boom");
  } finally {
    await remote.terminate();
    if (temporaryDirectory !== null) {
      await rm(temporaryDirectory, { recursive: true, force: true });
    }
  }
});

test("daemon errors retain status and structured codes during provisioning", async () => {
  const server = new FakeVmonServer();
  server.createFailure = {
    status: 503,
    code: "capacity_exhausted",
    message: "no VM slots",
  };
  const remote = clientFor(server).remoteFunction((value: number) => value);

  try {
    await remote.remote(1);
    throw new Error("expected sandbox provisioning to fail");
  } catch (error) {
    expect(error).toBeInstanceOf(VmonApiError);
    if (!(error instanceof VmonApiError)) {
      throw error;
    }
    expect(error.status).toBe(503);
    expect(error.code).toBe("capacity_exhausted");
    expect(error.message).toBe("no VM slots");
  }
});

test("map preserves input order with a bounded ephemeral pool", async () => {
  const server = new FakeVmonServer();
  const gates = [new Gate(), new Gate(), new Gate()];
  server.gateInvocation(1, gates[0]);
  server.gateInvocation(2, gates[1]);
  server.gateInvocation(3, gates[2]);
  const double = clientFor(server).remoteFunction(async function double(value: number) {
    return value * 2;
  });

  const mapped = double.map([1, 2, 3], { concurrency: 3 });
  await server.waitForInvocations(3);
  gates[1].open();
  await server.waitForCompletions(1);
  gates[2].open();
  await server.waitForCompletions(2);
  gates[0].open();

  await expect(mapped).resolves.toEqual([2, 4, 6]);
  expect(server.completedArguments.map((args) => args[0])).toEqual([2, 3, 1]);
  expect(server.createBodies).toHaveLength(3);
  expect(server.terminatedSandboxIds).toEqual(["sb-1", "sb-2", "sb-3"]);
});

test("map can return explicit completion order", async () => {
  const server = new FakeVmonServer();
  const gates = [new Gate(), new Gate(), new Gate()];
  server.gateInvocation(1, gates[0]);
  server.gateInvocation(2, gates[1]);
  server.gateInvocation(3, gates[2]);
  const double = clientFor(server).remoteFunction(async function double(value: number) {
    return value * 2;
  });

  const mapped = double.map([1, 2, 3], { concurrency: 3, ordered: false });
  await server.waitForInvocations(3);
  gates[1].open();
  await server.waitForCompletions(1);
  gates[2].open();
  await server.waitForCompletions(2);
  gates[0].open();

  await expect(mapped).resolves.toEqual([4, 6, 2]);
  expect(server.terminatedSandboxIds).toHaveLength(3);
});

test("map stops scheduling after failures and cleans every provisioned worker", async () => {
  const server = new FakeVmonServer();
  const explode = clientFor(server).remoteFunction(function explode(value: number) {
    throw new Error(`boom ${value}`);
  });

  await expect(explode.map([1, 2, 3, 4], { concurrency: 2 })).rejects.toBeInstanceOf(
    RemoteFunctionError,
  );
  expect(server.invocationArguments).toHaveLength(2);
  expect(server.terminatedSandboxIds).toEqual(["sb-1", "sb-2"]);
  expect(server.createBodies).toHaveLength(2);
  await expect(explode.map([1], { concurrency: 0 })).rejects.toThrow(
    "concurrency must be an integer >= 1",
  );
  expect(server.createBodies).toHaveLength(2);
});

test("serialization rejects lossy values before sandbox creation", async () => {
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
  await expect(echo.remote(cyclic)).rejects.toThrow("remote function arguments[0].self contains a cycle");
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

test("stdout and structured guest errors propagate to the caller", async () => {
  const server = new FakeVmonServer();
  const stdout: string[] = [];
  const explode = clientFor(server).remoteFunction(
    function explode(value: number): never {
      console.log("guest value", value);
      throw new RangeError(`bad value ${value}`);
    },
    { onStdout: (output) => stdout.push(output) },
  );

  try {
    await explode.remote(7);
    throw new Error("expected the remote function to fail");
  } catch (error) {
    expect(error).toBeInstanceOf(RemoteFunctionError);
    if (!(error instanceof RemoteFunctionError)) {
      throw error;
    }
    expect(error.message).toBe("bad value 7");
    expect(error.remoteType).toBe("RangeError");
    expect(error.remoteStack).toContain("bad value 7");
  }
  expect(stdout).toEqual(["guest value 7\n"]);
  await explode.terminate();
});

test("missing captures and non-JSON results remain structured remote errors", async () => {
  const server = new FakeVmonServer();
  const closureCallable = (() => {
    const hiddenCapture = { value: 11 };
    return (value: number) => value + hiddenCapture.value;
  })();
  const closure = clientFor(server).remoteFunction(closureCallable);

  try {
    await closure.remote(1);
    throw new Error("expected the missing capture to fail remotely");
  } catch (error) {
    expect(error).toBeInstanceOf(RemoteFunctionError);
    if (!(error instanceof RemoteFunctionError)) {
      throw error;
    }
    expect(error.remoteType).toBe("ReferenceError");
    expect(error.remoteStack).toContain("hiddenCapture");
  }
  await closure.terminate();

  const badResult = clientFor(server).remoteFunction(() => new Date(0));
  try {
    await badResult.remote();
    throw new Error("expected the result serialization to fail remotely");
  } catch (error) {
    expect(error).toBeInstanceOf(RemoteFunctionError);
    if (!(error instanceof RemoteFunctionError)) {
      throw error;
    }
    expect(error.remoteType).toBe("TypeError");
    expect(error.message).toContain("non-plain object");
  }
  await badResult.terminate();
});

function invocationBody(value: unknown): RemoteFunctionInvocation {
  if (
    !isRecord(value) ||
    typeof value.source !== "string" ||
    typeof value.exportName !== "string" ||
    !Array.isArray(value.args)
  ) {
    throw new TypeError("invalid invocation body");
  }
  return {
    source: value.source,
    exportName: value.exportName,
    args: value.args.map((item, index) => fakeJsonValue(item, `args[${index}]`, new Set())),
  };
}

function fakeJsonValue(value: unknown, path: string, active: Set<object>): JsonValue {
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
        result.push(fakeJsonValue(value[index], `${path}[${index}]`, active));
      }
      return result;
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError(`${path} contains a non-plain object`);
    }
    const result: Record<string, JsonValue> = {};
    const descriptors = Object.getOwnPropertyDescriptors(value);
    for (const key of Object.keys(value).sort()) {
      const descriptor = descriptors[key];
      if (!("value" in descriptor)) {
        throw new TypeError(`${path}.${key} is an accessor property`);
      }
      result[key] = fakeJsonValue(descriptor.value, `${path}.${key}`, active);
    }
    return result;
  } finally {
    active.delete(value);
  }
}

function namespaceStdout(namespace: Record<string, unknown> | null): string {
  const output = namespace?.__vmonFakeStdout;
  if (!Array.isArray(output) || !output.every((value) => typeof value === "string")) {
    return "";
  }
  return output.join("");
}

function failureDetails(error: unknown): RemoteFunctionFailureDetails {
  if (error instanceof Error) {
    return { type: error.name || "Error", message: error.message, stack: error.stack ?? "" };
  }
  return { type: "RemoteError", message: String(error), stack: "" };
}

function notifyWaiters(waiters: CountWaiter[], count: number): void {
  for (let index = waiters.length - 1; index >= 0; index -= 1) {
    if (count >= waiters[index].count) {
      waiters[index].resolve();
      waiters.splice(index, 1);
    }
  }
}

function recordBody(value: unknown): Record<string, unknown> {
  if (!isRecord(value)) {
    throw new TypeError("expected a JSON object body");
  }
  return value;
}

function jsonResponse(body: unknown, status = 200): Response {
  return Response.json(body, { status });
}

function captureResponse(exit: number, stdout: string, stderr: string): Response {
  return jsonResponse({
    exit,
    stdout_b64: Buffer.from(stdout).toString("base64"),
    stderr_b64: Buffer.from(stderr).toString("base64"),
  });
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
