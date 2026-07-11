import { expect, test } from "bun:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { createRouterTransport } from "@connectrpc/connect";
import type { FunctionValueAdapter, PortableValue } from "../src";
import {
  Client,
  FunctionCall,
  FunctionExecutionError,
  MeshDriver,
  decodeValue,
  encodeValue,
} from "../src";
import type { CallError, ValueEnvelope } from "../src/gen/vmon/v1/api_pb";
import {
  CallService,
  CallStatus,
  CallType,
  FunctionService,
  LogStream,
  CallEventSchema,
  CallRecordSchema,
  CreateCallRequestSchema,
  FunctionRevisionSchema,
  GetFunctionRequestSchema,
  WatchCallRequestSchema,
} from "../src/gen/vmon/v1/api_pb";
import { bridgeServer, clientFor } from "./sessions.test";

const numberAdapter: FunctionValueAdapter<number> = {
  toPortable(value) { return value; },
  fromPortable(value) {
    if (typeof value !== "number") throw new TypeError("expected number result");
    return value;
  },
};

interface HarnessState {
  nextId: number;
  cancelCount: number;
  cancelled: Promise<void>;
  createCount: number;
  creatorSessionId?: string;
  watchSessionId?: string;
  lastAfterSequence: bigint;
  inputs: Map<string, PortableValue[]>;
  results: Map<string, ValueEnvelope[]>;
  failures: Map<string, CallError>;
}

function callError(message: string): CallError {
  return {
    $typeName: "vmon.v1.CallError",
    code: "user_error",
    message,
    type: "RangeError",
    retryable: false,
    frames: [{
      $typeName: "vmon.v1.ErrorFrame",
      file: "handler.ts",
      line: 12,
      function: "double",
      codePresence: { case: "code", value: "throw new RangeError()" },
    }],
    causePresence: { case: undefined },
    details: { input: "bad" },
  };
}

function makeClient(): { client: Client; state: HarnessState } {
  let resolveCancellation = () => {};
  const cancelled = new Promise<void>((resolve) => { resolveCancellation = resolve; });
  const state: HarnessState = {
    nextId: 1,
    cancelCount: 0,
    cancelled,
    createCount: 0,
    creatorSessionId: undefined,
    watchSessionId: undefined,
    lastAfterSequence: 0n,
    inputs: new Map(),
    results: new Map(),
    failures: new Map(),
  };
  const transport = createRouterTransport(({ service }) => {
    service(FunctionService, {
      get(request) {
        const selection = request.function?.selection;
        const functionRef = selection?.case === "pinned" ? selection.value.function : selection?.value;
        return {
          ref: {
            function: functionRef,
            revisionId: selection?.case === "pinned" ? selection.value.revisionId : "revision-1",
          },
          status: 1,
        };
      },
      getApp(request) {
        const selection = request.app?.selection;
        const app = selection?.case === "pinned" ? selection.value.app : selection?.value;
        return {
          ref: { app, revisionId: selection?.case === "pinned" ? selection.value.revisionId : "app-revision-1" },
          functions: [{ name: "double", revision: { function: { namespace: app?.namespace ?? "default", name: "double" }, revisionId: "revision-1" } }],
          createdAtUnixMillis: 1n,
        };
      },
    });
    service(CallService, {
      async create(request) {
        state.createCount += 1;
        state.creatorSessionId = request.clientSessionIdPresence.case === "clientSessionId"
          ? request.clientSessionIdPresence.value
          : undefined;
        const id = `call-${state.nextId++}`;
        state.inputs.set(id, []);
        state.results.set(id, []);
        for (const input of request.inputs) {
          if (!input.value) throw new Error("missing input envelope");
          const value = await decodeValue(input.value);
          state.inputs.get(id)?.push(value);
          if (value === "error") state.failures.set(id, callError("bad input"));
          else if (typeof value === "number") state.results.get(id)?.push(await encodeValue(value * 2));
          else state.results.get(id)?.push(await encodeValue(value));
        }
        return {
          ref: { callId: id },
          type: request.type,
          target: request.target,
          status: CallStatus.QUEUED,
          inputsClosed: request.inputsClosed,
          inputCount: BigInt(request.inputs.length),
          resultCount: 0n,
          createdAtUnixMillis: 1n,
          updatedAtUnixMillis: 1n,
          errorPresence: { case: undefined },
          labels: request.labels,
        };
      },
      async streamInputs(requests) {
        let id = "";
        for await (const request of requests) {
          if (request.frame.case === "call") { id = request.frame.value.callId; continue; }
          if (request.frame.case !== "input" || !request.frame.value.value) throw new Error("invalid input frame");
          const value = await decodeValue(request.frame.value.value);
          state.inputs.get(id)?.push(value);
          state.results.get(id)?.push(await encodeValue(typeof value === "number" ? value * 2 : value));
        }
        return { call: { callId: id }, committedInputCount: BigInt(state.inputs.get(id)?.length ?? 0) };
      },
      closeInputs(request) {
        const id = request.call?.callId ?? "";
        const count = BigInt(state.inputs.get(id)?.length ?? 0);
        return {
          ref: { callId: id }, type: CallType.BATCH, status: CallStatus.SUCCEEDED,
          inputsClosed: true, inputCount: count, resultCount: count,
          createdAtUnixMillis: 1n, updatedAtUnixMillis: 2n,
          errorPresence: { case: undefined }, labels: {},
        };
      },
      get(request) {
        const id = request.callId;
        const count = BigInt(state.inputs.get(id)?.length ?? 0);
        const failure = state.failures.get(id);
        return {
          ref: { callId: id }, type: count > 1n ? CallType.BATCH : CallType.UNARY,
          status: failure ? CallStatus.FAILED : CallStatus.SUCCEEDED,
          inputsClosed: true, inputCount: count,
          resultCount: BigInt(state.results.get(id)?.length ?? 0),
          graph: { parentCallIds: ["parent-1"], rootCallIdPresence: { case: "rootCallId", value: "root-1" }, parentInputIds: ["input-1"] },
          createdAtUnixMillis: 1n, updatedAtUnixMillis: 2n,
          errorPresence: failure ? { case: "error", value: failure } : { case: undefined },
          stats: { queueMillis: 1n, startupMillis: 2n, executionMillis: 3n, wallMillis: 6n, cpuMillis: 2n, peakMemoryBytes: 1024n, attempts: [] },
          labels: {},
        };
      },
      getResult(request) {
        const id = request.call?.callId ?? "";
        const index = Number(request.index);
        const value = state.results.get(id)?.[index];
        const failure = state.failures.get(id);
        return {
          call: { callId: id }, index: request.index,
          outcome: failure ? { case: "error", value: failure }
            : value ? { case: "value", value } : { case: undefined },
          createdAtUnixMillis: 2n, sequence: BigInt(index + 1),
          inputId: `input-${index}`, inputIndex: request.index,
          yieldIndexPresence: { case: undefined },
        };
      },
      async *watch(request) {
        state.lastAfterSequence = request.cursor?.afterSequence ?? 0n;
        state.watchSessionId = request.clientSessionIdPresence.case === "clientSessionId"
          ? request.clientSessionIdPresence.value
          : undefined;
        const id = request.cursor?.call?.callId ?? "";
        const failure = state.failures.get(id);
        if (state.lastAfterSequence < 8n) {
          yield {
            call: { callId: id }, sequence: 8n, createdAtUnixMillis: 2n, type: 2,
            payload: { case: "log", value: { stream: LogStream.STDOUT, data: new TextEncoder().encode("hello\n") } },
            inputIdPresence: { case: undefined }, inputIndexPresence: { case: undefined }, attemptPresence: { case: undefined },
          };
        }
        if (failure) {
          yield {
            call: { callId: id }, sequence: 9n, createdAtUnixMillis: 2n, type: 6,
            payload: { case: "error", value: failure },
            inputIdPresence: { case: undefined }, inputIndexPresence: { case: undefined }, attemptPresence: { case: undefined },
          };
          return;
        }
        const value = state.results.get(id)?.[0];
        if (value) yield {
          call: { callId: id }, sequence: 9n, createdAtUnixMillis: 2n, type: 4,
          payload: { case: "result", value: {
            call: { callId: id }, index: 0n, outcome: { case: "value", value },
            createdAtUnixMillis: 2n, sequence: 1n, inputId: "input-0", inputIndex: 0n,
            yieldIndexPresence: { case: undefined },
          } },
          inputIdPresence: { case: "inputId", value: "input-0" }, inputIndexPresence: { case: "inputIndex", value: 0n }, attemptPresence: { case: undefined },
        };
      },
      cancel(request) {
        state.cancelCount += 1;
        resolveCancellation();
        return {
          ref: request.call, type: CallType.UNARY, status: CallStatus.CANCELLED,
          inputsClosed: true, inputCount: 1n, resultCount: 0n,
          createdAtUnixMillis: 1n, updatedAtUnixMillis: 2n,
          errorPresence: { case: undefined }, labels: {},
        };
      },
    });
  });
  const driver = new MeshDriver("http://functions.test", { discover: false, transport: () => transport });
  return { client: new Client(driver), state };
}

test("deployed lookup pins revisions and invokes through FunctionService/CallService only", async () => {
  const { client, state } = makeClient();
  const fn = await client.functions.fromName("double", { namespace: "acme", input: numberAdapter, result: numberAdapter });
  expect(fn.revision.revisionId).toBe("revision-1");
  expect(await fn.remote(21)).toBe(42);
  expect(state.createCount).toBe(1);

  const pinned = await client.functions.fromName("double", { namespace: "acme", revisionId: "revision-pinned", input: numberAdapter, result: numberAdapter });
  expect(pinned.revision.revisionId).toBe("revision-pinned");
  const app = await client.apps.fromName("billing", { namespace: "acme" });
  expect(app.function("double").revision.revisionId).toBe("revision-1");
});

test("durable IDs reconstruct, expose metadata, logs, and resume cursors", async () => {
  const { client, state } = makeClient();
  const fn = await client.functions.fromName("double");
  const spawned = await fn.spawn(5);
  const reconstructed = FunctionCall.fromId(client, spawned.id);
  expect(await reconstructed.get()).toBe(10);
  expect((await reconstructed.stats())?.executionMillis).toBe(3n);
  expect((await reconstructed.graph())?.rootCallIdPresence).toEqual({ case: "rootCallId", value: "root-1" });
  const logs = [];
  for await (const log of reconstructed.logs({ afterSequence: 7n, follow: false })) logs.push(log.text);
  expect(logs).toEqual(["hello\n"]);
  expect(state.lastAfterSequence).toBe(7n);
});

test("streamed batches preserve input order and indexed access", async () => {
  const { client } = makeClient();
  const fn = await client.functions.fromName("double", { input: numberAdapter, result: numberAdapter });
  const batch = await fn.spawnMap([3, 1, 2]);
  const values: number[] = [];
  for await (const value of batch) values.push(value);
  expect(values).toEqual([6, 2, 4]);
  expect(await batch.result(1)).toBe(2);
  expect(await batch.resultEntry(1)).toMatchObject({
    value: 2,
    index: 1n,
    inputId: "input-1",
    inputIndex: 1n,
    yieldIndex: undefined,
    sequence: 2n,
  });
});

test("disconnect cancellation uses a matching creator/watch session capability", async () => {
  const { client, state } = makeClient();
  const fn = await client.functions.fromName("double");
  const call = await fn.spawn(2, { cancelOnDisconnect: true, clientSessionId: "session-1" });
  expect(await call.get()).toBe(4);
  expect(state.creatorSessionId).toBe("session-1");
  expect(state.watchSessionId).toBe("session-1");

  const observer = FunctionCall.fromId(client, call.id);
  for await (const _event of observer.events({ follow: false })) break;
  expect(state.watchSessionId).toBeUndefined();
});

test("AbortSignal reaches durable cancellation and structured errors decode", async () => {
  const { client, state } = makeClient();
  const fn = await client.functions.fromName("double");
  const controller = new AbortController();
  await fn.spawn(1, { signal: controller.signal });
  controller.abort();
  await state.cancelled;
  expect(state.cancelCount).toBe(1);

  const failed = await fn.spawn("error");
  try {
    await failed.get();
    throw new Error("expected failure");
  } catch (error) {
    expect(error).toBeInstanceOf(FunctionExecutionError);
    if (error instanceof FunctionExecutionError) {
      expect(error.code).toBe("user_error");
      expect(error.remoteType).toBe("RangeError");
      expect(error.frames[0]).toEqual({ file: "handler.ts", line: 12, functionName: "double" });
    }
  }
});

test("function lookup and invocation traverse the binary WebSocket bridge without SandboxService", async () => {
  const resultEnvelope = await encodeValue(42);
  let createdInput: ValueEnvelope | undefined;
  const methods: string[] = [];
  const server = bridgeServer({
    "/vmon.v1.FunctionService/Get": {
      onMessage(conn, payload) {
        methods.push("FunctionService.Get");
        const request = fromBinary(GetFunctionRequestSchema, payload);
        const selection = request.function?.selection;
        const functionRef = selection?.case === "pinned" ? selection.value.function : selection?.value;
        conn.sendMessage(toBinary(FunctionRevisionSchema, create(FunctionRevisionSchema, {
          ref: { function: functionRef, revisionId: "bridge-revision" },
        })));
        conn.end();
      },
    },
    "/vmon.v1.CallService/Create": {
      onMessage(conn, payload) {
        methods.push("CallService.Create");
        const request = fromBinary(CreateCallRequestSchema, payload);
        createdInput = request.inputs[0]?.value;
        conn.sendMessage(toBinary(CallRecordSchema, create(CallRecordSchema, {
          ref: { callId: "bridge-call" },
          type: CallType.UNARY,
          status: CallStatus.QUEUED,
          inputsClosed: true,
          inputCount: 1n,
        })));
        conn.end();
      },
    },
    "/vmon.v1.CallService/Watch": {
      onMessage(_conn, payload) {
        methods.push("CallService.Watch");
        fromBinary(WatchCallRequestSchema, payload);
      },
      onHalfClose(conn) {
        conn.sendMessage(toBinary(CallEventSchema, create(CallEventSchema, {
          call: { callId: "bridge-call" },
          sequence: 1n,
          type: 4,
          payload: {
            case: "result",
            value: {
              call: { callId: "bridge-call" },
              outcome: { case: "value", value: resultEnvelope },
              inputId: "input-0",
            },
          },
        })));
        conn.end();
      },
    },
  });
  const fn = await clientFor(server).functions.fromName("double", {
    input: numberAdapter,
    result: numberAdapter,
  });
  expect(await fn.remote(21)).toBe(42);
  if (!createdInput) throw new Error("CallService.Create did not receive an input");
  expect(await decodeValue(createdInput)).toBe(21);
  expect(methods).toEqual(["FunctionService.Get", "CallService.Create", "CallService.Watch"]);
});
