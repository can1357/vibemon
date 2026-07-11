import type { MessageInitShape } from "@bufbuild/protobuf";
import type { Client } from "./client";
import type {
  AppRevision,
  ArtifactRef,
  CallError,
  CallEvent,
  CallGraph,
  CallRecord,
  CallResult,
  CallStats,
  RevisionRef,
  CreateCallRequestSchema,
  FunctionSelectorSchema,
  AppSelectorSchema,
  PutArtifactRequestSchema,
  StreamCallInputsRequestSchema,
  ValueEnvelope,
} from "./gen/vmon/v1/api_pb";
import {
  ArtifactService,
  CallService,
  CallStatus,
  CallType,
  ClientCancellationPolicy,
  FunctionService,
  LogStream,
} from "./gen/vmon/v1/api_pb";
import type {
  EncodeValueOptions,
  PortableValue,
  ValueArtifactStore,
} from "./function-values";
import { decodeValue, encodeValue } from "./function-values";

/** Converts application values to and from the portable JSON/CBOR data model. */
export interface FunctionValueAdapter<Value> {
  toPortable(value: Value): PortableValue;
  fromPortable(value: PortableValue): Value;
}
/** Per-call settings copied into the durable call record. */
export interface FunctionCallOptions extends EncodeValueOptions {
  labels?: Readonly<Record<string, string>>;
  resultTtlMillis?: number;
  cancelOnDisconnect?: boolean;
  clientSessionId?: string;
  signal?: AbortSignal;
}
/** Function lookup settings. */
export interface FunctionLookupOptions<Input = PortableValue, Result = PortableValue>
  extends FunctionCallOptions {
  namespace?: string;
  revisionId?: string;
  input?: FunctionValueAdapter<Input>;
  result?: FunctionValueAdapter<Result>;
}
/** Cursor used to reconnect to a durable event stream. */
export interface EventCursor { afterSequence?: bigint; follow?: boolean; }
/** A decoded durable worker log entry. */
export interface FunctionLog {
  sequence: bigint;
  createdAtUnixMillis: bigint;
  stream: "stdout" | "stderr" | "structured" | "unknown";
  data: Uint8Array;
  text: string;
  inputId?: string;
  inputIndex?: bigint;
  attempt?: number;
}
/** One decoded durable result with stable input/yield correlation metadata. */
export interface FunctionResult<Value> {
  value: Value;
  index: bigint;
  inputId: string;
  inputIndex: bigint;
  yieldIndex?: bigint;
  sequence: bigint;
  createdAtUnixMillis: bigint;
}

/** Structured execution failure returned by the call service. */
export class FunctionExecutionError extends Error {
  override readonly name = "FunctionExecutionError";
  readonly code: string;
  readonly remoteType: string;
  readonly retryable: boolean;
  readonly frames: readonly { file: string; line: number; functionName: string }[];
  readonly details: Readonly<Record<string, string>>;
  constructor(failure: CallError) {
    super(failure.message, failure.causePresence.case === "cause"
      ? { cause: new FunctionExecutionError(failure.causePresence.value) }
      : undefined);
    this.code = failure.code;
    this.remoteType = failure.type;
    this.retryable = failure.retryable;
    this.frames = failure.frames.map((frame) => ({
      file: frame.file,
      line: frame.line,
      functionName: frame.function,
    }));
    this.details = { ...failure.details };
  }
}

class DriverArtifactStore implements ValueArtifactStore {
  constructor(readonly client: Client) {}
  async put(data: Uint8Array, mediaType: string): Promise<ArtifactRef> {
    if (!this.client.driver.clientStream) throw new Error("driver does not support client-streaming artifact uploads");
    const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(data)));
    const response = await this.client.driver.clientStream(
      ArtifactService.method.put,
      (async function* (): AsyncGenerator<MessageInitShape<typeof PutArtifactRequestSchema>> {
        yield {
          frame: {
            case: "header",
            value: {
              expectedDigest: { algorithm: 1, value: digest },
              expectedSizeBytes: BigInt(data.byteLength),
              mediaTypePresence: { case: "mediaType", value: mediaType },
            },
          },
        };
        yield { frame: { case: "data", value: data } };
      })(),
    );
    if (!response.message.ref) throw new Error("artifact upload returned no reference");
    return response.message.ref;
  }
  async get(ref: ArtifactRef): Promise<Uint8Array> {
    const handle = await this.client.driver.serverStream(ArtifactService.method.get, {
      artifact: ref,
      rangePresence: { case: undefined },
    });
    const chunks: Uint8Array[] = [];
    let size = 0;
    for await (const chunk of handle.stream) {
      chunks.push(chunk.data);
      size += chunk.data.byteLength;
    }
    const output = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) { output.set(chunk, offset); offset += chunk.byteLength; }
    return output;
  }
}

interface BoundValueCodec<Value> {
  encode(value: Value): Promise<ValueEnvelope>;
  decode(value: ValueEnvelope): Promise<Value>;
  withOptions(options: FunctionCallOptions): BoundValueCodec<Value>;
}
function bindCodec<Value>(
  store: ValueArtifactStore,
  options: FunctionCallOptions,
  adapter: FunctionValueAdapter<Value>,
): BoundValueCodec<Value> {
  return {
    encode(value: Value): Promise<ValueEnvelope> {
      return encodeValue(adapter.toPortable(value), {
        serializer: options.serializer,
        compression: options.compression,
        artifactStore: store,
        inlineLimit: options.inlineLimit,
        typeName: options.typeName,
      });
    },
    async decode(value: ValueEnvelope): Promise<Value> {
      return adapter.fromPortable(await decodeValue(value, { artifactStore: store }));
    },
    withOptions(next: FunctionCallOptions): BoundValueCodec<Value> {
      return bindCodec(store, next, adapter);
    },
  };
}
const portableAdapter: FunctionValueAdapter<PortableValue> = {
  toPortable(value) { return value; },
  fromPortable(value) { return value; },
};

/** Durable handle for one unary function invocation. Reconstruct with {@link FunctionCall.fromId}. */
export class FunctionCall<Result = PortableValue> {
  readonly id: string;
  readonly #client: Client;
  readonly #codec: BoundValueCodec<Result>;
  readonly #clientSessionId?: string;
  constructor(client: Client, id: string, codec: BoundValueCodec<Result>, clientSessionId?: string) {
    if (!id) throw new TypeError("call id cannot be empty");
    this.#client = client;
    this.id = id;
    this.#codec = codec;
    this.#clientSessionId = clientSessionId;
  }
  /** Reconnect to a durable call from another process. */
  static fromId(client: Client, id: string): FunctionCall<PortableValue> {
    const store = new DriverArtifactStore(client);
    return new FunctionCall(client, id, bindCodec(store, {}, portableAdapter));
  }
  /** Await several durable calls concurrently. Gathering intentionally materializes its finite input. */
  static gather<Result>(calls: Iterable<FunctionCall<Result>>): Promise<Result[]>;
  static gather<Result>(calls: Iterable<FunctionCall<Result>>, options: { returnExceptions: true }): Promise<(Result | Error)[]>;
  static async gather<Result>(
    calls: Iterable<FunctionCall<Result>>,
    options: { returnExceptions?: boolean } = {},
  ): Promise<(Result | Error)[]> {
    const pending = Array.from(calls, async (call): Promise<Result | Error> => {
      try { return await call.get(); }
      catch (error) {
        if (!options.returnExceptions) throw error;
        return error instanceof Error ? error : new Error(String(error));
      }
    });
    return Promise.all(pending);
  }
  /** Return the latest durable call record. */
  async status(): Promise<CallRecord> {
    return (await this.#client.driver.call(CallService.method.get, { callId: this.id })).message;
  }
  /** Return accumulated execution statistics. */
  async stats(): Promise<CallStats | undefined> { return (await this.status()).stats; }
  /** Return durable parent/root relationships. */
  async graph(): Promise<CallGraph | undefined> { return (await this.status()).graph; }
  /** Request durable cancellation. */
  async cancel(reason = "cancelled by client"): Promise<CallRecord> {
    return (await this.#client.driver.call(CallService.method.cancel, {
      call: { callId: this.id }, reason, requestId: crypto.randomUUID(),
    })).message;
  }
  /** Stream reconnectable durable events after the supplied sequence. */
  async *events(cursor: EventCursor = {}): AsyncGenerator<CallEvent> {
    const follow = cursor.follow ?? true;
    const handle = await this.#client.driver.serverStream(CallService.method.watch, {
      cursor: { call: { callId: this.id }, afterSequence: cursor.afterSequence ?? 0n },
      follow,
      clientSessionIdPresence: follow && this.#clientSessionId
        ? { case: "clientSessionId", value: this.#clientSessionId }
        : { case: undefined },
    });
    try { for await (const event of handle.stream) yield event; }
    finally { handle.cancel(); }
  }
  /** Follow decoded stdout, stderr, and structured log events. */
  async *logs(cursor: EventCursor = {}): AsyncGenerator<FunctionLog> {
    for await (const event of this.events(cursor)) {
      if (event.payload.case !== "log") continue;
      const stream = event.payload.value.stream === LogStream.STDOUT ? "stdout"
        : event.payload.value.stream === LogStream.STDERR ? "stderr"
          : event.payload.value.stream === LogStream.STRUCTURED ? "structured" : "unknown";
      yield {
        sequence: event.sequence,
        createdAtUnixMillis: event.createdAtUnixMillis,
        stream,
        data: event.payload.value.data,
        text: new TextDecoder().decode(event.payload.value.data),
        inputId: event.inputIdPresence.case === "inputId" ? event.inputIdPresence.value : undefined,
        inputIndex: event.inputIndexPresence.case === "inputIndex" ? event.inputIndexPresence.value : undefined,
        attempt: event.attemptPresence.case === "attempt" ? event.attemptPresence.value : undefined,
      };
    }
  }
  /** Await and decode the unary result. An AbortSignal requests durable server cancellation. */
  async get(options: { signal?: AbortSignal } = {}): Promise<Result> {
    const signal = options.signal;
    let aborting: Promise<CallRecord> | undefined;
    const onAbort = () => { aborting = this.cancel("aborted by AbortSignal"); };
    if (signal?.aborted) await this.cancel("aborted by AbortSignal");
    else signal?.addEventListener("abort", onAbort, { once: true });
    try {
      for await (const event of this.events()) {
        if (event.payload.case === "result") return (await this.#decodeResult(event.payload.value)).value;
        if (event.payload.case === "error") throw new FunctionExecutionError(event.payload.value);
        if (event.payload.case === "status" && terminal(event.payload.value.status)) {
          const record = await this.status();
          if (record.errorPresence.case === "error") throw new FunctionExecutionError(record.errorPresence.value);
          if (record.status === CallStatus.CANCELLED) throw new FunctionExecutionError(cancelledFailure());
        }
      }
      if (aborting) await aborting;
      const result = (await this.#client.driver.call(CallService.method.getResult, {
        call: { callId: this.id }, index: 0n,
      })).message;
      return (await this.#decodeResult(result)).value;
    } finally { signal?.removeEventListener("abort", onAbort); }
  }
  /** Decode one indexed durable result. */
  async result(index: number): Promise<Result> {
    if (!Number.isSafeInteger(index) || index < 0) throw new RangeError("result index must be a non-negative safe integer");
    const result = (await this.#client.driver.call(CallService.method.getResult, {
      call: { callId: this.id }, index: BigInt(index),
    })).message;
    return (await this.#decodeResult(result)).value;
  }
  /** Decode one indexed result together with stable input and generator-yield correlation. */
  async resultEntry(index: number): Promise<FunctionResult<Result>> {
    if (!Number.isSafeInteger(index) || index < 0) throw new RangeError("result index must be a non-negative safe integer");
    return this.#decodeResult((await this.#client.driver.call(CallService.method.getResult, {
      call: { callId: this.id }, index: BigInt(index),
    })).message);
  }
  async #decodeResult(result: CallResult): Promise<FunctionResult<Result>> {
    if (result.outcome.case === "error") throw new FunctionExecutionError(result.outcome.value);
    if (result.outcome.case !== "value") throw new Error("call result has no outcome");
    return {
      value: await this.#codec.decode(result.outcome.value),
      index: result.index,
      inputId: result.inputId,
      inputIndex: result.inputIndex,
      yieldIndex: result.yieldIndexPresence.case === "yieldIndex" ? result.yieldIndexPresence.value : undefined,
      sequence: result.sequence,
      createdAtUnixMillis: result.createdAtUnixMillis,
    };
  }
}

/** Durable handle for an indexed batch call. */
export class BatchCall<Result = PortableValue> extends FunctionCall<Result> implements AsyncIterable<Result> {
  readonly #submitted: Promise<void>;
  constructor(client: Client, id: string, codec: BoundValueCodec<Result>, submitted: Promise<void> = Promise.resolve(), clientSessionId?: string) {
    super(client, id, codec, clientSessionId);
    this.#submitted = submitted;
  }
  /** Reconnect to a durable batch from another process. */
  static fromId(client: Client, id: string): BatchCall<PortableValue> {
    const store = new DriverArtifactStore(client);
    return new BatchCall(client, id, bindCodec(store, {}, portableAdapter));
  }
  /** Await one result by its input index. */
  override async result(index: number): Promise<Result> { await this.#submitted; return super.result(index); }
  /** Stream results in input order without accumulating an unbounded output list. */
  async *[Symbol.asyncIterator](): AsyncGenerator<Result> {
    await this.#submitted;
    const record = await this.status();
    if (record.inputCount > BigInt(Number.MAX_SAFE_INTEGER)) throw new RangeError("batch input count exceeds JavaScript safe integer range");
    for (let index = 0n; index < record.inputCount; index += 1n) yield await super.result(Number(index));
  }
}

/** A pinned, immutable deployed function revision. */
export class RemoteFunction<Input = PortableValue, Result = PortableValue> {
  readonly revision: RevisionRef;
  readonly #client: Client;
  readonly #input: BoundValueCodec<Input>;
  readonly #result: BoundValueCodec<Result>;
  readonly #options: FunctionCallOptions;
  constructor(client: Client, revision: RevisionRef, input: BoundValueCodec<Input>, result: BoundValueCodec<Result>, options: FunctionCallOptions) {
    this.#client = client; this.revision = revision; this.#input = input; this.#result = result; this.#options = options;
  }
  /** Resolve the active revision of a deployed function. */
  static async fromName<Input, Result>(client: Client, name: string, options: FunctionLookupOptions<Input, Result> & { input: FunctionValueAdapter<Input>; result: FunctionValueAdapter<Result> }): Promise<RemoteFunction<Input, Result>>;
  static async fromName(client: Client, name: string, options?: FunctionLookupOptions): Promise<RemoteFunction>;
  static async fromName<Input, Result>(client: Client, name: string, options: FunctionLookupOptions<Input, Result> = {}): Promise<RemoteFunction<Input, Result> | RemoteFunction> {
    const namespace = options.namespace ?? "default";
    const selector: MessageInitShape<typeof FunctionSelectorSchema> = options.revisionId
      ? { selection: { case: "pinned", value: { function: { namespace, name }, revisionId: options.revisionId } } }
      : { selection: { case: "current", value: { namespace, name } } };
    const revision = (await client.driver.call(FunctionService.method.get, { function: selector })).message.ref;
    if (!revision) throw new Error("function lookup returned no revision reference");
    const store = new DriverArtifactStore(client);
    if (options.input && options.result) return new RemoteFunction(client, revision, bindCodec(store, options, options.input), bindCodec(store, options, options.result), options);
    return new RemoteFunction(client, revision, bindCodec(store, options, portableAdapter), bindCodec(store, options, portableAdapter), options);
  }
  /** Return an immutable handle with different per-call defaults; the revision remains pinned. */
  withOptions(options: FunctionCallOptions): RemoteFunction<Input, Result> {
    const merged = mergeOptions(this.#options, options);
    return new RemoteFunction(this.#client, this.revision, this.#input.withOptions(merged), this.#result.withOptions(merged), merged);
  }
  /** Create a durable unary call and return immediately. */
  async spawn(input: Input, options: FunctionCallOptions = {}): Promise<FunctionCall<Result>> {
    const merged = mergeOptions(this.#options, options);
    const clientSessionId = merged.cancelOnDisconnect
      ? merged.clientSessionId || crypto.randomUUID()
      : undefined;
    const envelope = await this.#input.encode(input);
    rejectCloudpickle(envelope);
    const record = (await this.#client.driver.call(CallService.method.create, createRequest(this.revision, CallType.UNARY, [{ index: 0n, value: envelope, inputId: "" }], true, merged, clientSessionId))).message;
    if (!record.ref) throw new Error("call creation returned no durable id");
    const call = new FunctionCall(this.#client, record.ref.callId, this.#result, clientSessionId);
    bindAbort(call, merged.signal);
    return call;
  }
  /** Invoke a deployed unary function and await its durable result. */
  async remote(input: Input, options: FunctionCallOptions = {}): Promise<Result> {
    const call = await this.spawn(input, options);
    return call.get();
  }
  /** Create a streamed durable batch. Input encoding and submission remain lazy. */
  async spawnMap(inputs: AsyncIterable<Input> | Iterable<Input>, options: FunctionCallOptions = {}): Promise<BatchCall<Result>> {
    const merged = mergeOptions(this.#options, options);
    const clientSessionId = merged.cancelOnDisconnect
      ? merged.clientSessionId || crypto.randomUUID()
      : undefined;
    const record = (await this.#client.driver.call(CallService.method.create, createRequest(this.revision, CallType.BATCH, [], false, merged, clientSessionId))).message;
    if (!record.ref) throw new Error("batch creation returned no durable id");
    const id = record.ref.callId;
    const submitted = this.#submitInputs(id, inputs, merged.signal);
    const call = new BatchCall(this.#client, id, this.#result, submitted, clientSessionId);
    bindAbort(call, merged.signal);
    return call;
  }
  /** Stream mapped results in input order. */
  async *map(inputs: AsyncIterable<Input> | Iterable<Input>, options: FunctionCallOptions = {}): AsyncGenerator<Result> {
    yield* await this.spawnMap(inputs, options);
  }
  async #submitInputs(id: string, inputs: AsyncIterable<Input> | Iterable<Input>, signal?: AbortSignal): Promise<void> {
    if (!this.#client.driver.clientStream) throw new Error("driver does not support streamed batch inputs");
    const codec = this.#input;
    await this.#client.driver.clientStream(CallService.method.streamInputs, (async function* (): AsyncGenerator<MessageInitShape<typeof StreamCallInputsRequestSchema>> {
      yield { frame: { case: "call", value: { callId: id } } };
      let index = 0;
      for await (const input of inputs) {
        if (signal?.aborted) break;
        const value = await codec.encode(input);
        rejectCloudpickle(value);
        yield { frame: { case: "input", value: { index: BigInt(index), value, inputId: "" } } };
        index += 1;
      }
    })());
    const status = await this.#client.driver.call(CallService.method.get, { callId: id });
    await this.#client.driver.call(CallService.method.closeInputs, {
      call: { callId: id }, expectedInputCount: status.message.inputCount,
    });
  }
}

/** A pinned application revision with named function bindings. */
export class App {
  readonly revision: AppRevision;
  readonly #client: Client;
  constructor(client: Client, revision: AppRevision) { this.#client = client; this.revision = revision; }
  /** Resolve the current or an immutable application revision. */
  static async fromName(client: Client, name: string, options: { namespace?: string; revisionId?: string } = {}): Promise<App> {
    const app = { namespace: options.namespace ?? "default", name };
    const selector: MessageInitShape<typeof AppSelectorSchema> = options.revisionId
      ? { selection: { case: "pinned", value: { app, revisionId: options.revisionId } } }
      : { selection: { case: "current", value: app } };
    return new App(client, (await client.driver.call(FunctionService.method.getApp, { app: selector })).message);
  }
  /** Return a portable handle for one pinned application binding. */
  function(name: string, options: FunctionCallOptions = {}): RemoteFunction {
    const binding = this.revision.functions.find((item) => item.name === name);
    if (!binding?.revision) throw new Error(`application has no function binding ${name}`);
    const store = new DriverArtifactStore(this.#client);
    return new RemoteFunction(this.#client, binding.revision, bindCodec(store, options, portableAdapter), bindCodec(store, options, portableAdapter), options);
  }
}

function createRequest(revision: RevisionRef, type: CallType, inputs: { index: bigint; value: ValueEnvelope; inputId: string }[], inputsClosed: boolean, options: FunctionCallOptions, clientSessionId?: string): MessageInitShape<typeof CreateCallRequestSchema> {
  return {
    type,
    target: { function: revision, actorPresence: { case: undefined }, actorMethodPresence: { case: undefined } },
    inputs,
    inputsClosed,
    requestId: crypto.randomUUID(),
    labels: options.labels ? { ...options.labels } : {},
    clientCancellation: options.cancelOnDisconnect ? ClientCancellationPolicy.CANCEL : ClientCancellationPolicy.DETACH,
    resultTtlMillisPresence: options.resultTtlMillis === undefined
      ? { case: undefined }
      : { case: "resultTtlMillis", value: BigInt(options.resultTtlMillis) },
    clientSessionIdPresence: clientSessionId
      ? { case: "clientSessionId", value: clientSessionId }
      : { case: undefined },
  };
}
function mergeOptions(base: FunctionCallOptions, override: FunctionCallOptions): FunctionCallOptions {
  return { ...base, ...override, labels: { ...base.labels, ...override.labels } };
}
function terminal(status: CallStatus): boolean {
  return status === CallStatus.SUCCEEDED || status === CallStatus.FAILED || status === CallStatus.CANCELLED;
}
function rejectCloudpickle(envelope: ValueEnvelope): void {
  if (envelope.serializer === 3) throw new Error("cloudpickle values are Python-only and cannot be sent by TypeScript");
}
function bindAbort<Result>(call: FunctionCall<Result>, signal: AbortSignal | undefined): void {
  if (!signal) return;
  if (signal.aborted) { void call.cancel("aborted by AbortSignal"); return; }
  signal.addEventListener("abort", () => { void call.cancel("aborted by AbortSignal"); }, { once: true });
}
function cancelledFailure(): CallError {
  return {
    $typeName: "vmon.v1.CallError", code: "cancelled", message: "function call was cancelled",
    type: "Cancelled", retryable: false, frames: [], causePresence: { case: undefined }, details: {},
  };
}

/** Alias matching the deployed-function lookup spelling (`Function.fromName`). */
export const Function = RemoteFunction;
