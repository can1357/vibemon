import type { MessageInitShape } from "@bufbuild/protobuf";
import type { Client } from "./client";
import { AsyncQueue } from "./async-queue";
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
  FunctionService,
  LogStream,
} from "./gen/vmon/v1/api_pb";
import type { EncodeValueOptions, PortableValue, ValueArtifactStore } from "./function-values";
import { decodeValue, encodeValue } from "./function-values";

/** Converts application values to and from the portable JSON/CBOR data model. */
export interface FunctionValueAdapter<Value> {
  toPortable(value: Value): PortableValue;
  fromPortable(value: PortableValue): Value;
}
/** Per-call settings copied into the durable call record. */
export interface FunctionCallOptions extends Omit<EncodeValueOptions, "artifactStore"> {
  labels?: Readonly<Record<string, string>>;
  resultTtlMillis?: number;
  signal?: AbortSignal;
}
interface FunctionLookupBaseOptions extends FunctionCallOptions {
  namespace?: string;
  revisionId?: string;
}
/** Function lookup settings. Input and result adapters must be supplied together. */
export type FunctionLookupOptions<
  Input = PortableValue,
  Result = PortableValue,
> = FunctionLookupBaseOptions &
  (
    | { input: FunctionValueAdapter<Input>; result: FunctionValueAdapter<Result> }
    | { input?: never; result?: never }
  );
/** Typed positional and named arguments for deployed callable invocation. */
export interface FunctionArguments<Input> {
  positional?: readonly Input[];
  named?: Readonly<Record<string, Input>>;
}
/** Cursor used to reconnect to a durable event stream. */
export interface EventCursor {
  afterSequence?: bigint;
  follow?: boolean;
}
/** A decoded durable worker log entry. */
export interface FunctionLog {
  sequence: bigint;
  createdAtUnixMillis: bigint;
  stream: "stdout" | "stderr" | "structured" | "unknown";
  data: Uint8Array;
  text: string;
  inputId?: string;
  attemptId?: string;
  inputIndex?: bigint;
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
    super(
      failure.message,
      failure.causePresence.case === "cause"
        ? { cause: new FunctionExecutionError(failure.causePresence.value) }
        : undefined,
    );
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
  #endpoint?: string;
  constructor(
    readonly client: Client,
    endpoint?: string,
  ) {
    this.#endpoint = endpoint;
  }
  setEndpoint(endpoint: string): void {
    this.#endpoint = endpoint;
  }
  async put(data: Uint8Array, mediaType: string): Promise<ArtifactRef> {
    if (!this.client.driver.clientStream)
      throw new Error("driver does not support client-streaming artifact uploads");
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
        for (let offset = 0; offset < data.byteLength; offset += 1024 * 1024) {
          yield { frame: { case: "data", value: data.subarray(offset, offset + 1024 * 1024) } };
        }
      })(),
      { endpoint: this.#endpoint },
    );
    this.#endpoint = response.endpoint;
    if (!response.message.ref) throw new Error("artifact upload returned no reference");
    return response.message.ref;
  }
  async get(ref: ArtifactRef): Promise<Uint8Array> {
    const handle = await this.client.driver.serverStream(
      ArtifactService.method.get,
      {
        artifact: ref,
        rangePresence: { case: undefined },
      },
      { endpoint: this.#endpoint },
    );
    this.#endpoint = handle.endpoint;
    const chunks: Uint8Array[] = [];
    let size = 0;
    let expectedOffset = 0n;
    let sawEof = false;
    try {
      for await (const chunk of handle.stream) {
        if (chunk.offset !== expectedOffset)
          throw new Error("artifact download contained a non-contiguous chunk");
        if (sawEof) throw new Error("artifact download contained data after EOF");
        chunks.push(chunk.data);
        size += chunk.data.byteLength;
        expectedOffset += BigInt(chunk.data.byteLength);
        sawEof = chunk.eof;
      }
    } finally {
      handle.cancel();
    }
    if (!sawEof) throw new Error("artifact download ended without EOF");
    const output = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      output.set(chunk, offset);
      offset += chunk.byteLength;
    }
    if (!ref.digest || ref.digest.algorithm !== 1)
      throw new Error("artifact reference requires a SHA-256 digest");
    const actual = new Uint8Array(await crypto.subtle.digest("SHA-256", output));
    if (actual.byteLength !== ref.digest.value.byteLength)
      throw new Error("artifact digest mismatch");
    let difference = 0;
    for (let index = 0; index < actual.byteLength; index += 1)
      difference |= actual[index] ^ ref.digest.value[index];
    if (difference !== 0) throw new Error("artifact digest mismatch");
    return output;
  }
}

interface BoundValueCodec<Value> {
  encode(value: Value): Promise<ValueEnvelope>;
  decode(value: ValueEnvelope): Promise<Value>;
  setEndpoint(endpoint: string): void;
  withOptions(options: FunctionCallOptions): BoundValueCodec<Value>;
}
function bindCodec<Value>(
  store: DriverArtifactStore,
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
    setEndpoint(endpoint: string): void {
      store.setEndpoint(endpoint);
    },
    withOptions(next: FunctionCallOptions): BoundValueCodec<Value> {
      return bindCodec(store, next, adapter);
    },
  };
}
const portableAdapter: FunctionValueAdapter<PortableValue> = {
  toPortable(value) {
    return value;
  },
  fromPortable(value) {
    return value;
  },
};

/** Durable handle for one unary function invocation. Reconstruct with {@link FunctionCall.fromId}. */
export class FunctionCall<Result = PortableValue> {
  readonly id: string;
  readonly #client: Client;
  readonly #codec: BoundValueCodec<Result>;
  #endpoint?: string;
  constructor(client: Client, id: string, codec: BoundValueCodec<Result>, endpoint?: string) {
    if (!id) throw new TypeError("call id cannot be empty");
    this.#client = client;
    this.id = id;
    this.#codec = codec;
    this.#endpoint = endpoint;
  }
  /** Reconnect to a durable call from another process. */
  static fromId(client: Client, id: string): FunctionCall<PortableValue> {
    const store = new DriverArtifactStore(client);
    return new FunctionCall(client, id, bindCodec(store, {}, portableAdapter));
  }
  /** Await several durable calls concurrently. Gathering intentionally materializes its finite input. */
  static gather<Result>(calls: Iterable<FunctionCall<Result>>): Promise<Result[]>;
  static gather<Result>(
    calls: Iterable<FunctionCall<Result>>,
    options: { returnExceptions: true },
  ): Promise<(Result | Error)[]>;
  static async gather<Result>(
    calls: Iterable<FunctionCall<Result>>,
    options: { returnExceptions?: boolean } = {},
  ): Promise<(Result | Error)[]> {
    const pending = Array.from(calls, async (call): Promise<Result | Error> => {
      try {
        return await call.get();
      } catch (error) {
        if (!options.returnExceptions) throw error;
        return error instanceof Error ? error : new Error(String(error));
      }
    });
    return Promise.all(pending);
  }
  /** Return the latest durable call record. */
  async status(): Promise<CallRecord> {
    if (!this.#endpoint && this.#client.driver.resolveCall) {
      this.#endpoint = await this.#client.driver.resolveCall(this.id);
      this.#codec.setEndpoint(this.#endpoint);
    }
    const response = await this.#client.driver.call(
      CallService.method.get,
      { callId: this.id },
      { endpoint: this.#endpoint },
    );
    this.#endpoint = response.endpoint;
    this.#codec.setEndpoint(response.endpoint);
    return response.message;
  }
  /** Return accumulated execution statistics. */
  async stats(): Promise<CallStats | undefined> {
    return (await this.status()).stats;
  }
  /** Return durable parent/root relationships. */
  async graph(): Promise<CallGraph | undefined> {
    return (await this.status()).graph;
  }
  /** Request durable cancellation. */
  async cancel(reason = "cancelled by client"): Promise<CallRecord> {
    return (
      await this.#client.driver.call(
        CallService.method.cancel,
        {
          call: { callId: this.id },
          reason,
          requestId: crypto.randomUUID(),
        },
        { endpoint: this.#endpoint },
      )
    ).message;
  }
  /** Stream reconnectable durable events after the supplied sequence. */
  async *events(cursor: EventCursor = {}): AsyncGenerator<CallEvent> {
    const handle = await this.#client.driver.serverStream(
      CallService.method.watch,
      {
        cursor: { call: { callId: this.id }, afterSequence: cursor.afterSequence ?? 0n },
        follow: cursor.follow ?? true,
      },
      { endpoint: this.#endpoint },
    );
    this.#endpoint = handle.endpoint;
    try {
      for await (const event of handle.stream) yield event;
    } finally {
      handle.cancel();
    }
  }
  /** Follow decoded stdout, stderr, and structured log events. */
  async *logs(cursor: EventCursor = {}): AsyncGenerator<FunctionLog> {
    for await (const event of this.events(cursor)) {
      if (event.payload.case !== "log") continue;
      const stream =
        event.payload.value.stream === LogStream.STDOUT
          ? "stdout"
          : event.payload.value.stream === LogStream.STDERR
            ? "stderr"
            : event.payload.value.stream === LogStream.STRUCTURED
              ? "structured"
              : "unknown";
      yield {
        sequence: event.sequence,
        attemptId:
          event.attemptIdPresence.case === "attemptId" ? event.attemptIdPresence.value : undefined,
        createdAtUnixMillis: event.createdAtUnixMillis,
        stream,
        data: event.payload.value.data,
        text: new TextDecoder().decode(event.payload.value.data),
        inputId: event.inputIdPresence.case === "inputId" ? event.inputIdPresence.value : undefined,
        inputIndex:
          event.inputIndexPresence.case === "inputIndex"
            ? event.inputIndexPresence.value
            : undefined,
      };
    }
  }
  /** Await and decode the unary result. An AbortSignal requests durable server cancellation. */
  async get(options: { signal?: AbortSignal } = {}): Promise<Result> {
    const signal = options.signal;
    let aborting: Promise<CallRecord> | undefined;
    const onAbort = () => {
      aborting = this.cancel("aborted by AbortSignal");
    };
    if (signal?.aborted) await this.cancel("aborted by AbortSignal");
    else signal?.addEventListener("abort", onAbort, { once: true });
    try {
      for await (const event of this.events()) {
        if (event.payload.case === "result")
          return (await this.decodeResult(event.payload.value)).value;
        if (event.payload.case === "error") throw new FunctionExecutionError(event.payload.value);
        if (event.payload.case === "status" && terminal(event.payload.value.status)) {
          const record = await this.status();
          if (record.errorPresence.case === "error")
            throw new FunctionExecutionError(record.errorPresence.value);
          if (record.status === CallStatus.CANCELLED)
            throw new FunctionExecutionError(cancelledFailure());
        }
      }
      if (aborting) await aborting;
      const result = (
        await this.#client.driver.call(
          CallService.method.getResult,
          {
            call: { callId: this.id },
            index: 0n,
          },
          { endpoint: this.#endpoint },
        )
      ).message;
      return (await this.decodeResult(result)).value;
    } finally {
      signal?.removeEventListener("abort", onAbort);
    }
  }
  /** Decode one indexed durable result. */
  async result(index: number): Promise<Result> {
    if (!Number.isSafeInteger(index) || index < 0)
      throw new RangeError("result index must be a non-negative safe integer");
    const result = (
      await this.#client.driver.call(
        CallService.method.getResult,
        {
          call: { callId: this.id },
          index: BigInt(index),
        },
        { endpoint: this.#endpoint },
      )
    ).message;
    return (await this.decodeResult(result)).value;
  }
  /** Decode one indexed result together with stable input and generator-yield correlation. */
  async resultEntry(index: number): Promise<FunctionResult<Result>> {
    if (!Number.isSafeInteger(index) || index < 0)
      throw new RangeError("result index must be a non-negative safe integer");
    return this.decodeResult(
      (
        await this.#client.driver.call(
          CallService.method.getResult,
          {
            call: { callId: this.id },
            index: BigInt(index),
          },
          { endpoint: this.#endpoint },
        )
      ).message,
    );
  }
  /** Page through committed results by durable sequence without following new execution. */
  async *results(afterSequence = 0n, pageSize = 128): AsyncGenerator<FunctionResult<Result>> {
    if (!Number.isSafeInteger(pageSize) || pageSize <= 0)
      throw new RangeError("pageSize must be a positive safe integer");
    let cursor: { call?: { callId: string }; afterSequence: bigint } = {
      call: { callId: this.id },
      afterSequence,
    };
    while (true) {
      const response = await this.#client.driver.call(
        CallService.method.listResults,
        { cursor, pageSize },
        { endpoint: this.#endpoint },
      );
      this.#endpoint = response.endpoint;
      for (const result of response.message.results) yield await this.decodeResult(result);
      if (response.message.end) return;
      if (!response.message.nextCursor)
        throw new Error("result page omitted its continuation cursor");
      cursor = response.message.nextCursor;
    }
  }
  protected async decodeResult(result: CallResult): Promise<FunctionResult<Result>> {
    if (result.outcome.case === "error") throw new FunctionExecutionError(result.outcome.value);
    if (result.outcome.case !== "value") throw new Error("call result has no outcome");
    return {
      value: await this.#codec.decode(result.outcome.value),
      index: result.index,
      inputId: result.inputId,
      inputIndex: result.inputIndex,
      yieldIndex:
        result.yieldIndexPresence.case === "yieldIndex"
          ? result.yieldIndexPresence.value
          : undefined,
      sequence: result.sequence,
      createdAtUnixMillis: result.createdAtUnixMillis,
    };
  }
}

/** Durable handle for an indexed batch call. */
export class BatchCall<Result = PortableValue>
  extends FunctionCall<Result>
  implements AsyncIterable<Result>
{
  readonly #submitted: Promise<void>;
  constructor(
    client: Client,
    id: string,
    codec: BoundValueCodec<Result>,
    submitted: Promise<void> = Promise.resolve(),
    endpoint?: string,
  ) {
    super(client, id, codec, endpoint);
    this.#submitted = submitted;
  }
  /** Reconnect to a durable batch from another process. */
  static fromId(client: Client, id: string): BatchCall<PortableValue> {
    const store = new DriverArtifactStore(client);
    return new BatchCall(client, id, bindCodec(store, {}, portableAdapter));
  }
  /** Await one result by its input index. */
  override async result(index: number): Promise<Result> {
    await this.#submitted;
    return super.result(index);
  }
  /** Await one result and its stable correlation metadata by input index. */
  override async resultEntry(index: number): Promise<FunctionResult<Result>> {
    await this.#submitted;
    return super.resultEntry(index);
  }
  /** Stream results in input order without accumulating an unbounded output list. */
  async *[Symbol.asyncIterator](): AsyncGenerator<Result> {
    let nextIndex = 0n;
    const pending = new Map<bigint, FunctionResult<Result>>();
    for await (const event of this.events()) {
      if (event.payload.case === "error") throw new FunctionExecutionError(event.payload.value);
      if (event.payload.case === "result") {
        const result = await this.decodeResult(event.payload.value);
        pending.set(result.inputIndex, result);
        while (true) {
          const ready = pending.get(nextIndex);
          if (!ready) break;
          pending.delete(nextIndex);
          nextIndex += 1n;
          yield ready.value;
        }
      }
      if (event.payload.case === "status" && terminal(event.payload.value.status)) break;
    }
    await this.#submitted;
    const record = await this.status();
    if (record.inputCount > BigInt(Number.MAX_SAFE_INTEGER))
      throw new RangeError("batch input count exceeds JavaScript safe integer range");
    while (nextIndex < record.inputCount) {
      const ready = pending.get(nextIndex);
      if (ready) {
        pending.delete(nextIndex);
        yield ready.value;
      } else {
        yield await super.result(Number(nextIndex));
      }
      nextIndex += 1n;
    }
  }
}

/** A pinned, immutable deployed function revision. */
export class RemoteFunction<Input = PortableValue, Result = PortableValue> {
  readonly revision: RevisionRef;
  readonly #client: Client;
  readonly #input: BoundValueCodec<Input>;
  readonly #result: BoundValueCodec<Result>;
  readonly #options: FunctionCallOptions;
  readonly #endpoint?: string;
  constructor(
    client: Client,
    revision: RevisionRef,
    input: BoundValueCodec<Input>,
    result: BoundValueCodec<Result>,
    options: FunctionCallOptions,
    endpoint?: string,
  ) {
    this.#client = client;
    this.revision = revision;
    this.#input = input;
    this.#result = result;
    this.#options = options;
    this.#endpoint = endpoint;
  }
  static async fromName<Input, Result>(
    client: Client,
    name: string,
    options: FunctionLookupOptions<Input, Result> & {
      input: FunctionValueAdapter<Input>;
      result: FunctionValueAdapter<Result>;
    },
  ): Promise<RemoteFunction<Input, Result>>;
  static async fromName(
    client: Client,
    name: string,
    options?: FunctionLookupOptions,
  ): Promise<RemoteFunction>;
  static async fromName<Input, Result>(
    client: Client,
    name: string,
    options: FunctionLookupOptions<Input, Result> = {},
  ): Promise<RemoteFunction<Input, Result> | RemoteFunction> {
    const namespace = options.namespace ?? "default";
    const selector: MessageInitShape<typeof FunctionSelectorSchema> = options.revisionId
      ? {
          selection: {
            case: "pinned",
            value: { function: { namespace, name }, revisionId: options.revisionId },
          },
        }
      : { selection: { case: "current", value: { namespace, name } } };
    const response = await client.driver.call(FunctionService.method.get, { function: selector });
    const revision = response.message.ref;
    if ((options.input === undefined) !== (options.result === undefined)) {
      throw new TypeError("input and result adapters must be supplied together");
    }
    if (!revision) throw new Error("function lookup returned no revision reference");
    const store = new DriverArtifactStore(client, response.endpoint);
    if (options.input && options.result)
      return new RemoteFunction(
        client,
        revision,
        bindCodec(store, options, options.input),
        bindCodec(store, options, options.result),
        options,
        response.endpoint,
      );
    return new RemoteFunction(
      client,
      revision,
      bindCodec(store, options, portableAdapter),
      bindCodec(store, options, portableAdapter),
      options,
      response.endpoint,
    );
  }
  /** Return an immutable handle with different per-call defaults; the revision remains pinned. */
  withOptions(options: FunctionCallOptions): RemoteFunction<Input, Result> {
    const merged = mergeOptions(this.#options, options);
    return new RemoteFunction(
      this.#client,
      this.revision,
      this.#input.withOptions(merged),
      this.#result.withOptions(merged),
      merged,
      this.#endpoint,
    );
  }
  /** Create a durable unary call and return immediately. */
  async spawn(input: Input, options: FunctionCallOptions = {}): Promise<FunctionCall<Result>> {
    const merged = mergeOptions(this.#options, options);
    const inputCodec = this.#input.withOptions(merged);
    const resultCodec = this.#result.withOptions(merged);
    const envelope = await inputCodec.encode(input);
    rejectCloudpickle(envelope);
    const response = await this.#client.driver.call(
      CallService.method.create,
      createRequest(
        this.revision,
        CallType.UNARY,
        [{ index: 0n, payload: { case: "value", value: envelope }, inputId: crypto.randomUUID() }],
        true,
        merged,
      ),
      { endpoint: this.#endpoint },
    );
    if (!response.message.ref) throw new Error("call creation returned no durable id");
    const call = new FunctionCall(
      this.#client,
      response.message.ref.callId,
      resultCodec,
      response.endpoint,
    );
    bindAbort(call, merged.signal);
    return call;
  }
  /** Invoke a deployed unary function and await its durable result. */
  async remote(input: Input, options: FunctionCallOptions = {}): Promise<Result> {
    const call = await this.spawn(input, options);
    return call.get();
  }
  /** Create a durable unary call with structured positional and named arguments. */
  async spawnArguments(
    arguments_: FunctionArguments<Input>,
    options: FunctionCallOptions = {},
  ): Promise<FunctionCall<Result>> {
    const merged = mergeOptions(this.#options, options);
    const inputCodec = this.#input.withOptions(merged);
    const resultCodec = this.#result.withOptions(merged);
    const positional: ValueEnvelope[] = [];
    for (const input of arguments_.positional ?? []) {
      const value = await inputCodec.encode(input);
      rejectCloudpickle(value);
      positional.push(value);
    }
    const named: Record<string, ValueEnvelope> = {};
    if (arguments_.named) {
      for (const name of Object.keys(arguments_.named)) {
        const value = await inputCodec.encode(arguments_.named[name]);
        rejectCloudpickle(value);
        Object.defineProperty(named, name, {
          value,
          enumerable: true,
          configurable: true,
          writable: true,
        });
      }
    }
    const response = await this.#client.driver.call(
      CallService.method.create,
      createRequest(
        this.revision,
        CallType.UNARY,
        [
          {
            index: 0n,
            payload: { case: "arguments", value: { positional, named } },
            inputId: crypto.randomUUID(),
          },
        ],
        true,
        merged,
      ),
      { endpoint: this.#endpoint },
    );
    if (!response.message.ref) throw new Error("call creation returned no durable id");
    const call = new FunctionCall(
      this.#client,
      response.message.ref.callId,
      resultCodec,
      response.endpoint,
    );
    bindAbort(call, merged.signal);
    return call;
  }
  /** Invoke structured positional and named arguments and await the durable result. */
  async remoteArguments(
    arguments_: FunctionArguments<Input>,
    options: FunctionCallOptions = {},
  ): Promise<Result> {
    return (await this.spawnArguments(arguments_, options)).get();
  }
  /** Create a streamed durable batch. Input encoding and submission remain lazy. */
  async spawnMap(
    inputs: AsyncIterable<Input> | Iterable<Input>,
    options: FunctionCallOptions = {},
  ): Promise<BatchCall<Result>> {
    const merged = mergeOptions(this.#options, options);
    const inputCodec = this.#input.withOptions(merged);
    const resultCodec = this.#result.withOptions(merged);
    const response = await this.#client.driver.call(
      CallService.method.create,
      createRequest(this.revision, CallType.BATCH, [], false, merged),
      { endpoint: this.#endpoint },
    );
    if (!response.message.ref) throw new Error("batch creation returned no durable id");
    const id = response.message.ref.callId;
    const submitted = this.#submitInputs(id, inputs, inputCodec, merged.signal, response.endpoint);
    const call = new BatchCall(this.#client, id, resultCodec, submitted, response.endpoint);
    bindAbort(call, merged.signal);
    return call;
  }
  /** Stream mapped results in input order. */
  async *map(
    inputs: AsyncIterable<Input> | Iterable<Input>,
    options: FunctionCallOptions = {},
  ): AsyncGenerator<Result> {
    yield* await this.spawnMap(inputs, options);
  }
  async #submitInputs(
    id: string,
    inputs: AsyncIterable<Input> | Iterable<Input>,
    codec: BoundValueCodec<Input>,
    signal?: AbortSignal,
    endpoint?: string,
  ): Promise<void> {
    const requests = new AsyncQueue<MessageInitShape<typeof StreamCallInputsRequestSchema>>();
    requests.push({ frame: { case: "call", value: { callId: id } } });
    const handle = await this.#client.driver.duplex(CallService.method.streamInputs, requests, {
      endpoint,
    });
    const onAbort = () => {
      requests.end();
      handle.cancel();
    };
    if (signal?.aborted) onAbort();
    else signal?.addEventListener("abort", onAbort, { once: true });
    const acknowledgements = handle.stream[Symbol.asyncIterator]();
    let acknowledged = 0n;
    let sent = 0n;
    let maximum = 1n;
    const sentIds = new Map<bigint, string>();
    const receiveAck = async (): Promise<void> => {
      const next = await acknowledgements.next();
      if (next.done) throw new Error("batch input stream closed before final acknowledgement");
      const committed = next.value.committedInputCount;
      if (committed < acknowledged || committed > sent)
        throw new Error("batch input acknowledgement frontier is invalid");
      if (
        !Number.isSafeInteger(next.value.maxInputsOutstanding) ||
        next.value.maxInputsOutstanding < 1
      ) {
        throw new Error("batch input acknowledgement advertised an invalid outstanding limit");
      }
      if (next.value.lastInputPresence.case === "lastInput") {
        const last = next.value.lastInputPresence.value;
        if (last.inputIndex + 1n !== committed || sentIds.get(last.inputIndex) !== last.inputId) {
          throw new Error("batch input acknowledgement identified the wrong input");
        }
      }
      for (let index = acknowledged; index < committed; index += 1n) sentIds.delete(index);
      acknowledged = committed;
      maximum = BigInt(next.value.maxInputsOutstanding);
    };
    await receiveAck();
    try {
      for await (const input of inputs) {
        if (signal?.aborted) break;
        while (sent - acknowledged >= maximum) await receiveAck();
        const value = await codec.encode(input);
        rejectCloudpickle(value);
        if (signal?.aborted) break;
        const inputId = crypto.randomUUID();
        sentIds.set(sent, inputId);
        requests.push({
          frame: {
            case: "input",
            value: {
              index: sent,
              payload: { case: "value", value },
              inputId,
            },
          },
        });
        sent += 1n;
      }
      requests.end();
      while (acknowledged < sent) await receiveAck();
    } finally {
      requests.end();
      handle.cancel();
      signal?.removeEventListener("abort", onAbort);
    }
    await this.#client.driver.call(
      CallService.method.closeInputs,
      {
        call: { callId: id },
        expectedInputCount: sent,
      },
      { endpoint },
    );
  }
}

/** A pinned application revision with named function bindings. */
export class App {
  readonly revision: AppRevision;
  readonly #client: Client;
  readonly #endpoint?: string;
  constructor(client: Client, revision: AppRevision, endpoint?: string) {
    this.#client = client;
    this.revision = revision;
    this.#endpoint = endpoint;
  }
  /** Resolve the current or an immutable application revision. */
  static async fromName(
    client: Client,
    name: string,
    options: { namespace?: string; revisionId?: string } = {},
  ): Promise<App> {
    const app = { namespace: options.namespace ?? "default", name };
    const selector: MessageInitShape<typeof AppSelectorSchema> = options.revisionId
      ? { selection: { case: "pinned", value: { app, revisionId: options.revisionId } } }
      : { selection: { case: "current", value: app } };
    const response = await client.driver.call(FunctionService.method.getApp, { app: selector });
    return new App(client, response.message, response.endpoint);
  }
  /** Return a portable handle for one pinned application binding. */
  function(name: string, options: FunctionCallOptions = {}): RemoteFunction {
    const binding = this.revision.functions.find((item) => item.name === name);
    if (!binding?.revision) throw new Error(`application has no function binding ${name}`);
    const store = new DriverArtifactStore(this.#client, this.#endpoint);
    return new RemoteFunction(
      this.#client,
      binding.revision,
      bindCodec(store, options, portableAdapter),
      bindCodec(store, options, portableAdapter),
      options,
      this.#endpoint,
    );
  }
}

type CallInputInit = {
  index: bigint;
  inputId: string;
  payload:
    | { case: "value"; value: ValueEnvelope }
    | {
        case: "arguments";
        value: { positional: ValueEnvelope[]; named: Record<string, ValueEnvelope> };
      };
};

function createRequest(
  revision: RevisionRef,
  type: CallType,
  inputs: CallInputInit[],
  inputsClosed: boolean,
  options: FunctionCallOptions,
): MessageInitShape<typeof CreateCallRequestSchema> {
  return {
    type,
    target: { function: revision, receiver: { case: undefined } },
    inputs,
    inputsClosed,
    requestId: crypto.randomUUID(),
    labels: options.labels ? { ...options.labels } : {},
    resultTtlMillisPresence:
      options.resultTtlMillis === undefined
        ? { case: undefined }
        : { case: "resultTtlMillis", value: BigInt(options.resultTtlMillis) },
  };
}
function mergeOptions(
  base: FunctionCallOptions,
  override: FunctionCallOptions,
): FunctionCallOptions {
  return { ...base, ...override, labels: { ...base.labels, ...override.labels } };
}
function terminal(status: CallStatus): boolean {
  return (
    status === CallStatus.SUCCEEDED ||
    status === CallStatus.FAILED ||
    status === CallStatus.CANCELLED
  );
}
function rejectCloudpickle(envelope: ValueEnvelope): void {
  if (envelope.serializer === 3)
    throw new Error("cloudpickle values are Python-only and cannot be sent by TypeScript");
}
/** Alias matching the deployed-function lookup spelling (`Function.fromName`). */
export { RemoteFunction as Function };
function bindAbort<Result>(call: FunctionCall<Result>, signal: AbortSignal | undefined): void {
  if (!signal) return;
  if (signal.aborted) {
    void call.cancel("aborted by AbortSignal");
    return;
  }
  signal.addEventListener(
    "abort",
    () => {
      void call.cancel("aborted by AbortSignal");
    },
    { once: true },
  );
}
function cancelledFailure(): CallError {
  return {
    $typeName: "vmon.v1.CallError",
    code: "cancelled",
    message: "function call was cancelled",
    type: "Cancelled",
    retryable: false,
    frames: [],
    causePresence: { case: undefined },
    details: {},
  };
}
