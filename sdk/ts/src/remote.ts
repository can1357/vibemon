import type { Client, SandboxCreateRequestWithSecrets } from "./client";
import type { JsonValue, SessionOutputSink, WireSourceSpec } from "./remote-session";
import { classifyFailure, RemoteFunctionError, WorkerSession } from "./remote-session";
import type { Sandbox } from "./sandbox";

export type {
  FailureKind,
  JsonPrimitive,
  JsonValue,
  RemoteFailureCode,
  RemoteFunctionErrorDetails,
  SessionOutputSink,
} from "./remote-session";
export {
  isRemoteUserFailure,
  MISSING_NODE_HINT,
  RemoteFunctionError,
  SESSION_RUNNER,
  SESSION_RUNNER_PATH,
} from "./remote-session";

/** A JavaScript function whose argument tuple and result type are retained remotely. */
export type RemoteCallable<Arguments extends unknown[], Result> = (
  ...args: Arguments
) => Result | Promise<Result>;

/** JavaScript module source plus the name of its exported remote handler. */
export interface RemoteFunctionSourceSpec {
  source: string;
  exportName: string;
}

/** Receives one chunk of live remote stdout or stderr text. */
export type RemoteOutputHandler = (output: string) => void;

/** The element type streamed by {@link RemoteFunction.remoteGen}. */
export type RemoteYield<Result> =
  Result extends Generator<infer Item, unknown, unknown>
    ? Item
    : Result extends AsyncGenerator<infer Item, unknown>
      ? Item
      : Result;

/** Options accepted by the {@link Retries} constructor. */
export interface RetriesOptions {
  maxRetries: number;
  backoffCoefficient?: number;
  initialDelay?: number;
  maxDelay?: number;
}

/** Exponential-backoff retry policy for failed remote calls (Modal parity). */
export class Retries {
  readonly maxRetries: number;
  readonly backoffCoefficient: number;
  /** First retry delay in seconds. */
  readonly initialDelay: number;
  /** Retry delay ceiling in seconds. */
  readonly maxDelay: number;

  constructor(options: RetriesOptions) {
    const { maxRetries, backoffCoefficient = 2.0, initialDelay = 1.0, maxDelay = 60.0 } = options;
    if (!Number.isInteger(maxRetries) || maxRetries < 0) {
      throw new RangeError("maxRetries must be an integer >= 0");
    }
    if (!(backoffCoefficient >= 1.0 && backoffCoefficient <= 10.0)) {
      throw new RangeError("backoffCoefficient must be between 1.0 and 10.0");
    }
    if (!(initialDelay >= 0.0 && initialDelay <= 60.0)) {
      throw new RangeError("initialDelay must be between 0.0 and 60.0 seconds");
    }
    if (!(maxDelay >= 1.0 && maxDelay <= 60.0)) {
      throw new RangeError("maxDelay must be between 1.0 and 60.0 seconds");
    }
    this.maxRetries = maxRetries;
    this.backoffCoefficient = backoffCoefficient;
    this.initialDelay = initialDelay;
    this.maxDelay = maxDelay;
  }

  /** Delay in seconds before 1-based retry `retryNumber`. */
  delayFor(retryNumber: number): number {
    return Math.min(
      this.initialDelay * this.backoffCoefficient ** (retryNumber - 1),
      this.maxDelay,
    );
  }
}

/** Sandbox, retry, and output settings for a remote function. */
export interface RemoteFunctionOptions extends SandboxCreateRequestWithSecrets {
  /**
   * Default per-call deadline in seconds; expiry kills the session and raises
   * a timeout error. `timeout` here configures the CALL deadline — set the
   * sandbox TTL with `timeout_secs`. Omitted = no client-side deadline.
   */
  timeout?: number | null;
  /** Retry policy for guest exceptions and timeouts: a count (fixed 1s delay) or {@link Retries}. */
  retries?: number | Retries;
  /** Server-side warm pool size registered for the sandbox template. */
  pool?: number;
  /** Live remote stdout; defaults to writing through to local stdout. */
  onStdout?: RemoteOutputHandler;
  /** Live remote stderr; defaults to writing through to local stderr. */
  onStderr?: RemoteOutputHandler;
  /** Testing hook: replaces the retry-delay sleep. Seconds. */
  retrySleep?: (seconds: number) => Promise<void>;
}

/** Controls the lazy worker pool behind {@link RemoteFunction.map}. */
export interface RemoteMapOptions {
  /** Worker sandbox cap; workers scale up greedily to this. Default 8. */
  concurrency?: number;
  /** Emit results in input order (default) or completion order. */
  orderOutputs?: boolean;
  /** Yield failed items' errors in their slot instead of failing fast. */
  returnExceptions?: boolean;
  /** Per-item deadline in seconds; overrides the function default. */
  timeout?: number;
}

/** Options for {@link RemoteFunction.forEach}. */
export interface RemoteForEachOptions {
  concurrency?: number;
  ignoreExceptions?: boolean;
  timeout?: number;
}

/** Options for {@link FunctionCall.gather}. */
export interface RemoteGatherOptions {
  returnExceptions?: boolean;
}

/** The default image, chosen for its stable Node.js 22 runtime and small Debian base. */
export const DEFAULT_REMOTE_FUNCTION_IMAGE = "node:22-slim";

const CONVENIENCE_EXPORT = "__vmon_handler";
const TERMINAL_SANDBOX_STATUSES = new Set(["stopped", "terminated", "failed"]);
const MAX_INFRA_FAILURES = 3;
const DEFAULT_MAP_CONCURRENCY = 8;

type GeneratorKind = "generator" | "value" | "unknown";
type OutputChunk = [stream: "stdout" | "stderr", text: string];

interface DispatchTarget {
  acquire(): Promise<WorkerSession>;
  invalidate(fresh: boolean): Promise<void>;
}

interface SpawnState {
  cancelled: boolean;
  session: WorkerSession | null;
}

interface MapCompletion<Result> {
  index: number;
  ok: boolean;
  value: Result | undefined;
  error: unknown;
  output: OutputChunk[];
}

function cancelledError(): RemoteFunctionError {
  return new RemoteFunctionError("remote function call was cancelled", { code: "cancelled" });
}

/** A handle for one spawned remote call. */
export class FunctionCall<Result = JsonValue> {
  /** Unique client-side identifier for this call. */
  readonly callId: string = crypto.randomUUID();
  readonly #outcome: Promise<Result>;
  readonly #state: SpawnState;
  readonly #output: OutputChunk[];
  readonly #forward: (chunks: OutputChunk[]) => void;
  #settled = false;
  #replayed = false;

  /** @internal Created by {@link RemoteFunction.spawn}. */
  constructor(
    outcome: Promise<Result>,
    state: SpawnState,
    output: OutputChunk[],
    forward: (chunks: OutputChunk[]) => void,
  ) {
    this.#outcome = outcome;
    this.#state = state;
    this.#output = output;
    this.#forward = forward;
    const settle = () => {
      this.#settled = true;
    };
    outcome.then(settle, settle);
  }

  /** True once the call has produced a result or failed. */
  done(): boolean {
    return this.#settled;
  }

  /**
   * Cancel the call by closing its dedicated exec session; the server
   * SIGTERMs the guest runner. A later {@link get} rejects with a
   * cancellation error. Cancelling a settled call is a no-op.
   */
  cancel(): void {
    if (this.#settled) return;
    this.#state.cancelled = true;
    this.#state.session?.close();
  }

  /**
   * Wait for the result, replaying buffered remote output first. With
   * `timeoutMs` the wait — not the call — is bounded; expiry rejects without
   * cancelling.
   */
  async get(timeoutMs?: number): Promise<Result> {
    if (timeoutMs !== undefined && !this.#settled) {
      await this.#awaitSettled(timeoutMs);
    }
    try {
      const value = await this.#outcome;
      this.#replay();
      return value;
    } catch (error) {
      this.#replay();
      throw error;
    }
  }

  /** Wait for every call in order; failures reject unless `returnExceptions`. */
  static async gather<Result>(
    ...items: (FunctionCall<Result> | RemoteGatherOptions)[]
  ): Promise<(Result | unknown)[]> {
    const calls: FunctionCall<Result>[] = [];
    let options: RemoteGatherOptions = {};
    for (const item of items) {
      if (item instanceof FunctionCall) calls.push(item);
      else options = item;
    }
    const returnExceptions = options.returnExceptions ?? false;
    const results: (Result | unknown)[] = [];
    for (const call of calls) {
      try {
        results.push(await call.get());
      } catch (error) {
        if (!returnExceptions) throw error;
        results.push(error);
      }
    }
    return results;
  }

  async #awaitSettled(timeoutMs: number): Promise<void> {
    const { promise, resolve, reject } = Promise.withResolvers<void>();
    const timer = setTimeout(
      () =>
        reject(
          new RemoteFunctionError(`remote function call was not ready within ${timeoutMs}ms`, {
            code: "timeout",
          }),
        ),
      timeoutMs,
    );
    const settle = () => resolve();
    this.#outcome.then(settle, settle);
    try {
      await promise;
    } finally {
      clearTimeout(timer);
    }
  }

  #replay(): void {
    if (this.#replayed) return;
    this.#replayed = true;
    this.#forward(this.#output);
  }
}

interface ChannelWaiter<T> {
  resolve: (result: IteratorResult<T>) => void;
  reject: (reason: unknown) => void;
}

class AsyncChannel<T> implements AsyncIterable<T> {
  readonly #values: T[] = [];
  readonly #waiters: ChannelWaiter<T>[] = [];
  #done = false;
  #failure: unknown = null;
  #failed = false;

  push(value: T): void {
    if (this.#done || this.#failed) return;
    const waiter = this.#waiters.shift();
    if (waiter) waiter.resolve({ value, done: false });
    else this.#values.push(value);
  }

  end(): void {
    if (this.#done || this.#failed) return;
    this.#done = true;
    for (const waiter of this.#waiters.splice(0)) {
      waiter.resolve({ value: undefined, done: true });
    }
  }

  fail(error: unknown): void {
    if (this.#done || this.#failed) return;
    this.#failed = true;
    this.#failure = error;
    for (const waiter of this.#waiters.splice(0)) waiter.reject(error);
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: (): Promise<IteratorResult<T>> => {
        const value = this.#values.shift();
        if (value !== undefined) return Promise.resolve({ value, done: false });
        if (this.#failed) return Promise.reject(this.#failure);
        if (this.#done) return Promise.resolve({ value: undefined, done: true });
        const { promise, resolve, reject } = Promise.withResolvers<IteratorResult<T>>();
        this.#waiters.push({ resolve, reject });
        return promise;
      },
    };
  }
}

/** A reusable source-serialized function backed by persistent guest sessions. */
export class RemoteFunction<Arguments extends unknown[] = JsonValue[], Result = JsonValue> {
  readonly #client: Client;
  readonly #spec: RemoteFunctionSourceSpec;
  readonly #sandboxRequest: SandboxCreateRequestWithSecrets;
  readonly #retries: Retries | null;
  readonly #timeout: number | null;
  readonly #pool: number;
  readonly #onStdout: RemoteOutputHandler;
  readonly #onStderr: RemoteOutputHandler;
  readonly #sleep: (seconds: number) => Promise<void>;
  readonly #generatorKind: GeneratorKind;
  readonly #spawnSessions = new Set<WorkerSession>();
  #wireSpecPromise: Promise<WireSourceSpec> | null = null;
  #cachedSandbox: Promise<Sandbox> | null = null;
  #sessionPromise: Promise<WorkerSession> | null = null;

  /** @internal Use {@link remoteFunction} or {@link remoteFunctionFromSource}. */
  constructor(
    client: Client,
    spec: RemoteFunctionSourceSpec,
    options: RemoteFunctionOptions = {},
    generatorKind: GeneratorKind = "unknown",
  ) {
    this.#client = client;
    this.#spec = checkedSourceSpec(spec);
    this.#generatorKind = generatorKind;
    const { timeout, retries, pool, onStdout, onStderr, retrySleep, ...sandboxRequest } = options;
    const reusableSecrets =
      sandboxRequest.secrets === undefined || sandboxRequest.secrets === null
        ? sandboxRequest.secrets
        : [...sandboxRequest.secrets];
    this.#sandboxRequest = {
      image: DEFAULT_REMOTE_FUNCTION_IMAGE,
      ...sandboxRequest,
      secrets: reusableSecrets,
    };
    if (timeout !== undefined && timeout !== null && !(timeout > 0)) {
      throw new RangeError("timeout must be a positive number of seconds");
    }
    this.#timeout = timeout ?? null;
    this.#retries = normalizeRetries(retries);
    if (pool !== undefined && (!Number.isInteger(pool) || pool < 0)) {
      throw new RangeError("pool must be an integer >= 0");
    }
    this.#pool = pool ?? 0;
    this.#onStdout = onStdout ?? defaultStdout;
    this.#onStderr = onStderr ?? defaultStderr;
    this.#sleep = retrySleep ?? sleepSeconds;
  }

  /**
   * Run the handler on the warm cached session, streaming remote stdout and
   * stderr live. Dead sandboxes and sessions are recreated transparently.
   */
  async remote(...args: Arguments): Promise<Result> {
    if (this.#generatorKind === "generator") {
      throw new TypeError("a generator function cannot be called with .remote(); use .remoteGen()");
    }
    const jsonArgs = this.#encodeArgs(args);
    const spec = await this.#wireSpec();
    return this.#callWithPolicy(
      this.#cachedTarget(),
      spec,
      jsonArgs,
      this.#timeout,
      this.#liveSink(),
    );
  }

  /**
   * Stream a remote generator's yields as they are produced. Breaking out of
   * the loop (or calling `return()`) kills that session; the next call
   * starts a fresh one.
   */
  async *remoteGen(...args: Arguments): AsyncGenerator<RemoteYield<Result>, void, undefined> {
    if (this.#generatorKind === "value") {
      throw new TypeError(
        "a non-generator function cannot be called with .remoteGen(); use .remote()",
      );
    }
    const jsonArgs = this.#encodeArgs(args);
    const spec = await this.#wireSpec();
    const output = this.#liveSink();
    let infraFailures = 0;
    let retriesUsed = 0;
    while (true) {
      let yielded = false;
      try {
        const session = await this.#ensureSession();
        const stream = session.callIter({
          spec,
          args: jsonArgs,
          timeout: this.#timeout,
          output,
        });
        for await (const item of stream) {
          yielded = true;
          yield item as RemoteYield<Result>;
        }
        return;
      } catch (error) {
        if (yielded) throw error;
        const kind = classifyFailure(error);
        if (kind === "infra" && infraFailures < MAX_INFRA_FAILURES) {
          infraFailures += 1;
          await this.#invalidateCached(true);
          continue;
        }
        if (
          (kind === "user" || kind === "timeout") &&
          this.#retries !== null &&
          retriesUsed < this.#retries.maxRetries
        ) {
          retriesUsed += 1;
          await this.#sleep(this.#retries.delayFor(retriesUsed));
          continue;
        }
        throw error;
      }
    }
  }

  /**
   * Start the call on a dedicated session of the cached sandbox and return a
   * handle immediately. Remote output is buffered and replayed by
   * {@link FunctionCall.get}.
   */
  async spawn(...args: Arguments): Promise<FunctionCall<Result>> {
    if (this.#generatorKind === "generator") {
      throw new TypeError("cannot spawn a generator function; use .remoteGen()");
    }
    const jsonArgs = this.#encodeArgs(args);
    const spec = await this.#wireSpec();
    const state: SpawnState = { cancelled: false, session: null };
    const output: OutputChunk[] = [];
    const target: DispatchTarget = {
      acquire: async () => {
        if (state.cancelled) throw cancelledError();
        if (state.session?.alive) return state.session;
        const sandbox = await this.#ensureSandbox();
        const session = await WorkerSession.start(sandbox);
        state.session = session;
        this.#spawnSessions.add(session);
        if (state.cancelled) {
          session.close();
          throw cancelledError();
        }
        return session;
      },
      invalidate: async (fresh) => {
        if (state.session !== null) {
          this.#spawnSessions.delete(state.session);
          state.session.close();
          state.session = null;
        }
        if (fresh) await this.#invalidateCached(true);
      },
    };
    const outcome = (async () => {
      try {
        return await this.#callWithPolicy(target, spec, jsonArgs, this.#timeout, (stream, text) =>
          output.push([stream, text]),
        );
      } catch (error) {
        if (state.cancelled) throw cancelledError();
        throw error;
      } finally {
        if (state.session !== null) {
          this.#spawnSessions.delete(state.session);
          state.session.close();
          state.session = null;
        }
      }
    })();
    return new FunctionCall<Result>(outcome, state, output, (chunks) =>
      this.#forwardOutput(chunks),
    );
  }

  /**
   * Lazily map unary calls over ephemeral worker sandboxes with greedy
   * scale-up. Results stream from the returned generator; with
   * `returnExceptions` failed items yield their error in-slot, otherwise the
   * first exhausted-retries failure fails fast and tears every worker down.
   */
  map(
    inputs: Iterable<Arguments[0]> | AsyncIterable<Arguments[0]>,
    options: RemoteMapOptions = {},
  ): AsyncGenerator<Result, void, undefined> {
    if (this.#generatorKind === "generator") {
      throw new TypeError("a generator function cannot be mapped; use .remoteGen() per input");
    }
    return this.#mapEngine(
      this.#argIterator(inputs, (item) => [item]),
      options,
    );
  }

  /** {@link map} over argument tuples, spread into the handler. */
  starmap(
    inputs: Iterable<Arguments> | AsyncIterable<Arguments>,
    options: RemoteMapOptions = {},
  ): AsyncGenerator<Result, void, undefined> {
    if (this.#generatorKind === "generator") {
      throw new TypeError("a generator function cannot be mapped; use .remoteGen() per input");
    }
    return this.#mapEngine(
      this.#argIterator(inputs, (item) => {
        if (!Array.isArray(item)) {
          throw new TypeError("starmap items must be argument arrays");
        }
        return item;
      }),
      options,
    );
  }

  /** Drain {@link map} in completion order, discarding results. */
  async forEach(
    inputs: Iterable<Arguments[0]> | AsyncIterable<Arguments[0]>,
    options: RemoteForEachOptions = {},
  ): Promise<void> {
    const results = this.map(inputs, {
      concurrency: options.concurrency,
      timeout: options.timeout,
      orderOutputs: false,
      returnExceptions: options.ignoreExceptions ?? false,
    });
    for await (const result of results) {
      void result;
    }
  }

  /** Close every session and terminate the cached sandbox; idempotent. */
  async terminate(): Promise<void> {
    for (const session of this.#spawnSessions) session.close();
    this.#spawnSessions.clear();
    await this.#invalidateCached(true);
  }

  async #callWithPolicy(
    target: DispatchTarget,
    spec: WireSourceSpec,
    args: JsonValue[],
    timeout: number | null,
    output: SessionOutputSink,
  ): Promise<Result> {
    let infraFailures = 0;
    let retriesUsed = 0;
    while (true) {
      try {
        const session = await target.acquire();
        const value = await session.callValue({ spec, args, timeout, output });
        return value as Result;
      } catch (error) {
        const kind = classifyFailure(error);
        if (kind === "infra" && infraFailures < MAX_INFRA_FAILURES) {
          infraFailures += 1;
          await target.invalidate(true);
          continue;
        }
        if (
          (kind === "user" || kind === "timeout") &&
          this.#retries !== null &&
          retriesUsed < this.#retries.maxRetries
        ) {
          retriesUsed += 1;
          await this.#sleep(this.#retries.delayFor(retriesUsed));
          continue;
        }
        throw error;
      }
    }
  }

  async *#mapEngine(
    items: AsyncIterator<JsonValue[]>,
    options: RemoteMapOptions,
  ): AsyncGenerator<Result, void, undefined> {
    const cap = options.concurrency ?? DEFAULT_MAP_CONCURRENCY;
    if (!Number.isInteger(cap) || cap < 1) {
      throw new RangeError("concurrency must be an integer >= 1");
    }
    const orderOutputs = options.orderOutputs ?? true;
    const returnExceptions = options.returnExceptions ?? false;
    const timeout = options.timeout ?? this.#timeout;
    const spec = await this.#wireSpec();

    const completions = new AsyncChannel<MapCompletion<Result>>();
    const liveSessions = new Set<WorkerSession>();
    const workers: Promise<void>[] = [];
    let nextIndex = 0;
    let inputDone = false;
    let stopped = false;
    let active = 0;
    let pullLock: Promise<unknown> = Promise.resolve();

    const pullNext = (): Promise<{ index: number; args: JsonValue[] } | null> => {
      const claim = pullLock.then(async () => {
        if (stopped || inputDone) return null;
        const step = await items.next();
        if (step.done === true) {
          inputDone = true;
          return null;
        }
        const index = nextIndex;
        nextIndex += 1;
        return { index, args: step.value };
      });
      pullLock = claim.catch(() => null);
      return claim;
    };

    const startWorker = (): void => {
      active += 1;
      const task = (async () => {
        const held: { sandbox: Sandbox | null; session: WorkerSession | null } = {
          sandbox: null,
          session: null,
        };
        const dropSession = (): void => {
          if (held.session !== null) {
            liveSessions.delete(held.session);
            held.session.close();
            held.session = null;
          }
        };
        const target: DispatchTarget = {
          acquire: async () => {
            if (held.session?.alive) return held.session;
            dropSession();
            held.sandbox ??= await this.#provisionSandbox();
            const session = await WorkerSession.start(held.sandbox);
            held.session = session;
            liveSessions.add(session);
            return session;
          },
          invalidate: async (fresh) => {
            dropSession();
            if (fresh && held.sandbox !== null) {
              const dying = held.sandbox;
              held.sandbox = null;
              await dying.terminate(false).catch(() => undefined);
            }
          },
        };
        try {
          while (!stopped) {
            const item = await pullNext();
            if (item === null) return;
            if (!stopped && !inputDone && active < cap) startWorker();
            const output: OutputChunk[] = [];
            try {
              const value = await this.#callWithPolicy(
                target,
                spec,
                item.args,
                timeout,
                (stream, text) => {
                  output.push([stream, text]);
                },
              );
              completions.push({ index: item.index, ok: true, value, error: undefined, output });
            } catch (error) {
              completions.push({
                index: item.index,
                ok: false,
                value: undefined,
                error,
                output,
              });
              if (!returnExceptions) {
                stopped = true;
                return;
              }
            }
          }
        } catch (error) {
          stopped = true;
          completions.fail(error);
        } finally {
          dropSession();
          if (held.sandbox !== null) await held.sandbox.terminate(false).catch(() => undefined);
          active -= 1;
          if (active === 0) completions.end();
        }
      })();
      workers.push(task);
      task.catch(() => undefined);
    };

    const heldBack = new Map<number, MapCompletion<Result>>();
    let emitIndex = 0;
    const deliver = (completion: MapCompletion<Result>): Result => {
      this.#forwardOutput(completion.output);
      if (!completion.ok && !returnExceptions) throw completion.error;
      return (completion.ok ? completion.value : completion.error) as Result;
    };
    try {
      startWorker();
      for await (const completion of completions) {
        if (!orderOutputs || (!completion.ok && !returnExceptions)) {
          yield deliver(completion);
          continue;
        }
        heldBack.set(completion.index, completion);
        while (heldBack.has(emitIndex)) {
          const next = heldBack.get(emitIndex) as MapCompletion<Result>;
          heldBack.delete(emitIndex);
          emitIndex += 1;
          yield deliver(next);
        }
      }
    } finally {
      stopped = true;
      for (const session of liveSessions) session.close();
      await Promise.allSettled(workers);
      if (typeof items.return === "function") {
        await items.return().catch(() => undefined);
      }
    }
  }

  #argIterator<Item>(
    inputs: Iterable<Item> | AsyncIterable<Item>,
    wrap: (item: Item) => unknown[],
  ): AsyncIterator<JsonValue[]> {
    async function* encode(): AsyncGenerator<JsonValue[], void, undefined> {
      for await (const item of inputs as AsyncIterable<Item>) {
        yield checkedJsonValue(wrap(item), "remote function arguments", new Set()) as JsonValue[];
      }
    }
    return encode()[Symbol.asyncIterator]();
  }

  #cachedTarget(): DispatchTarget {
    return {
      acquire: () => this.#ensureSession(),
      invalidate: (fresh) => this.#invalidateCached(fresh),
    };
  }

  async #ensureSession(): Promise<WorkerSession> {
    while (true) {
      const current = this.#sessionPromise;
      if (current !== null) {
        let session: WorkerSession;
        try {
          session = await current;
        } catch (error) {
          if (this.#sessionPromise === current) this.#sessionPromise = null;
          throw error;
        }
        if (session.alive) return session;
        if (this.#sessionPromise === current) this.#sessionPromise = null;
        continue;
      }
      const creation = (async () => {
        const sandbox = await this.#ensureSandbox();
        return WorkerSession.start(sandbox);
      })();
      this.#sessionPromise = creation;
      try {
        return await creation;
      } catch (error) {
        if (this.#sessionPromise === creation) this.#sessionPromise = null;
        throw error;
      }
    }
  }

  async #invalidateCached(fresh: boolean): Promise<void> {
    const sessionPromise = this.#sessionPromise;
    this.#sessionPromise = null;
    if (sessionPromise !== null) {
      const session = await sessionPromise.catch(() => null);
      session?.close();
    }
    if (!fresh) return;
    const cached = this.#cachedSandbox;
    this.#cachedSandbox = null;
    if (cached !== null) {
      const sandbox = await cached.catch(() => null);
      if (sandbox !== null) await sandbox.terminate(false).catch(() => undefined);
    }
  }

  async #ensureSandbox(): Promise<Sandbox> {
    const cached = this.#cachedSandbox;
    if (cached === null) {
      const creation = this.#provisionSandbox();
      this.#cachedSandbox = creation;
      try {
        return await creation;
      } catch (error) {
        if (this.#cachedSandbox === creation) this.#cachedSandbox = null;
        throw error;
      }
    }

    let sandbox: Sandbox;
    try {
      sandbox = await cached;
    } catch (error) {
      if (this.#cachedSandbox === cached) this.#cachedSandbox = null;
      throw error;
    }
    const live = await this.#sandboxIsLive(sandbox);
    if (this.#cachedSandbox !== cached) return this.#ensureSandbox();
    if (live) return sandbox;

    const replacement = (async () => {
      await sandbox.terminate(false).catch(() => undefined);
      return this.#provisionSandbox();
    })();
    this.#cachedSandbox = replacement;
    try {
      return await replacement;
    } catch (error) {
      if (this.#cachedSandbox === replacement) this.#cachedSandbox = null;
      throw error;
    }
  }

  async #sandboxIsLive(sandbox: Sandbox): Promise<boolean> {
    try {
      const view = await this.#client.sandboxes.get(sandbox.id);
      const status = typeof view.info.status === "string" ? view.info.status.toLowerCase() : "";
      return !TERMINAL_SANDBOX_STATUSES.has(status);
    } catch (error) {
      if (isRecord(error) && (error.status === 404 || error.code === "not_found")) {
        return false;
      }
      throw error;
    }
  }

  #provisionSandbox(): Promise<Sandbox> {
    const request = { ...this.#sandboxRequest };
    if (this.#pool > 0) request.pool_size = this.#pool;
    return this.#client.sandboxes.create(request);
  }

  #wireSpec(): Promise<WireSourceSpec> {
    this.#wireSpecPromise ??= (async () => ({
      hash: await sha256Hex(this.#spec.source),
      source: this.#spec.source,
      exportName: this.#spec.exportName,
    }))();
    return this.#wireSpecPromise;
  }

  #encodeArgs(args: unknown[]): JsonValue[] {
    return checkedJsonValue(args, "remote function arguments", new Set()) as JsonValue[];
  }

  #liveSink(): SessionOutputSink {
    return (stream, text) => {
      if (stream === "stderr") this.#onStderr(text);
      else this.#onStdout(text);
    };
  }

  #forwardOutput(chunks: OutputChunk[]): void {
    for (const [stream, text] of chunks) {
      if (stream === "stderr") this.#onStderr(text);
      else this.#onStdout(text);
    }
  }
}

/** Create a typed remote function from a source-serializable JavaScript closure. */
export function remoteFunction<Arguments extends unknown[], Result>(
  client: Client,
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
  const tag = Object.prototype.toString.call(fn);
  const generatorKind: GeneratorKind =
    tag === "[object GeneratorFunction]" || tag === "[object AsyncGeneratorFunction]"
      ? "generator"
      : "value";
  return new RemoteFunction<Arguments, Result>(
    client,
    {
      source: `export const ${CONVENIENCE_EXPORT} = (${source});\n`,
      exportName: CONVENIENCE_EXPORT,
    },
    options,
    generatorKind,
  );
}

/** Create a typed remote function from JavaScript module source and an exported handler. */
export function remoteFunctionFromSource<
  Arguments extends unknown[] = JsonValue[],
  Result = JsonValue,
>(
  client: Client,
  spec: RemoteFunctionSourceSpec,
  options: RemoteFunctionOptions = {},
): RemoteFunction<Arguments, Result> {
  return new RemoteFunction<Arguments, Result>(client, spec, options, "unknown");
}

function normalizeRetries(retries: number | Retries | undefined): Retries | null {
  if (retries === undefined) return null;
  if (retries instanceof Retries) return retries;
  return new Retries({ maxRetries: retries, backoffCoefficient: 1.0, initialDelay: 1.0 });
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

async function sha256Hex(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  let hex = "";
  for (const byte of new Uint8Array(digest)) {
    hex += byte.toString(16).padStart(2, "0");
  }
  return hex;
}

function sleepSeconds(seconds: number): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  setTimeout(resolve, seconds * 1000);
  return promise;
}

function defaultStdout(output: string): void {
  if (typeof process !== "undefined") {
    process.stdout.write(output);
    return;
  }
  console.log(output);
}

function defaultStderr(output: string): void {
  if (typeof process !== "undefined") {
    process.stderr.write(output);
    return;
  }
  console.error(output);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
