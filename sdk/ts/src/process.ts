import { APIError, ProtocolError } from "./errors";
import type { EventRecord, ExecExit } from "./models";

/** One decoded process output event. */
export interface ExecEvent {
  stream: "stdout" | "stderr" | "console";
  data: Uint8Array;
}
/** Optional process output callbacks. */
export interface ProcessOptions {
  onStdout?: (data: Uint8Array) => void;
  onStderr?: (data: Uint8Array) => void;
}

type Resolver<T> = { promise: Promise<T>; resolve(value: T): void; reject(reason: unknown): void };
function deferred<T>(): Resolver<T> {
  return Promise.withResolvers<T>();
}

class AsyncQueue<T> implements AsyncIterable<T> {
  readonly #values: T[] = [];
  readonly #waiters: Array<Resolver<IteratorResult<T>>> = [];
  #error: unknown;
  #done = false;
  push(value: T): void {
    const waiter = this.#waiters.shift();
    if (waiter) waiter.resolve({ value, done: false });
    else this.#values.push(value);
  }
  end(): void {
    this.#done = true;
    for (const waiter of this.#waiters.splice(0)) waiter.resolve({ value: undefined, done: true });
  }
  fail(error: unknown): void {
    this.#error = error;
    this.#done = true;
    for (const waiter of this.#waiters.splice(0)) waiter.reject(error);
  }
  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: async () => {
        const value = this.#values.shift();
        if (value !== undefined) return { value, done: false };
        if (this.#error) throw this.#error;
        if (this.#done) return { value: undefined, done: true };
        const waiter = deferred<IteratorResult<T>>();
        this.#waiters.push(waiter);
        return waiter.promise;
      },
    };
  }
}

/** Bidirectional exec or shell session using the vmon WebSocket protocol. */
export class Process implements AsyncIterable<ExecEvent> {
  readonly #socket: WebSocket;
  readonly #events = new AsyncQueue<ExecEvent>();
  readonly #exit = deferred<ExecExit>();
  readonly #ready = deferred<void>();
  readonly #options: ProcessOptions;
  #finished = false;
  /** Writable process standard input. */
  readonly stdin: { write(data: Uint8Array | string): void; close(): void };
  /** Bind a process to an open WebSocket and send its initial request. */
  constructor(socket: WebSocket, request?: unknown, options: ProcessOptions = {}) {
    this.#socket = socket;
    this.#options = options;
    this.stdin = {
      write: (data) =>
        this.#send({
          stdin_b64: encodeBase64(typeof data === "string" ? new TextEncoder().encode(data) : data),
        }),
      close: () => this.#send({ eof: true }),
    };
    socket.binaryType = "arraybuffer";
    socket.addEventListener("message", (event) => void this.#message(event));
    socket.addEventListener("error", () => this.#fail(new ProtocolError("WebSocket error")));
    socket.addEventListener("close", () => {
      if (!this.#finished) this.#fail(new ProtocolError("WebSocket closed before an exit frame"));
    });
    if (request) socket.send(JSON.stringify(request));
  }
  /** Resize the guest terminal. */
  resize(rows: number, cols: number): void {
    this.#send({ resize: [rows, cols] });
  }
  /** Wait for the process exit envelope. */
  wait(): Promise<ExecExit> {
    return this.#exit.promise;
  }
  /** Close the exec WebSocket; the server terminates the guest process. */
  close(): void {
    this.#socket.close();
  }
  /** Wait for a shell ready envelope. */
  ready(): Promise<void> {
    return Promise.race([
      this.#ready.promise,
      this.#exit.promise.then(() => {
        throw new ProtocolError("process exited before a ready frame");
      }),
    ]);
  }
  /** Iterate decoded output events. */
  [Symbol.asyncIterator](): AsyncIterator<ExecEvent> {
    return this.#events[Symbol.asyncIterator]();
  }
  async #message(event: MessageEvent): Promise<void> {
    try {
      const text = await messageText(event.data);
      const envelope: unknown = JSON.parse(text);
      if (!isRecord(envelope)) throw new ProtocolError("WebSocket frame must be an object");
      if (isRecord(envelope.error)) {
        const error = new APIError({
          status: 0,
          code: typeof envelope.error.code === "string" ? envelope.error.code : "protocol",
          message:
            typeof envelope.error.message === "string" ? envelope.error.message : "remote error",
        });
        this.#fail(error);
        return;
      }
      if (typeof envelope.exit === "number") {
        const result = {
          code: envelope.exit,
          signal: typeof envelope.signal === "number" ? envelope.signal : null,
        };
        this.#finished = true;
        this.#exit.resolve(result);
        this.#events.end();
        return;
      }
      if (
        typeof envelope.stream === "string" &&
        typeof envelope.b64 === "string" &&
        (envelope.stream === "stdout" ||
          envelope.stream === "stderr" ||
          envelope.stream === "console")
      ) {
        const data = decodeBase64(envelope.b64);
        if (envelope.stream === "stdout") this.#options.onStdout?.(data);
        if (envelope.stream === "stderr") this.#options.onStderr?.(data);
        this.#events.push({ stream: envelope.stream, data });
        return;
      }
      if ("ready" in envelope) {
        this.#ready.resolve();
        return;
      }
      throw new ProtocolError("unknown WebSocket envelope");
    } catch (error) {
      this.#fail(error instanceof Error ? error : new ProtocolError("malformed WebSocket frame"));
    }
  }
  #send(envelope: unknown): void {
    if (this.#socket.readyState !== 1) throw new ProtocolError("WebSocket is not open");
    this.#socket.send(JSON.stringify(envelope));
  }
  #fail(error: Error): void {
    if (this.#finished) return;
    this.#finished = true;
    this.#events.fail(error);
    this.#exit.reject(error);
  }
}

/** Read-only console attachment stream. */
export class ConsoleStream implements AsyncIterable<Uint8Array> {
  readonly #socket: WebSocket;
  readonly #chunks = new AsyncQueue<Uint8Array>();
  /** Bind a console stream to an open WebSocket. */
  constructor(socket: WebSocket) {
    this.#socket = socket;
    socket.addEventListener("message", (event) => void this.#message(event));
    socket.addEventListener("close", () => this.#chunks.end());
    socket.addEventListener("error", () =>
      this.#chunks.fail(new ProtocolError("console WebSocket error")),
    );
  }
  /** Close the console socket. */
  close(): void {
    this.#socket.close();
  }
  /** Iterate decoded console bytes. */
  [Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
    return this.#chunks[Symbol.asyncIterator]();
  }
  async #message(event: MessageEvent): Promise<void> {
    try {
      const value: unknown = JSON.parse(await messageText(event.data));
      if (!isRecord(value)) throw new ProtocolError("console frame must be an object");
      if (isRecord(value.error))
        throw new APIError({
          status: 0,
          code: String(value.error.code ?? "protocol"),
          message: String(value.error.message ?? "remote error"),
        });
      if (typeof value.b64 === "string") this.#chunks.push(decodeBase64(value.b64));
      else throw new ProtocolError("malformed console frame");
    } catch (error) {
      this.#chunks.fail(error);
    }
  }
}

/** Server-sent daemon event stream. */
export class EventStream implements AsyncIterable<EventRecord> {
  readonly #reader: ReadableStreamDefaultReader<string>;
  /** Bind an event stream to an HTTP response body. */
  constructor(response: Response) {
    if (!response.body) throw new ProtocolError("response has no stream body");
    this.#reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  }
  /** Cancel the event response body. */
  close(): Promise<void> {
    return this.#reader.cancel();
  }
  /** Iterate decoded daemon events. */
  [Symbol.asyncIterator](): AsyncIterator<EventRecord> {
    return parseSse(this.#reader, (data) => {
      const parsed: unknown = JSON.parse(data);
      if (!isRecord(parsed)) throw new ProtocolError("SSE event must be an object");
      return parsed;
    });
  }
}

/** Server-sent follow-log stream. */
export class LogStream implements AsyncIterable<string> {
  readonly #reader: ReadableStreamDefaultReader<string>;
  /** Bind a log stream to an HTTP response body. */
  constructor(response: Response) {
    if (!response.body) throw new ProtocolError("response has no stream body");
    this.#reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  }
  /** Cancel the follow-log response body. */
  close(): Promise<void> {
    return this.#reader.cancel();
  }
  /** Iterate decoded log chunks. */
  [Symbol.asyncIterator](): AsyncIterator<string> {
    return parseSse(this.#reader, (data) => {
      const parsed: unknown = JSON.parse(data);
      if (!isRecord(parsed)) throw new ProtocolError("log event must be an object");
      if (typeof parsed.data === "string") return parsed.data;
      if (typeof parsed.b64 === "string") return new TextDecoder().decode(decodeBase64(parsed.b64));
      throw new ProtocolError("malformed log event");
    });
  }
}

async function* sseValues<T>(
  reader: ReadableStreamDefaultReader<string>,
  convert: (data: string) => T,
): AsyncGenerator<T> {
  let buffer = "";
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer = `${buffer}${value}`.replace(/\r\n/g, "\n");
      while (true) {
        const boundary = buffer.indexOf("\n\n");
        if (boundary < 0) break;
        const block = buffer.slice(0, boundary).replace(/\r/g, "");
        buffer = buffer.slice(boundary + 2);
        for (const line of block.split("\n"))
          if (line.startsWith("data:")) yield convert(line.slice(5).trimStart());
      }
    }
  } finally {
    reader.releaseLock();
  }
}
function parseSse<T>(
  reader: ReadableStreamDefaultReader<string>,
  convert: (data: string) => T,
): AsyncIterator<T> {
  return sseValues(reader, convert);
}

async function messageText(data: unknown): Promise<string> {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (data instanceof Blob) return data.text();
  throw new ProtocolError("unsupported WebSocket frame type");
}
function encodeBase64(data: Uint8Array): string {
  if (typeof Buffer !== "undefined") return Buffer.from(data).toString("base64");
  let binary = "";
  for (const byte of data) binary += String.fromCharCode(byte);
  return btoa(binary);
}
function decodeBase64(value: string): Uint8Array {
  try {
    if (typeof Buffer !== "undefined") return new Uint8Array(Buffer.from(value, "base64"));
    return Uint8Array.from(atob(value), (character) => character.charCodeAt(0));
  } catch (error) {
    throw new ProtocolError("invalid base64 frame", { cause: error });
  }
}
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
