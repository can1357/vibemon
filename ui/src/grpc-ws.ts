// @connectrpc/connect Transport over the vmon gRPC-WebSocket bridge
// (proto/vmon/v1/bridge.proto): one RPC per socket to GET /grpc; every WS
// binary message is exactly one encoded BridgeFrame; `message` payloads are
// single encoded gRPC messages WITHOUT the 5-byte wire prefix (WS message
// boundaries provide framing). Text frames are protocol errors.
//
//   client:  call → message* → half_close
//   server:  message* → end   (status + trailers, incl. vmon-code)

import {
  create,
  type DescMessage,
  type DescMethodStreaming,
  type DescMethodUnary,
  fromBinary,
  type MessageInitShape,
  type MessageShape,
  toBinary,
} from "@bufbuild/protobuf";
import {
  Code,
  ConnectError,
  type StreamResponse,
  type Transport,
  type UnaryResponse,
} from "@connectrpc/connect";
import { type BridgeEnd, BridgeFrameSchema } from "./gen/vmon/v1/bridge_pb.ts";

export interface WsBridgeTransportOptions {
  // Page origin, e.g. window.location.origin; http(s) is rewritten to ws(s).
  baseUrl: string;
  // Read per RPC so a token change applies to the next call immediately.
  token?: () => string;
}

// Single-consumer push queue bridging callback-style producers (WebSocket
// events, xterm handlers) to an async iterable. Items pushed before a
// terminal finish/fail are drained first; fail() then surfaces as a throw.
export class PushQueue<T> implements AsyncIterable<T> {
  #items: T[] = [];
  #done = false;
  #error: unknown;
  #wake: (() => void) | null = null;

  push(item: T): void {
    if (this.#done) return;
    this.#items.push(item);
    this.#wake?.();
  }

  finish(): void {
    this.#done = true;
    this.#wake?.();
  }

  fail(err: unknown): void {
    if (this.#done) return;
    this.#error = err;
    this.#done = true;
    this.#wake?.();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<T> {
    for (;;) {
      if (this.#items.length > 0) {
        yield this.#items.shift() as T;
        continue;
      }
      if (this.#done) {
        if (this.#error !== undefined) throw this.#error;
        return;
      }
      await new Promise<void>((resolve) => {
        this.#wake = resolve;
      });
      this.#wake = null;
    }
  }
}

function endError(end: BridgeEnd): ConnectError {
  const code = end.status >= 1 && end.status <= 16 ? (end.status as Code) : Code.Unknown;
  return new ConnectError(end.message || "request failed", code, end.trailers);
}

// Runs one RPC over a fresh bridge socket, yielding decoded response
// messages; response trailers land in `trailer` once the end frame arrives.
async function* runCall<I extends DescMessage, O extends DescMessage>(
  options: WsBridgeTransportOptions,
  method: DescMethodUnary<I, O> | DescMethodStreaming<I, O>,
  signal: AbortSignal | undefined,
  timeoutMs: number | undefined,
  header: HeadersInit | undefined,
  input: Iterable<MessageInitShape<I>> | AsyncIterable<MessageInitShape<I>>,
  trailer: Headers,
): AsyncGenerator<MessageShape<O>> {
  const token = options.token?.() ?? "";
  const url = new URL("/grpc", options.baseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  // Browsers cannot set WebSocket handshake headers; auth rides the same
  // ?token= query the old WS endpoints used, plus RPC metadata below.
  if (token) url.searchParams.set("token", token);

  const metadata: Record<string, string> = {};
  new Headers(header).forEach((value, key) => {
    metadata[key] = value;
  });
  if (token && metadata.authorization === undefined) {
    metadata.authorization = `Bearer ${token}`;
  }

  const queue = new PushQueue<MessageShape<O>>();
  const ws = new WebSocket(url);
  ws.binaryType = "arraybuffer";
  let ended = false;

  const abort = (err: ConnectError): void => {
    ended = true;
    queue.fail(err);
    ws.close();
  };

  let openResolve!: (open: boolean) => void;
  const opened = new Promise<boolean>((resolve) => {
    openResolve = resolve;
  });
  ws.onopen = () => openResolve(true);
  ws.onclose = (ev) => {
    openResolve(false);
    if (!ended) {
      queue.fail(new ConnectError(ev.reason || `connection closed (${ev.code})`, Code.Unavailable));
    }
  };
  ws.onmessage = (ev) => {
    if (ended) return;
    if (!(ev.data instanceof ArrayBuffer)) {
      abort(new ConnectError("expected binary protobuf frame", Code.Internal));
      return;
    }
    try {
      const frame = fromBinary(BridgeFrameSchema, new Uint8Array(ev.data));
      switch (frame.frame.case) {
        case "message":
          queue.push(fromBinary(method.output, frame.frame.value));
          break;
        case "end": {
          ended = true;
          const end = frame.frame.value;
          for (const key in end.trailers) trailer.set(key, end.trailers[key]);
          if (end.status === 0) queue.finish();
          else queue.fail(endError(end));
          ws.close();
          break;
        }
        default:
          abort(new ConnectError("unexpected bridge frame from server", Code.Internal));
      }
    } catch (err) {
      abort(ConnectError.from(err, Code.Internal));
    }
  };

  const sendFrame = (frame: MessageInitShape<typeof BridgeFrameSchema>): void => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(toBinary(BridgeFrameSchema, create(BridgeFrameSchema, frame)));
    }
  };
  const pump = async (): Promise<void> => {
    if (!(await opened)) return;
    sendFrame({
      frame: {
        case: "call",
        value: { method: `/${method.parent.typeName}/${method.name}`, metadata },
      },
    });
    for await (const message of input) {
      sendFrame({
        frame: { case: "message", value: toBinary(method.input, create(method.input, message)) },
      });
    }
    sendFrame({ frame: { case: "halfClose", value: {} } });
  };
  pump().catch((err: unknown) => abort(ConnectError.from(err)));

  const onAbort = (): void => {
    abort(ConnectError.from(signal?.reason ?? new ConnectError("canceled", Code.Canceled)));
  };
  signal?.addEventListener("abort", onAbort);
  if (signal?.aborted) onAbort();
  const timer =
    timeoutMs === undefined
      ? undefined
      : setTimeout(
          () => abort(new ConnectError("the operation timed out", Code.DeadlineExceeded)),
          timeoutMs,
        );

  try {
    yield* queue;
  } finally {
    signal?.removeEventListener("abort", onAbort);
    clearTimeout(timer);
    ended = true;
    if (ws.readyState === WebSocket.CONNECTING || ws.readyState === WebSocket.OPEN) {
      ws.close();
    }
  }
}

export function createWsBridgeTransport(options: WsBridgeTransportOptions): Transport {
  return {
    async unary<I extends DescMessage, O extends DescMessage>(
      method: DescMethodUnary<I, O>,
      signal: AbortSignal | undefined,
      timeoutMs: number | undefined,
      header: HeadersInit | undefined,
      input: MessageInitShape<I>,
    ): Promise<UnaryResponse<I, O>> {
      const trailer = new Headers();
      let message: MessageShape<O> | undefined;
      for await (const item of runCall(
        options,
        method,
        signal,
        timeoutMs,
        header,
        [input],
        trailer,
      )) {
        if (message !== undefined) {
          throw new ConnectError("protocol error: extra message for unary call", Code.Internal);
        }
        message = item;
      }
      if (message === undefined) {
        throw new ConnectError("protocol error: missing unary response message", Code.Internal);
      }
      return {
        stream: false,
        service: method.parent,
        method,
        header: new Headers(),
        trailer,
        message,
      };
    },
    async stream<I extends DescMessage, O extends DescMessage>(
      method: DescMethodStreaming<I, O>,
      signal: AbortSignal | undefined,
      timeoutMs: number | undefined,
      header: HeadersInit | undefined,
      input: AsyncIterable<MessageInitShape<I>>,
    ): Promise<StreamResponse<I, O>> {
      const trailer = new Headers();
      return {
        stream: true,
        service: method.parent,
        method,
        header: new Headers(),
        trailer,
        message: runCall(options, method, signal, timeoutMs, header, input, trailer),
      };
    },
  };
}
