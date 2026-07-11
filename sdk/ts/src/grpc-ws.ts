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
import { AsyncQueue } from "./async-queue";
import { type BridgeFrame, BridgeFrameSchema } from "./gen/vmon/v1/bridge_pb";

/** Options for the gRPC-over-WebSocket bridge transport. */
export interface WsBridgeTransportOptions {
  /** HTTP(S) endpoint of the vmond node; `/grpc` is appended per RPC. */
  baseUrl: string;
  /** Bearer token: sent as `?token=` on the upgrade and as request metadata. */
  token?: string;
  /** WebSocket constructor override. */
  websocket?: typeof WebSocket;
  /** WebSocket upgrade timeout in milliseconds. */
  connectTimeoutMs?: number;
}

type BridgeFrameInit = MessageInitShape<typeof BridgeFrameSchema>["frame"];

interface BridgeCallHandle {
  send(frame: BridgeFrameInit): void;
  responses: AsyncQueue<Uint8Array>;
  trailer: Headers;
  cancel(): void;
}

/**
 * Create a Transport speaking gRPC over the vmond `/grpc` WebSocket bridge
 * (proto/vmon/v1/bridge.proto): one RPC per socket, one encoded BridgeFrame
 * per binary WS message, payloads without the 5-byte gRPC prefix.
 */
export function createWsBridgeTransport(options: WsBridgeTransportOptions): Transport {
  return {
    async unary<I extends DescMessage, O extends DescMessage>(
      method: DescMethodUnary<I, O>,
      signal: AbortSignal | undefined,
      timeoutMs: number | undefined,
      header: HeadersInit | undefined,
      input: MessageInitShape<I>,
    ): Promise<UnaryResponse<I, O>> {
      const call = await openBridge(options, method, signal, timeoutMs, header);
      try {
        call.send({ case: "message", value: toBinary(method.input, create(method.input, input)) });
        call.send({ case: "halfClose", value: {} });
        let received: Uint8Array | undefined;
        for await (const payload of call.responses) {
          if (received !== undefined)
            throw new ConnectError("unary RPC received multiple responses", Code.Internal);
          received = payload;
        }
        if (received === undefined)
          throw new ConnectError("unary RPC ended without a response", Code.Internal);
        return {
          stream: false,
          service: method.parent,
          method,
          header: new Headers(),
          trailer: call.trailer,
          message: fromBinary(method.output, received),
        };
      } finally {
        call.cancel();
      }
    },
    async stream<I extends DescMessage, O extends DescMessage>(
      method: DescMethodStreaming<I, O>,
      signal: AbortSignal | undefined,
      timeoutMs: number | undefined,
      header: HeadersInit | undefined,
      input: AsyncIterable<MessageInitShape<I>>,
    ): Promise<StreamResponse<I, O>> {
      const call = await openBridge(options, method, signal, timeoutMs, header);
      void (async () => {
        try {
          for await (const request of input)
            call.send({
              case: "message",
              value: toBinary(method.input, create(method.input, request)),
            });
          call.send({ case: "halfClose", value: {} });
        } catch {
          call.cancel();
        }
      })();
      return {
        stream: true,
        service: method.parent,
        method,
        header: new Headers(),
        trailer: call.trailer,
        message: decodePayloads(method.output, call.responses),
      };
    },
  };
}

async function openBridge(
  options: WsBridgeTransportOptions,
  method: { parent: { typeName: string }; name: string },
  signal: AbortSignal | undefined,
  timeoutMs: number | undefined,
  header: HeadersInit | undefined,
): Promise<BridgeCallHandle> {
  const url = new URL(`${options.baseUrl.replace(/\/$/, "")}/grpc`);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  if (options.token) url.searchParams.set("token", options.token);
  const Socket = options.websocket ?? globalThis.WebSocket;
  const socket = new Socket(url);
  socket.binaryType = "arraybuffer";

  const responses = new AsyncQueue<Uint8Array>();
  const trailer = new Headers();
  let settled = false;
  const deadline =
    timeoutMs === undefined
      ? undefined
      : setTimeout(
          () => finish(new ConnectError("the operation timed out", Code.DeadlineExceeded)),
          timeoutMs,
        );

  function finish(error?: ConnectError): void {
    if (settled) return;
    settled = true;
    clearTimeout(deadline);
    if (error) responses.fail(error);
    else responses.end();
    try {
      socket.close();
    } catch {
      // Already closed.
    }
  }

  socket.addEventListener("message", (event) => {
    let frame: BridgeFrame;
    try {
      frame = decodeBridgeFrame((event as MessageEvent).data);
    } catch (error) {
      finish(ConnectError.from(error, Code.Internal));
      return;
    }
    switch (frame.frame.case) {
      case "message":
        responses.push(frame.frame.value);
        return;
      case "end": {
        const end = frame.frame.value;
        for (const key in end.trailers) trailer.set(key, end.trailers[key]);
        if (end.status !== 0)
          finish(new ConnectError(end.message || "RPC failed", end.status as Code, end.trailers));
        else finish();
        return;
      }
      default:
        finish(new ConnectError("unexpected bridge frame", Code.Internal));
    }
  });
  socket.addEventListener("close", () =>
    finish(new ConnectError("bridge closed without an end frame", Code.Unavailable)),
  );
  socket.addEventListener("error", () =>
    finish(new ConnectError("bridge WebSocket error", Code.Unavailable)),
  );
  signal?.addEventListener("abort", () => finish(ConnectError.from(signal.reason, Code.Canceled)), {
    once: true,
  });

  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new ConnectError("WebSocket connect timeout", Code.Unavailable)),
      options.connectTimeoutMs ?? 60_000,
    );
    socket.addEventListener(
      "open",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
    socket.addEventListener(
      "error",
      () => {
        clearTimeout(timer);
        reject(
          new ConnectError(`WebSocket connection to ${options.baseUrl} failed`, Code.Unavailable),
        );
      },
      { once: true },
    );
  });

  const metadata: Record<string, string> = {};
  new Headers(header).forEach((value, key) => {
    metadata[key] = value;
  });
  if (options.token && metadata.authorization === undefined)
    metadata.authorization = `Bearer ${options.token}`;
  const send = (frame: BridgeFrameInit): void => {
    if (socket.readyState !== 1) return;
    socket.send(toBinary(BridgeFrameSchema, create(BridgeFrameSchema, { frame })));
  };
  send({
    case: "call",
    value: { method: `/${method.parent.typeName}/${method.name}`, metadata },
  });
  return {
    send,
    responses,
    trailer,
    cancel: () => finish(new ConnectError("call canceled", Code.Canceled)),
  };
}

async function* decodePayloads<O extends DescMessage>(
  schema: O,
  payloads: AsyncIterable<Uint8Array>,
): AsyncGenerator<MessageShape<O>> {
  for await (const payload of payloads) yield fromBinary(schema, payload);
}

function decodeBridgeFrame(data: unknown): BridgeFrame {
  if (typeof data === "string")
    throw new ConnectError("expected binary bridge frame", Code.Internal);
  // Frames can arrive before binaryType applies (Bun defaults to nodebuffer).
  if (data instanceof Uint8Array) return fromBinary(BridgeFrameSchema, data);
  if (data instanceof ArrayBuffer) return fromBinary(BridgeFrameSchema, new Uint8Array(data));
  throw new ConnectError("unsupported WebSocket frame type", Code.Internal);
}
