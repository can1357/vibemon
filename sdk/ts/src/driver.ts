import type {
  DescMessage,
  DescMethodBiDiStreaming,
  DescMethodServerStreaming,
  DescMethodStreaming,
  DescMethodUnary,
  MessageInitShape,
  MessageShape,
} from "@bufbuild/protobuf";
import { Code, ConnectError, type Transport } from "@connectrpc/connect";
import { type DsnConfig, type DsnEndpoint, type ParseDsnOptions, parseDsn } from "./dsn";
import { APIError, rpcError, TransportError } from "./errors";
import { SandboxService, SystemService } from "./gen/vmon/v1/api_pb";
import { createWsBridgeTransport } from "./grpc-ws";

/** Mesh roster refresh interval in milliseconds. */
export const ROSTER_TTL = 60_000;
/** Failed-endpoint cooldown in milliseconds. */
export const ENDPOINT_COOLDOWN = 5_000;

/** Fetch-compatible transport injection point. */
export type VmonFetch = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;
/** Public health and provenance view of one roster endpoint. */
export interface EndpointInfo {
  url: string;
  healthy: boolean;
  source: "seed" | "discovered";
}
/** Options for one driver request. */
export interface DriverRequestOptions {
  params?: URLSearchParams | Record<string, string | number | boolean | null | undefined>;
  json?: unknown;
  content?: BodyInit | null;
  headers?: HeadersInit;
  stream?: boolean;
  endpoint?: string;
}
/** HTTP response annotated with the endpoint that served it. */
export interface DriverResponse extends Response {
  readonly endpoint: string;
}
/** Options for one RPC. */
export interface RpcCallOptions {
  endpoint?: string;
}
/** Unary RPC result annotated with the endpoint that served it. */
export interface RpcResult<T> {
  message: T;
  endpoint: string;
}
/** Cancelable server-streaming RPC handle annotated with its endpoint. */
export interface RpcStream<T> {
  stream: AsyncIterable<T>;
  endpoint: string;
  cancel(): void;
}
/** Per-endpoint gRPC-web transport factory (test seam). */
export type TransportFactory = (baseUrl: string) => Transport;
/** Transport seam consumed by Client and bound resources. */
export interface Driver {
  /** Bearer token used by HTTP and WebSocket requests. */
  readonly token?: string;
  /** Perform one affinity-aware HTTP request (healthz, metrics, ports proxy). */
  request(method: string, path: string, options?: DriverRequestOptions): Promise<DriverResponse>;
  /** Perform one affinity-aware unary RPC. */
  call<I extends DescMessage, O extends DescMessage>(
    method: DescMethodUnary<I, O>,
    input: MessageInitShape<I>,
    options?: RpcCallOptions,
  ): Promise<RpcResult<MessageShape<O>>>;
  /** Open one affinity-aware server-streaming RPC. */
  serverStream<I extends DescMessage, O extends DescMessage>(
    method: DescMethodServerStreaming<I, O>,
    input: MessageInitShape<I>,
    options?: RpcCallOptions,
  ): Promise<RpcStream<MessageShape<O>>>;
  /** Open one affinity-aware bidi-streaming RPC. */
  duplex<I extends DescMessage, O extends DescMessage>(
    method: DescMethodBiDiStreaming<I, O>,
    input: AsyncIterable<MessageInitShape<I>>,
    options?: RpcCallOptions,
  ): Promise<RpcStream<MessageShape<O>>>;
  /** Open one affinity-aware WebSocket. */
  websocket(
    path: string,
    options?: { query?: URLSearchParams | Record<string, string>; endpoint?: string },
  ): Promise<[WebSocket, string]>;
  /** Locate the endpoint currently serving a sandbox. */
  resolveSandbox(id: string, hint?: string): Promise<string>;
  /** Snapshot the current endpoint roster. */
  endpoints(): EndpointInfo[];
  /** Refresh mesh discovery. */
  refresh(force?: boolean): Promise<void>;
  /** Release driver resources. */
  close(): void | Promise<void>;
}
/** Mesh driver construction and transport overrides. */
export interface DriverOptions extends ParseDsnOptions {
  fetch?: VmonFetch;
  websocket?: typeof WebSocket;
  transport?: TransportFactory;
  now?: () => number;
}

type RosterEntry = DsnEndpoint & {
  source: "seed" | "discovered";
  failedUntil: number;
};
class EndpointResponse extends Response implements DriverResponse {
  readonly endpoint: string;
  constructor(response: Response, endpoint: string) {
    super(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
    this.endpoint = endpoint;
  }
}

/** Fetch/WebSocket driver with discovery, affinity, and transport-only failover. */
export class MeshDriver implements Driver {
  readonly token?: string;
  readonly #fetch: VmonFetch;
  readonly #websocket: typeof WebSocket;
  readonly #discover: boolean;
  readonly #timeout: number;
  readonly #now: () => number;
  #roster: RosterEntry[];
  #preferred = 0;
  #lastRefresh = 0;
  #refreshing: Promise<void> | null = null;
  #closed = false;
  readonly #transportFactory?: TransportFactory;
  readonly #transports = new Map<string, Transport>();

  /** Build a driver from a DSN or parsed configuration. */
  constructor(dsn?: string | null | DsnConfig, options: DriverOptions = {}) {
    const config = typeof dsn === "object" && dsn !== null ? dsn : parseDsn(dsn, options);
    this.token = options.token ?? config.token;
    this.#fetch = options.fetch ?? globalThis.fetch;
    this.#websocket = options.websocket ?? globalThis.WebSocket;
    this.#discover = options.discover ?? config.discover;
    this.#timeout = (options.timeout ?? config.timeout) * 1_000;
    this.#now = options.now ?? Date.now;
    this.#transportFactory = options.transport;
    this.#roster = config.endpoints.map((entry) => ({ ...entry, source: "seed", failedUntil: 0 }));
  }

  /** Snapshot endpoint health and provenance. */
  endpoints(): EndpointInfo[] {
    const now = this.#now();
    return this.#roster.map((entry) => ({
      url: entry.url,
      healthy: entry.failedUntil <= now,
      source: entry.source,
    }));
  }

  /** Perform one request with transport-only failover. */
  async request(
    method: string,
    path: string,
    options: DriverRequestOptions = {},
  ): Promise<DriverResponse> {
    const result = await this.#request(method, path, options, true);
    if (
      this.#discover &&
      (this.#lastRefresh === 0 || this.#now() - this.#lastRefresh >= ROSTER_TTL)
    )
      await this.refresh();
    return result;
  }

  async #request(
    method: string,
    path: string,
    options: DriverRequestOptions,
    markFailover: boolean,
  ): Promise<DriverResponse> {
    if (this.#closed) throw new TransportError("driver is closed");
    const candidates = this.#candidates(options.endpoint);
    let last: TransportError | undefined;
    let failedOver = false;
    for (const entry of candidates) {
      const cooling = entry.failedUntil > this.#now();
      if (cooling && candidates.length > 1) continue;
      try {
        const fetched = await this.#fetchEndpoint(entry, method, path, options);
        const response = new EndpointResponse(fetched, entry.url);
        entry.failedUntil = 0;
        const index = this.#roster.indexOf(entry);
        if (index >= 0) this.#preferred = index;
        if (failedOver && markFailover && this.#discover) await this.refresh(true);
        return response;
      } catch (error) {
        const transport =
          error instanceof TransportError
            ? error
            : new TransportError(`request to ${entry.url} failed`, { cause: error });
        last = transport;
        entry.failedUntil = this.#now() + ENDPOINT_COOLDOWN;
        failedOver = true;
      }
    }
    throw last ?? new TransportError("no usable endpoints");
  }

  /** Perform one unary RPC with transport-only failover. */
  async call<I extends DescMessage, O extends DescMessage>(
    method: DescMethodUnary<I, O>,
    input: MessageInitShape<I>,
    options: RpcCallOptions = {},
  ): Promise<RpcResult<MessageShape<O>>> {
    const result = await this.#call(method, input, options, true);
    if (
      this.#discover &&
      (this.#lastRefresh === 0 || this.#now() - this.#lastRefresh >= ROSTER_TTL)
    )
      await this.refresh();
    return result;
  }

  async #call<I extends DescMessage, O extends DescMessage>(
    method: DescMethodUnary<I, O>,
    input: MessageInitShape<I>,
    options: RpcCallOptions,
    markFailover: boolean,
  ): Promise<RpcResult<MessageShape<O>>> {
    if (this.#closed) throw new TransportError("driver is closed");
    const candidates = this.#candidates(options.endpoint);
    let last: TransportError | undefined;
    let failedOver = false;
    for (const entry of candidates) {
      if (entry.failedUntil > this.#now() && candidates.length > 1) continue;
      try {
        const response = await this.#transportFor(entry).unary(
          method,
          undefined,
          this.#timeout,
          this.token ? { authorization: `Bearer ${this.token}` } : undefined,
          input,
        );
        entry.failedUntil = 0;
        const index = this.#roster.indexOf(entry);
        if (index >= 0) this.#preferred = index;
        if (failedOver && markFailover && this.#discover) await this.refresh(true);
        return { message: response.message, endpoint: entry.url };
      } catch (error) {
        const failure = ConnectError.from(error);
        if (!isTransportFailure(failure)) throw rpcError(failure);
        last = new TransportError(`request to ${entry.url} failed`, { cause: error });
        entry.failedUntil = this.#now() + ENDPOINT_COOLDOWN;
        failedOver = true;
      }
    }
    throw last ?? new TransportError("no usable endpoints");
  }

  /** Open one server-streaming RPC with transport-only failover on connect. */
  async serverStream<I extends DescMessage, O extends DescMessage>(
    method: DescMethodServerStreaming<I, O>,
    input: MessageInitShape<I>,
    options: RpcCallOptions = {},
  ): Promise<RpcStream<MessageShape<O>>> {
    return this.#openStream(
      method,
      (async function* () {
        yield input;
      })(),
      options,
    );
  }

  /** Open one bidi-streaming RPC with transport-only failover on connect. */
  async duplex<I extends DescMessage, O extends DescMessage>(
    method: DescMethodBiDiStreaming<I, O>,
    input: AsyncIterable<MessageInitShape<I>>,
    options: RpcCallOptions = {},
  ): Promise<RpcStream<MessageShape<O>>> {
    return this.#openStream(method, input, options);
  }

  // The bridge transport consumes `input` only after the socket opens, so a
  // connect rejection leaves the request iterable untouched for the next
  // candidate.
  async #openStream<I extends DescMessage, O extends DescMessage>(
    method: DescMethodStreaming<I, O>,
    input: AsyncIterable<MessageInitShape<I>>,
    options: RpcCallOptions,
  ): Promise<RpcStream<MessageShape<O>>> {
    if (this.#closed) throw new TransportError("driver is closed");
    const candidates = this.#candidates(options.endpoint);
    let last: TransportError | undefined;
    for (const entry of candidates) {
      if (entry.failedUntil > this.#now() && candidates.length > 1) continue;
      const controller = new AbortController();
      try {
        const response = await this.#transportFor(entry).stream(
          method,
          controller.signal,
          undefined,
          this.token ? { authorization: `Bearer ${this.token}` } : undefined,
          input,
        );
        entry.failedUntil = 0;
        const index = this.#roster.indexOf(entry);
        if (index >= 0) this.#preferred = index;
        return {
          stream: rpcMessages(response.message),
          endpoint: entry.url,
          cancel: () => controller.abort(),
        };
      } catch (error) {
        const failure = ConnectError.from(error);
        if (!isTransportFailure(failure)) throw rpcError(failure);
        last = new TransportError(`request to ${entry.url} failed`, { cause: error });
        entry.failedUntil = this.#now() + ENDPOINT_COOLDOWN;
      }
    }
    throw last ?? new TransportError("no usable endpoints");
  }

  #transportFor(entry: RosterEntry): Transport {
    const cached = this.#transports.get(entry.url);
    if (cached) return cached;
    if (entry.unix && !this.#transportFactory)
      throw new TransportError("gRPC bridge over UDS is not supported");
    const transport = this.#transportFactory
      ? this.#transportFactory(entry.url)
      : createWsBridgeTransport({
          baseUrl: entry.url,
          token: this.token,
          websocket: this.#websocket,
          connectTimeoutMs: this.#timeout,
        });
    this.#transports.set(entry.url, transport);
    return transport;
  }

  async #fetchEndpoint(
    entry: RosterEntry,
    method: string,
    path: string,
    options: DriverRequestOptions,
  ): Promise<Response> {
    const url = new URL(path.startsWith("/") ? `${entry.url}${path}` : `${entry.url}/${path}`);
    if (options.params instanceof URLSearchParams) {
      options.params.forEach((value, key) => {
        url.searchParams.append(key, value);
      });
    } else if (options.params) {
      for (const key in options.params) {
        const value = options.params[key];
        if (value !== null && value !== undefined) url.searchParams.set(key, String(value));
      }
    }
    const headers = new Headers(options.headers);
    if (this.token) headers.set("Authorization", `Bearer ${this.token}`);
    let body = options.content;
    if (options.json !== undefined) {
      headers.set("Content-Type", "application/json");
      body = JSON.stringify(options.json);
    }
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.#timeout);
    try {
      const init: RequestInit & { unix?: string } = {
        method,
        headers,
        body,
        signal: controller.signal,
      };
      if (entry.unix) {
        if (typeof Bun === "undefined") throw new Error("UDS endpoints require Bun");
        init.unix = entry.unix;
      }
      return await this.#fetch(url, init);
    } catch (error) {
      if (entry.unix && typeof Bun === "undefined") throw error;
      throw new TransportError(`request to ${entry.url} failed`, { cause: error });
    } finally {
      clearTimeout(timer);
    }
  }

  /** Open a WebSocket with affinity and transport-only failover. */
  async websocket(
    path: string,
    options: { query?: URLSearchParams | Record<string, string>; endpoint?: string } = {},
  ): Promise<[WebSocket, string]> {
    let last: TransportError | undefined;
    const candidates = this.#candidates(options.endpoint);
    for (const entry of candidates) {
      if (entry.failedUntil > this.#now() && candidates.length > 1) continue;
      if (entry.unix) {
        last = new TransportError(
          "WebSocket over UDS is not supported by the global WebSocket implementation",
        );
        continue;
      }
      const url = new URL(path.startsWith("/") ? `${entry.url}${path}` : `${entry.url}/${path}`);
      url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
      if (options.query instanceof URLSearchParams) {
        options.query.forEach((value, key) => {
          url.searchParams.append(key, value);
        });
      } else if (options.query)
        for (const key in options.query) url.searchParams.set(key, options.query[key]);
      if (this.token) url.searchParams.set("token", this.token);
      try {
        const socket = new this.#websocket(url);
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(
            () => reject(new Error("WebSocket connect timeout")),
            this.#timeout,
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
              reject(new Error("WebSocket connect failed"));
            },
            { once: true },
          );
        });
        entry.failedUntil = 0;
        const index = this.#roster.indexOf(entry);
        if (index >= 0) this.#preferred = index;
        return [socket, entry.url];
      } catch (error) {
        entry.failedUntil = this.#now() + ENDPOINT_COOLDOWN;
        last = new TransportError(`WebSocket connection to ${entry.url} failed`, { cause: error });
      }
    }
    throw last ?? new TransportError("no usable endpoints");
  }

  /** Locate a sandbox by probing the endpoint roster. */
  async resolveSandbox(id: string, hint?: string): Promise<string> {
    let original: APIError | undefined;
    for (const entry of this.#candidates(hint)) {
      try {
        const result = await this.#call(
          SandboxService.method.get,
          { id },
          { endpoint: entry.url },
          false,
        );
        return result.endpoint;
      } catch (error) {
        if (error instanceof TransportError) continue;
        if (!(error instanceof APIError)) throw error;
        if (!original) original = error;
        if (error.code !== "not_found" && error.status !== 404) throw error;
      }
    }
    throw (
      original ??
      new APIError({ status: 404, code: "not_found", message: `sandbox ${id} not found` })
    );
  }

  /** Refresh discovered mesh endpoints. */
  async refresh(force = false): Promise<void> {
    if (!this.#discover || this.#closed) return;
    if (!force && this.#lastRefresh !== 0 && this.#now() - this.#lastRefresh < ROSTER_TTL) return;
    if (this.#refreshing) return this.#refreshing;
    this.#refreshing = (async () => {
      try {
        const result = await this.#call(SystemService.method.meshStatus, {}, {}, false);
        const status: unknown = JSON.parse(result.message.json);
        if (!isRecord(status)) return;
        const advertised: string[] = [];
        const self = status.self;
        if (isRecord(self) && typeof self.advertise === "string") advertised.push(self.advertise);
        if (typeof status.advertise === "string") advertised.push(status.advertise);
        if (Array.isArray(status.peers))
          for (const peer of status.peers)
            if (isRecord(peer) && typeof peer.advertise === "string")
              advertised.push(peer.advertise);
        const seeds = this.#roster.filter((entry) => entry.source === "seed");
        const known = new Set(seeds.map((entry) => entry.url));
        const discovered: RosterEntry[] = [];
        for (const value of advertised) {
          let url: string;
          try {
            url = new URL(value).toString().replace(/\/$/, "");
          } catch {
            continue;
          }
          if (!known.has(url)) {
            known.add(url);
            const old = this.#roster.find((entry) => entry.url === url);
            discovered.push({ url, source: "discovered", failedUntil: old?.failedUntil ?? 0 });
          }
        }
        const preferredUrl = this.#roster[this.#preferred]?.url;
        this.#roster = [...seeds, ...discovered];
        const preferred = this.#roster.findIndex((entry) => entry.url === preferredUrl);
        this.#preferred = preferred < 0 ? 0 : preferred;
      } catch {
        // Discovery is opportunistic; the active roster remains usable.
      } finally {
        this.#lastRefresh = this.#now();
        this.#refreshing = null;
      }
    })();
    return this.#refreshing;
  }

  /** Close the driver. */
  close(): void {
    this.#closed = true;
  }

  #candidates(hint?: string): RosterEntry[] {
    const result: RosterEntry[] = [];
    const append = (entry: RosterEntry | undefined) => {
      if (entry && !result.includes(entry)) result.push(entry);
    };
    if (hint) append(this.#roster.find((entry) => entry.url === hint));
    append(this.#roster[this.#preferred]);
    for (const entry of this.#roster) append(entry);
    return result;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

/** A failure eligible for endpoint failover: no server trailer and a transport-shaped code. */
function isTransportFailure(error: ConnectError): boolean {
  if (error.metadata.get("vmon-code") !== null) return false;
  return (
    error.code === Code.Unavailable ||
    error.code === Code.Unknown ||
    error.code === Code.DeadlineExceeded
  );
}

/** Re-yield RPC stream messages, mapping failures to SDK error types. */
async function* rpcMessages<T>(source: AsyncIterable<T>): AsyncGenerator<T> {
  try {
    yield* source;
  } catch (error) {
    const failure = ConnectError.from(error);
    if (failure.code === Code.Canceled) return;
    if (isTransportFailure(failure)) throw new TransportError("stream failed", { cause: error });
    throw rpcError(failure);
  }
}
