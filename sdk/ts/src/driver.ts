import { type DsnConfig, type DsnEndpoint, type ParseDsnOptions, parseDsn } from "./dsn";
import { APIError, apiError, TransportError } from "./errors";

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
/** Transport seam consumed by Client and bound resources. */
export interface Driver {
  /** Bearer token used by HTTP and WebSocket requests. */
  readonly token?: string;
  /** Perform one affinity-aware HTTP request. */
  request(method: string, path: string, options?: DriverRequestOptions): Promise<DriverResponse>;
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

  /** Build a driver from a DSN or parsed configuration. */
  constructor(dsn?: string | null | DsnConfig, options: DriverOptions = {}) {
    const config = typeof dsn === "object" && dsn !== null ? dsn : parseDsn(dsn, options);
    this.token = options.token ?? config.token;
    this.#fetch = options.fetch ?? globalThis.fetch;
    this.#websocket = options.websocket ?? globalThis.WebSocket;
    this.#discover = options.discover ?? config.discover;
    this.#timeout = (options.timeout ?? config.timeout) * 1_000;
    this.#now = options.now ?? Date.now;
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
      let response: DriverResponse;
      try {
        response = await this.#request(
          "GET",
          `/v1/sandboxes/${encodeURIComponent(id)}`,
          { endpoint: entry.url },
          false,
        );
      } catch (error) {
        if (error instanceof TransportError) continue;
        throw error;
      }
      if (response.status === 200) return response.endpoint;
      const error = await apiError(response);
      if (!original) original = error;
      if (error.code !== "not_found" && response.status !== 404) throw error;
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
        const response = await this.#request("GET", "/v1/mesh/status", {}, false);
        if (!response.ok) return;
        const status: unknown = await response.json();
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
