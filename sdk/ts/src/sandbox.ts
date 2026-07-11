import type { Client } from "./client";
import type { DriverRequestOptions, DriverResponse } from "./driver";
import { APIError, apiError, ProtocolError } from "./errors";
import type {
  ExecRequest,
  ExecResult,
  FileInfo,
  NetworkPolicy,
  SandboxInfo,
  SnapshotFilesystemRequest,
  SnapshotRequest,
  TunnelSet,
} from "./models";
import { ConsoleStream, LogStream, Process, type ProcessOptions } from "./process";
import { mergeSecretEnv, type SecretInput } from "./values";

/** Options for captured command execution. */
export interface RunOptions extends Omit<ExecRequest, "cmd"> {
  secrets?: Iterable<SecretInput> | null;
}
/** Options for streaming command execution. */
export interface ExecOptions extends RunOptions, ProcessOptions {}
/** Readiness probe and polling options. */
export interface WaitReadyOptions {
  probe?: number | string | { port: number };
  timeout?: number;
  interval?: number;
}
/** Sandbox list filters. */
export interface SandboxListOptions {
  tags?: Record<string, string>;
  node?: string;
}

/** A sandbox bound to its client and serving mesh endpoint. */
export class Sandbox {
  readonly #client: Client;
  #info: SandboxInfo;
  #endpoint?: string;
  #connectToken?: string;
  readonly files: Files;
  readonly ports: Ports;
  /** Bind a sandbox view or identifier to a client and endpoint. */
  constructor(client: Client, info: SandboxInfo | string, endpoint?: string) {
    this.#client = client;
    this.#info = typeof info === "string" ? { id: info } : info;
    this.#endpoint = endpoint;
    this.files = new Files(this);
    this.ports = new Ports(this);
  }
  /** Stable sandbox identifier. */
  get id(): string {
    return this.#info.id;
  }
  /** Latest typed sandbox view. */
  get info(): Readonly<SandboxInfo> {
    return this.#info;
  }
  /** Mesh node that reported the sandbox. */
  get node(): string | null {
    return typeof this.#info.node === "string" ? this.#info.node : null;
  }
  /** Preferred serving endpoint. */
  get endpoint(): string | undefined {
    return this.#endpoint;
  }
  /** Owning root client. */
  get client(): Client {
    return this.#client;
  }

  /** Refresh and merge the sandbox view. */
  async refresh(): Promise<SandboxInfo> {
    const response = await this.request("GET", "");
    this.#info = mergeInfo(this.#info, await response.json());
    return this.#info;
  }
  /** Run a command and capture its output. */
  async run(cmd: string[], options: RunOptions = {}): Promise<ExecResult> {
    const { secrets, ...request } = options;
    const body: ExecRequest = { ...request, cmd };
    if (secrets) body.env = { ...(body.env ?? {}), ...mergeSecretEnv(secrets) };
    return this.json("POST", "/exec", { json: body });
  }
  /** Open a streaming command process. */
  async exec(cmd: string[], options: ExecOptions = {}): Promise<Process> {
    const { secrets, onStdout, onStderr, ...request } = options;
    const body: ExecRequest = { ...request, cmd };
    if (secrets) body.env = { ...(body.env ?? {}), ...mergeSecretEnv(secrets) };
    const [socket] = await this.websocket("/exec");
    return new Process(socket, body, { onStdout, onStderr });
  }
  /** Attach a read-only console stream. */
  async attach(): Promise<ConsoleStream> {
    const [socket] = await this.websocket("/attach");
    return new ConsoleStream(socket);
  }
  /** Fetch current logs as text. */
  async logs(): Promise<string> {
    const response = await this.request("GET", "/logs", { params: { follow: false } });
    return response.text();
  }
  /** Follow logs as a stream. */
  async followLogs(): Promise<LogStream> {
    const response = await this.request("GET", "/logs", { params: { follow: true }, stream: true });
    return new LogStream(response);
  }
  /** Fetch sandbox metrics. */
  metrics(): Promise<Record<string, unknown>> {
    return this.json("GET", "/metrics");
  }
  /** Gracefully stop the sandbox. */
  async stop(wait = true): Promise<SandboxInfo> {
    const result = await this.json<SandboxInfo>("POST", "/stop");
    this.#info = result;
    if (wait) await this.#waitForState(new Set(["stopped", "exited"]));
    return this.#info;
  }
  /** Forcefully terminate the sandbox. */
  async terminate(wait = true): Promise<void> {
    await (await this.request("POST", "/terminate")).arrayBuffer();
    if (wait) await this.#waitForState(new Set(["terminated", "stopped", "exited"]));
  }
  /** Remove the sandbox record. */
  async remove(): Promise<void> {
    await this.request("DELETE", "");
  }
  /** Pause sandbox execution. */
  async pause(): Promise<SandboxInfo> {
    this.#info = await this.json("POST", "/pause");
    return this.#info;
  }
  /** Resume sandbox execution. */
  async resume(): Promise<SandboxInfo> {
    this.#info = await this.json("POST", "/resume");
    return this.#info;
  }
  /** Extend the sandbox lease. */
  async extend(secs: number): Promise<SandboxInfo> {
    this.#info = mergeInfo(this.#info, await this.json("POST", "/extend", { json: { secs } }));
    return this.#info;
  }
  /** Migrate and re-pin the sandbox. */
  async migrate(target: string): Promise<SandboxInfo> {
    this.#info = mergeInfo(this.#info, await this.json("POST", "/migrate", { json: { target } }));
    this.#endpoint = await this.#client.driver.resolveSandbox(this.id, this.#endpoint);
    return this.#info;
  }
  /** Create a full VM snapshot. */
  async snapshot(name?: string, stop = false): Promise<string> {
    const body: SnapshotRequest = { name: name ?? null, stop };
    return responseField(await this.json("POST", "/snapshots", { json: body }), "snapshot");
  }
  /** Create a filesystem snapshot. */
  async snapshotFilesystem(name?: string): Promise<string> {
    const body: SnapshotFilesystemRequest = { name: name ?? null };
    return responseField(await this.json("POST", "/snapshots/fs", { json: body }), "image");
  }
  /** Fetch the network policy. */
  network(): Promise<NetworkPolicy> {
    return this.json("GET", "/network");
  }
  /** Replace the network policy. */
  setNetwork(policy: NetworkPolicy): Promise<NetworkPolicy> {
    return this.json("PUT", "/network", { json: policy });
  }
  /** Fetch tunnels and cache the proxy token. */
  async tunnels(): Promise<TunnelSet> {
    const value = await this.json<TunnelSet>("GET", "/tunnels");
    if (typeof value.connect_token === "string") this.#connectToken = value.connect_token;
    return value;
  }
  /** Wait for a command or port readiness probe. */
  async waitReady(options: WaitReadyOptions = {}): Promise<SandboxInfo> {
    if (options.probe === undefined) return this.#info;
    const deadline = Date.now() + (options.timeout ?? 300) * 1_000;
    const interval = options.interval ?? 100;
    let lastError: unknown;
    while (Date.now() < deadline) {
      try {
        const port =
          typeof options.probe === "number"
            ? options.probe
            : typeof options.probe === "object"
              ? options.probe.port
              : undefined;
        if (port !== undefined) {
          const response = await this.ports.http(port, "GET");
          if (!response.ok) throw new Error(`readiness port returned HTTP ${response.status}`);
        } else {
          const result = await this.run(["sh", "-lc", String(options.probe)], { timeout: 5 });
          if (result.exit !== 0) throw new Error(`readiness command exited with ${result.exit}`);
        }
        return this.#info;
      } catch (error) {
        lastError = error;
        await sleep(interval);
      }
    }
    throw new Error(
      `sandbox readiness probe timed out${lastError instanceof Error ? `: ${lastError.message}` : ""}`,
      lastError === undefined ? undefined : { cause: lastError },
    );
  }
  /** Resolve and cache the proxy connect token. */
  async connectToken(): Promise<string> {
    if (!this.#connectToken) await this.tunnels();
    if (!this.#connectToken) throw new ProtocolError("server did not return a connect token");
    return this.#connectToken;
  }
  /** Encoded API path for this sandbox. */
  get path(): string {
    return `/v1/sandboxes/${encodeURIComponent(this.id)}`;
  }
  /** Perform a sandbox request and decode JSON. */
  async json<T>(method: string, suffix: string, options: DriverRequestOptions = {}): Promise<T> {
    const response = await this.request(method, suffix, options);
    return response.json();
  }
  /** Perform a sandbox request and reject API errors. */
  async request(
    method: string,
    suffix: string,
    options: DriverRequestOptions = {},
  ): Promise<DriverResponse> {
    const response = await this.requestRaw(method, suffix, options);
    if (!response.ok) throw await apiError(response);
    return response;
  }
  /** Perform a sandbox request while preserving downstream HTTP statuses. */
  async requestRaw(
    method: string,
    suffix: string,
    options: DriverRequestOptions = {},
  ): Promise<DriverResponse> {
    const execute = async () => {
      const response = await this.#client.driver.request(method, `${this.path}${suffix}`, {
        ...options,
        endpoint: this.#endpoint,
      });
      this.#endpoint = response.endpoint;
      return response;
    };
    const response = await execute();
    if (response.status !== 404 || this.#client.driver.endpoints().length <= 1) return response;
    const error = await apiError(response.clone());
    if (error.code !== "not_found") return response;
    this.#endpoint = await this.#client.driver.resolveSandbox(this.id, this.#endpoint);
    return execute();
  }
  /** Open an affinity-aware sandbox WebSocket with relocation. */
  async websocket(
    suffix: string,
    query?: URLSearchParams | Record<string, string>,
  ): Promise<[WebSocket, string]> {
    const execute = async () => {
      const result = await this.#client.driver.websocket(`${this.path}${suffix}`, {
        query,
        endpoint: this.#endpoint,
      });
      this.#endpoint = result[1];
      return result;
    };
    try {
      return await execute();
    } catch (error) {
      if (
        !(error instanceof APIError) ||
        error.code !== "not_found" ||
        this.#client.driver.endpoints().length <= 1
      )
        throw error;
      this.#endpoint = await this.#client.driver.resolveSandbox(this.id, this.#endpoint);
      return execute();
    }
  }
  async #waitForState(states: Set<string>): Promise<void> {
    const deadline = Date.now() + 300_000;
    while (Date.now() < deadline) {
      const info = await this.refresh();
      const state = typeof info.state === "string" ? info.state : info.status;
      if (state && states.has(state)) return;
      await sleep(250);
    }
    throw new Error(`sandbox state wait timed out after 300 seconds`);
  }
}

/** Bound guest filesystem operations. */
export class Files {
  readonly #sandbox: Sandbox;
  /** Bind filesystem operations to a sandbox. */
  constructor(sandbox: Sandbox) {
    this.#sandbox = sandbox;
  }
  /** Read raw guest file bytes. */
  async read(path: string): Promise<Uint8Array> {
    const response = await this.#sandbox.request("GET", "/files", { params: { path } });
    return new Uint8Array(await response.arrayBuffer());
  }
  /** Read a UTF-8 guest file. */
  async readText(path: string): Promise<string> {
    return new TextDecoder().decode(await this.read(path));
  }
  /** Write raw guest file content. */
  async write(path: string, content: BodyInit): Promise<void> {
    await this.#sandbox.request("PUT", "/files", {
      params: { path },
      content,
      headers: { "Content-Type": "application/octet-stream" },
    });
  }
  /** Write UTF-8 guest file content. */
  writeText(path: string, content: string): Promise<void> {
    return this.write(path, content);
  }
  /** List guest directory entries. */
  async list(path = "."): Promise<FileInfo[]> {
    const body = await this.#sandbox.json<unknown>("GET", "/files/list", { params: { path } });
    if (!isRecord(body) || !Array.isArray(body.entries))
      throw new ProtocolError("file list response did not include entries");
    return body.entries.filter(isFileInfo);
  }
  /** Stat a guest filesystem path. */
  stat(path: string): Promise<FileInfo> {
    return this.#sandbox.json("GET", "/files/stat", { params: { path } });
  }
  /** Delete a guest path, optionally recursively. */
  async delete(path: string, recursive = false): Promise<void> {
    await this.#sandbox.request("DELETE", "/files", { params: { path, recursive } });
  }
  /** Create a guest directory and its parents. */
  async mkdir(path: string): Promise<void> {
    const result = await this.#sandbox.run(["mkdir", "-p", "--", path]);
    if (result.exit !== 0)
      throw new APIError({
        status: 0,
        code: "engine",
        message: new TextDecoder().decode(decode(result.stderr_b64)),
      });
  }
  /** Open a streaming guest file reader. */
  async open(path: string): Promise<ReadableStream<Uint8Array>> {
    const response = await this.#sandbox.request("GET", "/files", {
      params: { path },
      stream: true,
    });
    if (!response.body) throw new ProtocolError("file response has no body");
    return response.body;
  }
}

/** Bound HTTP and WebSocket port proxy operations. */
export class Ports {
  readonly #sandbox: Sandbox;
  /** Bind port proxies to a sandbox. */
  constructor(sandbox: Sandbox) {
    this.#sandbox = sandbox;
  }
  /** Proxy an HTTP request to an exposed guest port. */
  async http(
    port: number,
    method: string,
    path = "",
    options: Omit<DriverRequestOptions, "endpoint"> = {},
  ): Promise<Response> {
    validatePort(port);
    const params = sanitizedParams(options.params);
    const token = await this.#sandbox.connectToken();
    if (token) params.set("connect_token", token);
    return this.#sandbox.requestRaw(method, `/ports/${port}/${path.replace(/^\//, "")}`, {
      ...options,
      params,
    });
  }
  /** Proxy a WebSocket to an exposed guest port. */
  async websocket(
    port: number,
    path = "",
    query: URLSearchParams | Record<string, string> = {},
  ): Promise<WebSocket> {
    validatePort(port);
    const params = sanitizedParams(query);
    const token = await this.#sandbox.connectToken();
    if (token) params.set("connect_token", token);
    const [socket] = await this.#sandbox.websocket(
      `/ports/${port}/ws/${path.replace(/^\//, "")}`,
      params,
    );
    return socket;
  }
}

function validatePort(port: number): void {
  if (!Number.isInteger(port) || port < 0 || port > 65_535)
    throw new RangeError("port must be an integer between 0 and 65535");
}

async function sleep(milliseconds: number): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  setTimeout(resolve, milliseconds);
  return promise;
}

function sanitizedParams(
  input?: URLSearchParams | Record<string, string | number | boolean | null | undefined>,
): URLSearchParams {
  const params = new URLSearchParams();
  if (input instanceof URLSearchParams) {
    input.forEach((value, key) => {
      params.append(key, value);
    });
  } else if (input)
    for (const key in input) {
      const value = input[key];
      if (value !== null && value !== undefined) params.set(key, String(value));
    }
  params.delete("connect_token");
  params.delete("token");
  params.delete("access_token");
  return params;
}
function responseField(value: unknown, field: "snapshot" | "image"): string {
  if (isRecord(value) && typeof value[field] === "string") return value[field];
  throw new ProtocolError(`snapshot response did not include ${field}`);
}
function mergeInfo(current: SandboxInfo, value: unknown): SandboxInfo {
  if (!isRecord(value)) throw new ProtocolError("sandbox response must be an object");
  return { ...current, ...value };
}
function isFileInfo(value: unknown): value is FileInfo {
  return isRecord(value);
}
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
function decode(value: string): Uint8Array {
  if (typeof Buffer !== "undefined") return new Uint8Array(Buffer.from(value, "base64"));
  return Uint8Array.from(atob(value), (character) => character.charCodeAt(0));
}
