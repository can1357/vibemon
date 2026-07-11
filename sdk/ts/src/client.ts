import { type Driver, type DriverOptions, type DriverRequestOptions, MeshDriver } from "./driver";
import { apiError, ProtocolError, TransportError } from "./errors";
import type {
  ForkRequest,
  Health,
  MeshNode,
  MeshStatus,
  PoolSetRequest,
  PoolStats,
  RestoreRequest,
  SandboxCreateRequest,
  SandboxInfo,
  ServerInfo,
} from "./models";
import { EventStream, Process } from "./process";
import type {
  JsonValue,
  RemoteCallable,
  RemoteFunction,
  RemoteFunctionOptions,
  RemoteFunctionSourceSpec,
} from "./remote";
import { remoteFunction, remoteFunctionFromSource } from "./remote";
import { Sandbox, type SandboxListOptions } from "./sandbox";
import { type SecretInput, secretWires, Volume } from "./values";

/** Options accepted by the synchronous connect factory. */
export interface ConnectOptions extends DriverOptions {}
/** Sandbox creation body with typed secret value objects. */
export type SandboxCreateRequestWithSecrets = Omit<SandboxCreateRequest, "secrets"> & {
  secrets?: Iterable<SecretInput> | null;
};

/** Root SDK object exposing service namespaces and daemon-wide operations. */
export class Client {
  readonly driver: Driver;
  readonly sandboxes: SandboxAPI;
  readonly snapshots: SnapshotAPI;
  readonly volumes: VolumeAPI;
  readonly pools: PoolAPI;
  readonly mesh: MeshAPI;
  /** Bind the client to a driver implementation. */
  constructor(driver: Driver) {
    this.driver = driver;
    this.sandboxes = new SandboxAPI(this);
    this.snapshots = new SnapshotAPI(this);
    this.volumes = new VolumeAPI(this);
    this.pools = new PoolAPI(this);
    this.mesh = new MeshAPI(this);
  }
  /** Check daemon health. */
  health(): Promise<Health> {
    return this.json("GET", "/healthz");
  }
  /** Fetch daemon and node information. */
  info(): Promise<ServerInfo> {
    return this.json("GET", "/v1/info");
  }
  /** Fetch Prometheus metrics text. */
  async metrics(): Promise<string> {
    return (await this.response("GET", "/metrics")).text();
  }
  /** Fetch the daemon OpenAPI document. */
  openapi(): Promise<Record<string, unknown>> {
    return this.json("GET", "/v1/openapi.json");
  }
  /** Open the daemon event stream. */
  async events(): Promise<EventStream> {
    return new EventStream(await this.response("GET", "/v1/events", { stream: true }));
  }
  /** Open an interactive shell process after its ready envelope. */
  async shell(request: Record<string, unknown> = {}): Promise<Process> {
    const [socket] = await this.driver.websocket("/v1/shell");
    const process = new Process(socket, request);
    await process.ready();
    return process;
  }
  /** Package a source-serializable function for remote execution. */
  remoteFunction<Arguments extends unknown[], Result>(
    fn: RemoteCallable<Arguments, Result>,
    options: RemoteFunctionOptions = {},
  ): RemoteFunction<Arguments, Result> {
    return remoteFunction(this, fn, options);
  }
  /** Create a remote function from JavaScript module source. */
  remoteFunctionFromSource<Arguments extends unknown[] = JsonValue[], Result = JsonValue>(
    spec: RemoteFunctionSourceSpec,
    options: RemoteFunctionOptions = {},
  ): RemoteFunction<Arguments, Result> {
    return remoteFunctionFromSource<Arguments, Result>(this, spec, options);
  }
  /** Close the underlying driver. */
  close(): void | Promise<void> {
    return this.driver.close();
  }
  /** Perform an authenticated request and reject API errors. */
  async response(method: string, path: string, options: DriverRequestOptions = {}) {
    const response = await this.driver.request(method, path, options);
    if (!response.ok) throw await apiError(response);
    return response;
  }
  /** Perform a request and decode its JSON response. */
  async json<T>(method: string, path: string, options: DriverRequestOptions = {}): Promise<T> {
    return (await this.response(method, path, options)).json();
  }
}

/** Create a client backed by a lazily discovering mesh driver. */
export function connect(dsn?: string, options: ConnectOptions = {}): Client {
  return new Client(new MeshDriver(dsn, options));
}

/** Sandbox collection operations. */
export class SandboxAPI {
  readonly #client: Client;
  /** Bind sandbox operations to a client. */
  constructor(client: Client) {
    this.#client = client;
  }
  /** Create a sandbox. */
  async create(request: SandboxCreateRequestWithSecrets): Promise<Sandbox> {
    const { secrets, ...rest } = request;
    const body: SandboxCreateRequest =
      secrets === undefined
        ? rest
        : { ...rest, secrets: secrets === null ? null : secretWires(secrets) };
    const response = await this.#client.response("POST", "/v1/sandboxes", { json: body });
    return new Sandbox(this.#client, await response.json(), response.endpoint);
  }
  /** Fetch a sandbox and pin its serving endpoint. */
  async get(id: string): Promise<Sandbox> {
    const response = await this.#client.response("GET", `/v1/sandboxes/${encodeURIComponent(id)}`);
    return new Sandbox(this.#client, await response.json(), response.endpoint);
  }
  /** Create an unfetched bound sandbox reference. */
  ref(id: string): Sandbox {
    return new Sandbox(this.#client, id);
  }
  /** List and merge sandboxes across live mesh endpoints. */
  async list(options: SandboxListOptions = {}): Promise<Sandbox[]> {
    const endpoints = this.#client.driver.endpoints().filter((entry) => entry.healthy);
    const params = new URLSearchParams();
    if (options.tags)
      for (const key in options.tags) params.append("tag", `${key}=${options.tags[key]}`);
    if (endpoints.length <= 1) {
      const response = await this.#client.response("GET", "/v1/sandboxes", { params });
      return sandboxRows(await response.json())
        .filter((row) => !options.node || row.node === options.node)
        .map((row) => new Sandbox(this.#client, row, response.endpoint));
    }
    const attempts = await Promise.allSettled(
      endpoints.map(async (entry) => {
        const response = await this.#client.driver.request("GET", "/v1/sandboxes", {
          params,
          endpoint: entry.url,
        });
        if (!response.ok) throw await apiError(response);
        return { rows: sandboxRows(await response.json()), endpoint: response.endpoint };
      }),
    );
    const merged = new Map<string, Sandbox>();
    let lastTransportError: TransportError | undefined;
    for (const attempt of attempts) {
      if (attempt.status === "rejected") {
        if (!(attempt.reason instanceof TransportError)) throw attempt.reason;
        lastTransportError = attempt.reason;
        continue;
      }
      for (const row of attempt.value.rows)
        if (!merged.has(row.id) && (!options.node || row.node === options.node))
          merged.set(row.id, new Sandbox(this.#client, row, attempt.value.endpoint));
    }
    if (attempts.every((attempt) => attempt.status === "rejected")) throw lastTransportError;
    return [...merged.values()];
  }
}

/** Snapshot collection, restore, and fork operations. */
export class SnapshotAPI {
  readonly #client: Client;
  /** Bind snapshot operations to a client. */
  constructor(client: Client) {
    this.#client = client;
  }
  /** List snapshot names. */
  async list(): Promise<string[]> {
    const body = await this.#client.json<{ snapshots: string[] }>("GET", "/v1/snapshots");
    return body.snapshots;
  }
  /** Restore one snapshot into a sandbox. */
  async restore(name: string, request: RestoreRequest = {}): Promise<Sandbox> {
    const response = await this.#client.response(
      "POST",
      `/v1/snapshots/${encodeURIComponent(name)}/restore`,
      { json: request },
    );
    return new Sandbox(this.#client, await response.json(), response.endpoint);
  }
  /** Fork one snapshot into multiple sandboxes. */
  async fork(name: string, request: ForkRequest): Promise<Sandbox[]> {
    const response = await this.#client.response(
      "POST",
      `/v1/snapshots/${encodeURIComponent(name)}/fork`,
      { json: request },
    );
    const body = await response.json();
    return sandboxRows(isRecord(body) ? body.clones : undefined).map(
      (row) => new Sandbox(this.#client, row, response.endpoint),
    );
  }
}

/** Persistent volume collection operations. */
export class VolumeAPI {
  readonly #client: Client;
  /** Bind volume operations to a client. */
  constructor(client: Client) {
    this.#client = client;
  }
  /** List persistent volumes. */
  async list(): Promise<Volume[]> {
    return (await this.#client.json<{ volumes: string[] }>("GET", "/v1/volumes")).volumes.map(
      (name) => new Volume(name),
    );
  }
  /** Create a persistent volume. */
  async create(name: string): Promise<Volume> {
    await this.#client.response("PUT", `/v1/volumes/${encodeURIComponent(name)}`);
    return new Volume(name);
  }
  /** Delete a persistent volume. */
  async delete(name: string): Promise<void> {
    await this.#client.response("DELETE", `/v1/volumes/${encodeURIComponent(name)}`);
  }
}

/** Bound warm-pool value returned by pool operations. */
export class Pool {
  readonly #api: PoolAPI;
  readonly ref: string;
  readonly count: number;
  readonly stats?: PoolStats;
  /** Bind a pool result to its service. */
  constructor(api: PoolAPI, ref: string, count: number, stats?: PoolStats) {
    this.#api = api;
    this.ref = ref;
    this.count = count;
    this.stats = stats;
  }
  /** Delete this warm pool. */
  delete(): Promise<void> {
    return this.#api.delete(this.ref);
  }
}
/** Warm-pool collection operations. */
export class PoolAPI {
  readonly #client: Client;
  /** Bind pool operations to a client. */
  constructor(client: Client) {
    this.#client = client;
  }
  /** List warm pools. */
  async list(): Promise<Pool[]> {
    const body = await this.#client.json<unknown>("GET", "/v1/pools");
    if (!isRecord(body) || Array.isArray(body))
      throw new ProtocolError("pool list response must be an object");
    const pools: Pool[] = [];
    for (const ref in body) {
      const stats = body[ref];
      if (!isRecord(stats) || Array.isArray(stats))
        throw new ProtocolError(`pool ${ref} statistics must be an object`);
      const count = Number(stats.count ?? stats.size ?? 0);
      if (!Number.isFinite(count)) throw new ProtocolError(`pool ${ref} size must be finite`);
      pools.push(new Pool(this, ref, count, stats));
    }
    return pools;
  }
  /** Set the desired warm-pool size and template. */
  async set(ref: string, count: number, template: Record<string, unknown> = {}): Promise<Pool> {
    const body: PoolSetRequest = { ...template, size: count };
    const result = await this.#client.json<PoolStats>(
      "PUT",
      `/v1/pools/${encodeURIComponent(ref)}`,
      { json: body },
    );
    return new Pool(this, ref, count, result);
  }
  /** Delete a warm pool. */
  async delete(ref: string): Promise<void> {
    await this.#client.response("DELETE", `/v1/pools/${encodeURIComponent(ref)}`);
  }
  /** Delete every warm pool. */
  async clear(): Promise<void> {
    const pools = await this.list();
    await Promise.all(pools.map((pool) => this.delete(pool.ref)));
  }
}

/** Mesh status and node views. */
export class MeshAPI {
  readonly #client: Client;
  /** Bind mesh operations to a client. */
  constructor(client: Client) {
    this.#client = client;
  }
  /** Fetch typed mesh status. */
  status(): Promise<MeshStatus> {
    return this.#client.json("GET", "/v1/mesh/status");
  }
  /** Return the local and peer mesh nodes. */
  async nodes(): Promise<MeshNode[]> {
    const status = await this.status();
    return [status.self, ...status.peers];
  }
}

function sandboxRows(body: unknown): SandboxInfo[] {
  const rows = Array.isArray(body)
    ? body
    : body && typeof body === "object" && "sandboxes" in body && Array.isArray(body.sandboxes)
      ? body.sandboxes
      : body && typeof body === "object" && "items" in body && Array.isArray(body.items)
        ? body.items
        : [];
  return rows.filter((row): row is SandboxInfo => isRecord(row) && typeof row.id === "string");
}
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
