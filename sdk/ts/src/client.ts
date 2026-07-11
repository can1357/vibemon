import type { components, operations } from "./schema";

type JsonResponse<
  Operation extends keyof operations,
  Status extends keyof operations[Operation]["responses"],
> = operations[Operation]["responses"][Status] extends {
  content: { "application/json": infer Body };
}
  ? Body
  : never;

type JsonRequest<Operation extends keyof operations> = operations[Operation] extends {
  requestBody: { content: { "application/json": infer Body } };
}
  ? Body
  : never;

export interface VmonClientOptions {
  baseUrl: string;
  token: string;
}

export interface VmonApiErrorShape {
  status: number;
  code: string;
  message: string;
}

export interface SecretWire {
  name: string;
  values: Record<string, string>;
}

export type SecretInput = Secret | SecretWire | Record<string, string>;

export type HealthBody = JsonResponse<"healthz", 200>;
export type SandboxCreateRequest = JsonRequest<"create_sandbox">;
export type SandboxCreateRequestWithSecrets = Omit<SandboxCreateRequest, "secrets"> & {
  secrets?: Iterable<SecretInput> | null;
};
export type SandboxListBody = JsonResponse<"list_sandboxes", 200>;
export type SandboxView = SandboxListBody["sandboxes"][number];
export type ExecSandboxRequest = JsonRequest<"exec_capture">;
export type ExecSandboxResponse = JsonResponse<"exec_capture", 200>;
export interface ExecSandboxOptions {
  secrets?: Iterable<SecretInput> | null;
}
export type VolumeName = JsonResponse<"volume_list", 200>["volumes"][number];
export type OkBody = components["schemas"]["OkBody"];

export class VmonApiError extends Error implements VmonApiErrorShape {
  override readonly name = "VmonApiError";
  readonly status: number;
  readonly code: string;

  constructor(error: VmonApiErrorShape) {
    super(error.message);
    this.status = error.status;
    this.code = error.code;
  }
}

export class Secret {
  readonly name: string;
  readonly values: Readonly<Record<string, string>>;

  constructor(values: Record<string, string> = {}, name = "secret") {
    this.name = checkedSecretName(name, "secret name");
    this.values = Object.freeze(checkedSecretEnv(values));
  }

  static fromDict(values: Record<string, string>, name = "secret"): Secret {
    return new Secret(values, name);
  }

  static fromEnv(names: Iterable<string>, name = "env"): Secret {
    const values: Record<string, string> = {};
    for (const rawName of names) {
      const envName = checkedSecretName(rawName, "secret environment names");
      const value = typeof process === "undefined" ? undefined : process.env[envName];
      if (value !== undefined) {
        values[envName] = checkedSecretValue(value);
      }
    }
    return new Secret(values, name);
  }

  names(): string[] {
    const names = Object.keys(this.values);
    names.sort();
    return names;
  }

  asEnv(): Record<string, string> {
    return { ...this.values };
  }

  toWire(): SecretWire {
    return { name: this.name, values: this.asEnv() };
  }
}

export class VmonClient {
  readonly #baseUrl: string;
  readonly #token: string;

  constructor(options: VmonClientOptions) {
    this.#baseUrl = normalizeBaseUrl(options.baseUrl);
    this.#token = options.token;
  }

  health(): Promise<HealthBody> {
    return this.#requestJson("/healthz");
  }

  async listSandboxes(): Promise<SandboxView[]> {
    const body = await this.#requestJson<SandboxListBody>("/v1/sandboxes");
    return body.sandboxes;
  }

  getSandbox(id: string): Promise<SandboxView> {
    return this.#requestJson(`/v1/sandboxes/${encodeURIComponent(id)}`);
  }

  createSandbox(request: SandboxCreateRequestWithSecrets): Promise<SandboxView> {
    return this.#requestJson("/v1/sandboxes", jsonRequest("POST", sandboxCreateBody(request)));
  }

  removeSandbox(id: string): Promise<SandboxView> {
    return this.#requestJson(`/v1/sandboxes/${encodeURIComponent(id)}`, { method: "DELETE" });
  }

  stopSandbox(id: string): Promise<SandboxView> {
    return this.#requestJson(`/v1/sandboxes/${encodeURIComponent(id)}/stop`, { method: "POST" });
  }

  execSandbox(
    id: string,
    request: ExecSandboxRequest,
    options: ExecSandboxOptions = {},
  ): Promise<ExecSandboxResponse> {
    return this.#requestJson(
      `/v1/sandboxes/${encodeURIComponent(id)}/exec`,
      jsonRequest("POST", execRequestBody(request, options.secrets)),
    );
  }

  async listVolumes(): Promise<VolumeName[]> {
    const body = await this.#requestJson<JsonResponse<"volume_list", 200>>("/v1/volumes");
    return body.volumes;
  }

  createVolume(name: string): Promise<OkBody> {
    return this.#requestJson(`/v1/volumes/${encodeURIComponent(name)}`, { method: "PUT" });
  }

  removeVolume(name: string): Promise<OkBody> {
    return this.#requestJson(`/v1/volumes/${encodeURIComponent(name)}`, { method: "DELETE" });
  }

  async #requestJson<T>(path: string, init?: RequestInit): Promise<T> {
    const response = await fetch(this.#url(path), this.#withAuth(init));
    if (!response.ok) {
      throw await apiError(response);
    }
    return response.json();
  }

  #url(path: string): string {
    if (path.startsWith("/")) {
      return `${this.#baseUrl}${path}`;
    }
    return `${this.#baseUrl}/${path}`;
  }

  #withAuth(init?: RequestInit): RequestInit {
    const headers = new Headers(init?.headers);
    headers.set("Authorization", `Bearer ${this.#token}`);
    return { ...init, headers };
  }
}

function sandboxCreateBody(request: SandboxCreateRequestWithSecrets): SandboxCreateRequest {
  const { secrets, ...body } = request;
  if (secrets === undefined) {
    return body;
  }
  return { ...body, secrets: secrets === null ? null : secretWires(secrets) };
}

function execRequestBody(
  request: ExecSandboxRequest,
  secrets: Iterable<SecretInput> | null | undefined,
): ExecSandboxRequest {
  if (secrets === undefined || secrets === null) {
    return request;
  }
  const secretEnv = mergeSecretEnv(secrets);
  if (Object.keys(secretEnv).length === 0) {
    return request;
  }
  return { ...request, env: { ...(request.env ?? {}), ...secretEnv } };
}

function secretWires(secrets: Iterable<SecretInput>): SecretWire[] {
  const wires: SecretWire[] = [];
  for (const secret of secrets) {
    wires.push(toSecret(secret).toWire());
  }
  return wires;
}

function mergeSecretEnv(secrets: Iterable<SecretInput>): Record<string, string> {
  const env: Record<string, string> = {};
  for (const secret of secrets) {
    const values = toSecret(secret).asEnv();
    for (const key in values) {
      env[key] = values[key];
    }
  }
  return env;
}

function toSecret(input: SecretInput): Secret {
  if (input instanceof Secret) {
    return input;
  }
  if (isSecretWire(input)) {
    return new Secret(input.values, input.name);
  }
  return new Secret(input);
}

function checkedSecretEnv(values: Record<string, string>): Record<string, string> {
  const checked: Record<string, string> = {};
  for (const key in values) {
    checked[checkedSecretName(key, "secret environment names")] = checkedSecretValue(values[key]);
  }
  return checked;
}

function checkedSecretName(name: string, label: string): string {
  if (name.length === 0 || name.includes("=") || name.includes("\0")) {
    throw new TypeError(`${label} must be non-empty and contain no '=' or NUL`);
  }
  return name;
}

function checkedSecretValue(value: string): string {
  if (value.includes("\0")) {
    throw new TypeError("secret environment values must contain no NUL bytes");
  }
  return value;
}

function normalizeBaseUrl(baseUrl: string): string {
  const trimmed = baseUrl.trim();
  if (!trimmed) {
    throw new TypeError("baseUrl must not be empty");
  }
  return trimmed.endsWith("/") ? trimmed.slice(0, -1) : trimmed;
}

function jsonRequest(method: "POST", body: unknown): RequestInit {
  return {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
}

async function apiError(response: Response): Promise<VmonApiError> {
  const fallback = response.statusText || "request failed";
  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) {
    const body: unknown = await response.json().catch(() => null);
    const parsed = parseErrorBody(body, fallback);
    return new VmonApiError({ status: response.status, code: parsed.code, message: parsed.message });
  }
  const text = await response.text().catch(() => "");
  return new VmonApiError({
    status: response.status,
    code: "http_error",
    message: text || fallback,
  });
}

function parseErrorBody(body: unknown, fallback: string): Omit<VmonApiErrorShape, "status"> {
  if (!isRecord(body)) {
    return { code: "http_error", message: fallback };
  }

  const detail = body.detail;
  if (isRecord(detail)) {
    const code = stringField(detail, "code") ?? "http_error";
    const message = stringField(detail, "message") ?? fallback;
    return { code, message };
  }
  if (typeof detail === "string") {
    return { code: "http_error", message: detail };
  }

  const code = stringField(body, "code") ?? "http_error";
  const message = stringField(body, "message") ?? fallback;
  return { code, message };
}

function isSecretWire(value: unknown): value is SecretWire {
  if (!isRecord(value)) {
    return false;
  }
  return typeof value.name === "string" && isStringRecord(value.values);
}

function isStringRecord(value: unknown): value is Record<string, string> {
  if (!isRecord(value)) {
    return false;
  }
  for (const key in value) {
    if (typeof value[key] !== "string") {
      return false;
    }
  }
  return true;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function stringField(record: Record<string, unknown>, field: string): string | null {
  const value = record[field];
  return typeof value === "string" ? value : null;
}
