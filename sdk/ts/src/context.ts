/** Context record persisted in contexts.json. */
export interface StoredContext {
  name: string;
  endpoints: string[];
  region?: string | null;
  updated?: number | null;
}

/** Context plus its optional credential token. */
export interface ResolvedContext {
  context: StoredContext;
  token?: string;
}

type NodeFs = {
  readFileSync(path: string, encoding: "utf8"): string;
};

function environment(): Record<string, string | undefined> {
  return typeof process === "undefined" ? {} : process.env;
}

/** Resolve the configured vmon home directory. */
export function vmonHome(): string {
  const env = environment();
  if (env.VMON_HOME) return env.VMON_HOME.replace(/\/$/, "");
  const home = env.HOME ?? env.USERPROFILE;
  if (!home) throw new Error("cannot determine vmon home directory");
  return `${home.replace(/\/$/, "")}/.vmon`;
}

function filesystem(): NodeFs {
  if (typeof process === "undefined") throw new Error("context DSN requires filesystem access");
  const fs: unknown = process.getBuiltinModule("node:fs");
  if (!isNodeFs(fs)) throw new Error("context DSN requires filesystem access");
  return fs;
}

/** Resolve a named context and credential from the local store. */
export function resolveContext(name: string): ResolvedContext {
  if (!name) throw new Error("context DSN requires a context name");
  const fs = filesystem();
  const home = vmonHome();
  let parsed: unknown;
  try {
    parsed = JSON.parse(fs.readFileSync(`${home}/contexts.json`, "utf8"));
  } catch (error) {
    throw new Error(`context ${name} not found`, { cause: error });
  }
  let candidate: unknown;
  if (Array.isArray(parsed)) {
    candidate = parsed.find((entry) => isRecord(entry) && entry.name === name);
  } else if (isRecord(parsed) && isRecord(parsed.contexts)) {
    candidate = parsed.contexts[name];
  } else if (isRecord(parsed)) {
    candidate = parsed[name];
  }
  const context = isStoredContext(candidate) && candidate.name === name ? candidate : undefined;
  if (!context || context.endpoints.length === 0)
    throw new Error(`context ${name} not found or has no endpoints`);
  let token: string | undefined;
  try {
    token = fs.readFileSync(`${home}/credentials/${name}.token`, "utf8").trim() || undefined;
  } catch {
    token = undefined;
  }
  return { context, token };
}

function isNodeFs(value: unknown): value is NodeFs {
  return isRecord(value) && typeof value.readFileSync === "function";
}

function isStoredContext(value: unknown): value is StoredContext {
  return (
    isRecord(value) &&
    typeof value.name === "string" &&
    Array.isArray(value.endpoints) &&
    value.endpoints.every((endpoint) => typeof endpoint === "string")
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
