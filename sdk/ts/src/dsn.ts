import { resolveContext, vmonHome } from "./context";

/** A normalized HTTP or Unix-domain-socket endpoint. */
export interface DsnEndpoint {
  url: string;
  unix?: string;
}

/** Parsed connection settings used by the mesh driver. */
export interface DsnConfig {
  endpoints: DsnEndpoint[];
  token?: string;
  discover: boolean;
  timeout: number;
  context?: string;
}

/** Explicit overrides applied while parsing a DSN. */
export interface ParseDsnOptions {
  token?: string;
  timeout?: number;
  discover?: boolean;
}

/** Parse a vmon connection string without performing network I/O. */
export function parseDsn(input?: string | null, options: ParseDsnOptions = {}): DsnConfig {
  const env = typeof process === "undefined" ? {} : process.env;
  let dsn = input?.trim();
  if (!dsn) {
    dsn = env.VMON_DSN?.trim();
    if (!dsn && env.VMON_CONTEXT) dsn = `vmon+context://${env.VMON_CONTEXT}`;
    if (!dsn) dsn = browserOrigin() ?? `vmon+unix://${vmonHome()}/vmond.sock`;
  }
  const queryAt = dsn.indexOf("?");
  const rawBase = queryAt < 0 ? dsn : dsn.slice(0, queryAt);
  const query = new URLSearchParams(queryAt < 0 ? "" : dsn.slice(queryAt + 1));
  for (const name of query.keys()) {
    if (name !== "token" && name !== "discover" && name !== "timeout")
      throw new Error(`invalid DSN parameter ${name}`);
  }
  const discoverValue = query.get("discover");
  if (discoverValue !== null && discoverValue !== "on" && discoverValue !== "off")
    throw new Error("invalid DSN parameter discover");
  const timeoutValue = query.get("timeout");
  const parsedTimeout = timeoutValue === null ? 60 : Number(timeoutValue);
  if (!Number.isFinite(parsedTimeout) || parsedTimeout <= 0)
    throw new Error("invalid DSN parameter timeout");

  let endpoints: DsnEndpoint[];
  let context: string | undefined;
  let contextToken: string | undefined;
  if (rawBase.startsWith("vmon+unix://")) {
    const socket = decodeURIComponent(rawBase.slice("vmon+unix://".length));
    if (!socket.startsWith("/")) throw new Error("vmon+unix DSN requires an absolute path");
    endpoints = [{ url: "http://localhost", unix: socket }];
  } else if (rawBase.startsWith("vmon+context://")) {
    context = decodeURIComponent(rawBase.slice("vmon+context://".length).replace(/^\/+/, ""));
    const resolved = resolveContext(context);
    endpoints = resolved.context.endpoints.map((url) => ({ url: normalizeHttpUrl(url) }));
    contextToken = resolved.token;
  } else if (rawBase.startsWith("vmon://") || rawBase.startsWith("vmons://")) {
    const secure = rawBase.startsWith("vmons://");
    const rest = rawBase.slice(secure ? 8 : 7);
    if (rest.includes("@")) throw new Error("DSN userinfo is not supported");
    const slash = rest.indexOf("/");
    const authority = slash < 0 ? rest : rest.slice(0, slash);
    const prefix = slash < 0 ? "" : rest.slice(slash).replace(/\/$/, "");
    if (!authority) throw new Error("DSN requires at least one host");
    endpoints = authority.split(",").map((host) => {
      if (!host) throw new Error("DSN contains an empty host");
      const withPort = hasPort(host) ? host : `${host}:8000`;
      return { url: `${secure ? "https" : "http"}://${withPort}${prefix}` };
    });
  } else if (rawBase.startsWith("http://") || rawBase.startsWith("https://")) {
    const url = new URL(rawBase);
    if (url.username || url.password) throw new Error("DSN userinfo is not supported");
    if (url.host.includes(",")) throw new Error("http(s) DSN accepts one endpoint");
    endpoints = [{ url: normalizeHttpUrl(rawBase) }];
  } else {
    throw new Error(`unsupported DSN scheme`);
  }
  return {
    endpoints,
    token: options.token ?? query.get("token") ?? env.VMON_API_TOKEN ?? contextToken,
    discover: options.discover ?? discoverValue !== "off",
    timeout: options.timeout ?? parsedTimeout,
    context,
  };
}

function normalizeHttpUrl(value: string): string {
  const url = new URL(value);
  if (url.protocol !== "http:" && url.protocol !== "https:")
    throw new Error(`context endpoint must use http or https`);
  return url.toString().replace(/\/$/, "");
}

function hasPort(host: string): boolean {
  if (host.startsWith("[")) return /^\[[^\]]+\]:\d+$/.test(host);
  return /:\d+$/.test(host);
}

function browserOrigin(): string | undefined {
  if (
    typeof location === "undefined" ||
    (location.protocol !== "http:" && location.protocol !== "https:")
  )
    return undefined;
  return location.origin;
}
