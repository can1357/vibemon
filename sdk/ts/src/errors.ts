/** Structured server error fields. */
export interface APIErrorShape {
  status: number;
  code: string;
  message: string;
}

/** An error envelope returned by the vmon API. */
export class APIError extends Error implements APIErrorShape {
  override readonly name = "APIError";
  readonly status: number;
  readonly code: string;

  /** Create a structured API error. */
  constructor(error: APIErrorShape) {
    super(error.message);
    this.status = error.status;
    this.code = error.code;
  }
}

/** A connection, DNS, or transport timeout failure eligible for failover. */
export class TransportError extends Error {
  override readonly name = "TransportError";
}

/** A malformed wire frame or response payload. */
export class ProtocolError extends Error {
  override readonly name = "ProtocolError";
}

/** Decode a non-success HTTP response into an APIError. */
export async function apiError(response: Response): Promise<APIError> {
  const fallback = response.statusText || "request failed";
  let body: unknown = null;
  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("json")) body = await response.json().catch(() => null);
  else {
    const text = await response.text().catch(() => "");
    if (text) return new APIError({ status: response.status, code: "http_error", message: text });
  }
  const record = isRecord(body) ? body : {};
  const detail = isRecord(record.detail) ? record.detail : record;
  return new APIError({
    status: response.status,
    code: typeof detail.code === "string" ? detail.code : "http_error",
    message:
      typeof detail.message === "string"
        ? detail.message
        : typeof record.detail === "string"
          ? record.detail
          : fallback,
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
