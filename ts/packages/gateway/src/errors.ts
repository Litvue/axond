/** A Store call failed. The driver message is dropped so it cannot leak a DSN. */
export class StoreFailure extends Error {
  constructor() {
    super("store is unavailable");
    this.name = "StoreFailure";
  }
}

export class GatewayFailure extends Error {
  readonly type: string;
  readonly status: number;
  /** A provider 429. This parks the credential. A 5xx does not. */
  readonly rateLimited: boolean;
  /** Which wait ran out, when `type` is `upstream_timeout`. */
  timeoutKind: "connect" | "response_headers" | "buffered_body" | "stream_idle" | "overall" | null = null;
  /** Whose budget ended that wait. */
  timeoutBound: "phase" | "walk_budget" | null = null;
  /** Provider HTTP status, before the gateway maps it onto its own response. */
  upstreamStatus: number | null = null;
  /** Seconds for `Retry-After`, when this refusal advertises a retry. */
  retryAfter: string | null = null;

  constructor(type: string, status: number, message: string, rateLimited = false) {
    super(message);
    this.name = "GatewayFailure";
    this.type = type;
    this.status = status;
    this.rateLimited = rateLimited;
  }
}

export function gatewayError(error: GatewayFailure): Response {
  const retry = error.retryAfter ?? (error.type === "draining" ? "0" : null);
  const headers = retry !== null ? { "retry-after": retry } : undefined;
  return Response.json(
    { error: {
      type: error.type,
      message: error.type === "bad_request" ? `bad request: ${error.message}` : error.message,
    } },
    { status: error.status, headers },
  );
}

export function badRequest(message: string): GatewayFailure {
  return new GatewayFailure("bad_request", 400, message);
}

/** Rust `StoreError::Invalid` when `cadence: "fixed"` has no period and no active row. */
export const FIXED_CADENCE_NEEDS_PERIOD =
  "fixed cadence needs a period: the namespace has no active period";
