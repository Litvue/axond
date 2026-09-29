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

  constructor(type: string, status: number, message: string) {
    super(message);
    this.name = "GatewayFailure";
    this.type = type;
    this.status = status;
  }
}

export function gatewayError(error: GatewayFailure): Response {
  const headers = error.type === "draining" ? { "retry-after": "0" } : undefined;
  return Response.json(
    { error: { type: error.type, message: error.message } },
    { status: error.status, headers },
  );
}

export function badRequest(message: string): GatewayFailure {
  return new GatewayFailure("bad_request", 400, message);
}
