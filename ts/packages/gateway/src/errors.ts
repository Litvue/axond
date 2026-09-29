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
  return Response.json(
    { error: { type: error.type, message: error.message } },
    { status: error.status },
  );
}

export function badRequest(message: string): GatewayFailure {
  return new GatewayFailure("bad_request", 400, message);
}
