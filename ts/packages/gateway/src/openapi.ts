const string = { type: "string" } as const;
const uint = { type: "integer", minimum: 0 } as const;
const policyInput = {
  type: "object",
  required: ["cadence", "limit_microdollars"],
  properties: {
    cadence: { type: "string", enum: ["monthly", "fixed"] },
    limit_microdollars: uint,
    timezone: string,
    period: { type: ["string", "null"] },
  },
} as const;
const ledgerProperties = {
  namespace: string,
  period: { type: ["string", "null"] },
  limit_microdollars: uint,
  spent_microdollars: uint,
  reserved_microdollars: uint,
  remaining_microdollars: uint,
  active: { type: "boolean" },
} as const;
const namespaceSchema = {
  type: "object",
  required: ["id", "attrs"],
  properties: {
    id: string,
    attrs: { type: "object", additionalProperties: true },
    blocklist: { type: "array", items: string },
  },
} as const;
const budgetSchema = { type: "object", required: Object.keys(ledgerProperties), properties: { ...ledgerProperties, period: string } } as const;
const policySchema = {
  type: "object",
  required: [...Object.keys(ledgerProperties), "cadence", "timezone"],
  properties: { ...ledgerProperties, ...policyInput.properties },
} as const;
const usageSchema = {
  type: "object",
  required: ["namespace", "period", "data"],
  properties: {
    namespace: string,
    period: string,
    data: {
      type: "array",
      items: {
        type: "object",
        required: ["model", "status", "count", "cost_microdollars"],
        properties: { model: string, status: string, count: uint, cost_microdollars: uint },
      },
    },
  },
} as const;
function jsonResponse(description: string, schema: object) {
  return { description, content: { "application/json": { schema } } };
}

/** OpenAPI 3.1 for the mounted management routes. Checked by ops/check-openapi.py. */
export const OPENAPI = {
  openapi: "3.1.0",
  info: {
    title: "Axond management API",
    version: "0.0.0",
  },
  components: {
    securitySchemes: {
      gateway_key: { type: "http", scheme: "bearer" },
    },
  },
  security: [{ gateway_key: [] }],
  paths: {
    "/api/v1/openapi.json": { get: { responses: { "200": { description: "This document" } } } },
    "/api/v1/namespaces": {
      get: { responses: { "200": { description: "List namespaces" } } },
      post: { responses: { "201": { description: "Created" } } },
    },
    "/api/v1/namespaces/{ns}": {
      parameters: [{ name: "ns", in: "path", required: true, schema: { type: "string" } }],
      get: { responses: { "200": jsonResponse("Read namespace", namespaceSchema) } },
      put: { responses: { "200": { description: "Replace attrs" } } },
      delete: { responses: { "204": { description: "Deleted" } } },
    },
    "/api/v1/namespaces/{ns}/budgets/{period}": {
      parameters: [{ name: "ns", in: "path", required: true, schema: { type: "string" } }, { name: "period", in: "path", required: true, schema: { type: "string" } }],
      get: { responses: { "200": jsonResponse("Read budget", budgetSchema) } },
      put: { responses: { "200": { description: "Set budget" } } },
    },
    "/api/v1/namespaces/{ns}/budget": {
      parameters: [{ name: "ns", in: "path", required: true, schema: { type: "string" } }],
      get: { responses: { "200": jsonResponse("Read policy", policySchema) } },
      put: {
        requestBody: { required: true, content: { "application/json": { schema: policyInput } } },
        responses: { "200": jsonResponse("Set policy", policySchema) },
      },
    },
    "/api/v1/namespaces/{ns}/usage": {
      parameters: [{ name: "ns", in: "path", required: true, schema: { type: "string" } }],
      get: {
        parameters: [{ name: "period", in: "query", required: true, schema: { type: "string" } }],
        responses: { "200": jsonResponse("Usage summary", usageSchema) },
      },
    },
    "/api/v1/providers/{id}/models": {
      parameters: [{ name: "id", in: "path", required: true, schema: { type: "string" } }],
      get: { responses: { "200": { description: "Cached provider models" } } },
    },
    "/api/v1/providers/models": {
      get: { responses: { "200": { description: "Cached models for every provider" } } },
    },
  },
} as const;
