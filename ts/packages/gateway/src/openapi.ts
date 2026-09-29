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
      get: { responses: { "200": { description: "Read namespace" } } },
      put: { responses: { "200": { description: "Replace attrs" } } },
      delete: { responses: { "204": { description: "Deleted" } } },
    },
    "/api/v1/namespaces/{ns}/budgets/{period}": {
      get: { responses: { "200": { description: "Read budget" } } },
      put: { responses: { "200": { description: "Set budget" } } },
    },
    "/api/v1/namespaces/{ns}/budget": {
      get: { responses: { "200": { description: "Read policy" } } },
      put: { responses: { "200": { description: "Set policy" } } },
    },
    "/api/v1/namespaces/{ns}/usage": {
      get: {
        parameters: [{ name: "period", in: "query", required: true, schema: { type: "string" } }],
        responses: { "200": { description: "Usage summary" } },
      },
    },
    "/api/v1/providers/{id}/models": {
      get: { responses: { "200": { description: "Cached provider models" } } },
    },
    "/api/v1/providers/models": {
      get: { responses: { "200": { description: "Cached models for every provider" } } },
    },
  },
} as const;
