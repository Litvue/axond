import type { paths } from "../generated/schema";

/** Compile-only witness that the generated spec types a mounted route. */
export type Namespace = paths["/api/v1/namespaces/{ns}"]["get"]["responses"]["200"]["content"]["application/json"];
export type UsageSummary =
  paths["/api/v1/namespaces/{ns}/usage"]["get"]["responses"]["200"]["content"]["application/json"];
export type Budget =
  paths["/api/v1/namespaces/{ns}/budgets/{period}"]["get"]["responses"]["200"]["content"]["application/json"];
