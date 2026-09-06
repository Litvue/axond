import type { paths } from "../generated/schema";

/** Compile-only witness that the generated spec types a mounted route. */
export type Namespace = paths["/api/v1/namespaces/{ns}"]["get"]["responses"]["200"]["content"]["application/json"];
export type DeleteNamespace = paths["/api/v1/namespaces/{ns}"]["delete"];
export type UsageSummary =
  paths["/api/v1/namespaces/{ns}/usage"]["get"]["responses"]["200"]["content"]["application/json"];
export type Budget =
  paths["/api/v1/namespaces/{ns}/budgets/{period}"]["get"]["responses"]["200"]["content"]["application/json"];

/** The cadence policy and its update body must be usable by generated clients. */
export type BudgetPolicy =
  paths["/api/v1/namespaces/{ns}/budget"]["get"]["responses"]["200"]["content"]["application/json"];
export type PutBudgetPolicy =
  paths["/api/v1/namespaces/{ns}/budget"]["put"]["requestBody"]["content"]["application/json"];
export const monthlyPolicy = {
  cadence: "monthly",
  limit_microdollars: 10_000_000,
  timezone: "UTC",
} satisfies PutBudgetPolicy;
