import type {
  BudgetLedger,
  BudgetPolicy,
  BudgetPolicyWrite,
  NamespaceWrite,
  ProviderModelCache,
  QueryResult,
  ResolvedNamespace,
  SettleInput,
  SqlValue,
  Store,
  UsageSummaryRow,
} from "@axond/sdk";

import { FIXED_CADENCE_NEEDS_PERIOD, GatewayFailure } from "./errors.ts";
import { monthlyPeriod } from "./namespace.ts";
import { serdeCanonical, serdeValue } from "./strict-json.ts";

const I64_MAX = 9223372036854775807n;
const U64_MAX = 18446744073709551615n;

interface BudgetRow {
  limit: bigint;
  spent: bigint;
}

interface PolicyRow {
  cadence: "monthly" | "fixed";
  limit: bigint;
  timezone: string;
  period: string | null;
}

function copyAttrs(attrs: NamespaceWrite["attrs"]): NamespaceWrite["attrs"] {
  const canonical = serdeCanonical(attrs);
  if (canonical !== null) {
    return serdeValue(canonical) as NamespaceWrite["attrs"];
  }
  if (Array.isArray(attrs) || (attrs !== null && typeof attrs === "object")) {
    return JSON.parse(JSON.stringify(attrs)) as NamespaceWrite["attrs"];
  }
  return attrs;
}

function copyNamespace(record: NamespaceWrite): NamespaceWrite {
  return { ...record, attrs: copyAttrs(record.attrs), blocklist: record.blocklist === null ? null : [...record.blocklist] };
}

interface UsageRow {
  requestId: string;
  namespace: string;
  period: string | null;
  model: string;
  status: string;
  cost: bigint | null;
}

/**
 * Process-local Store used by tests and by hosts that have not opened SQLite
 * or Postgres yet. Settlement is serialized so concurrent callers share one
 * ledger.
 */
export function createMemoryStore(): Store {
  const namespaces = new Map<string, NamespaceWrite>();
  const incarnations = new Map<string, bigint>();
  const budgets = new Map<string, BudgetRow>();
  const active = new Map<string, string>();
  const policies = new Map<string, PolicyRow>();
  const usage = new Map<string, UsageRow>();
  const models = new Map<string, ProviderModelCache>();
  let chain: Promise<unknown> = Promise.resolve();

  function lock<T>(fn: () => T): Promise<T> {
    const run = chain.then(() => fn());
    chain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  function key(namespace: string, period: string): string {
    return `${namespace}\0${period}`;
  }

  function requireNamespace(id: string): NamespaceWrite {
    const record = namespaces.get(id);
    if (!record) {
      throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
    }
    return record;
  }

  function periodIsActive(namespace: string, period: string, nowMs: number): boolean {
    const policy = policies.get(namespace);
    if (policy?.cadence === "monthly") {
      return monthlyPeriod(nowMs, policy.timezone) === period;
    }
    return active.get(namespace) === period;
  }

  function ledger(namespace: string, period: string, nowMs: number): BudgetLedger {
    const row = budgets.get(key(namespace, period));
    if (!row) {
      throw new GatewayFailure("unknown_budget", 404, "unknown budget");
    }
    return {
      namespace,
      period,
      limit: row.limit,
      spent: row.spent,
      active: periodIsActive(namespace, period, nowMs),
    };
  }

  function ensureMonthly(namespace: string, nowMs: number): string | null {
    const policy = policies.get(namespace);
    if (!policy || policy.cadence !== "monthly") {
      return active.get(namespace) ?? null;
    }
    const period = monthlyPeriod(nowMs, policy.timezone);
    const slot = key(namespace, period);
    if (!budgets.has(slot)) {
      budgets.set(slot, { limit: policy.limit, spent: 0n });
    }
    return period;
  }

  let catalogStreak = 0;

  const store: Store = {
    async query(): Promise<QueryResult> {
      throw new GatewayFailure("bad_request", 400, "memory store query is only available to a trusted extension with a SQL store");
    },
    resolveNamespace(id, nowMs) {
      return lock(() => {
        const stored = namespaces.get(id);
        const record = stored ? copyNamespace(stored) : undefined;
        if (!record) {
          return null;
        }
        const period = ensureMonthly(id, nowMs);
        const incarnation = incarnations.get(id) ?? 1n;
        if (!period) {
          return { record, period: null, limit: null, spent: null, incarnation, admitted: false };
        }
        const row = budgets.get(key(id, period));
        if (!row) {
          return { record, period, limit: null, spent: null, incarnation, admitted: false };
        }
        return {
          record,
          period,
          limit: row.limit,
          spent: row.spent,
          incarnation,
          admitted: row.spent < row.limit,
        };
      });
    },
    putNamespace(record) {
      return lock(() => {
        if (namespaces.has(record.id)) {
          return "exists" as const;
        }
        namespaces.set(record.id, copyNamespace(record));
        return "created" as const;
      });
    },
    adoptConfigNamespace(id, allowPlatformFallback) {
      return lock(() => {
        const current = namespaces.get(id);
        if (!current) {
          namespaces.set(id, { id, attrs: {}, blocklist: null, allowPlatformFallback, fromConfig: true });
          return;
        }
        namespaces.set(id, { ...current, allowPlatformFallback, fromConfig: true });
      });
    },
    releaseConfigNamespace(id) {
      return lock(() => {
        const current = namespaces.get(id);
        if (!current) {
          return;
        }
        namespaces.set(id, { ...current, fromConfig: false });
      });
    },
    getNamespace(id) {
      return lock(() => { const record = namespaces.get(id); return record ? copyNamespace(record) : null; });
    },
    updateNamespace(id, attrs, blocklist) {
      return lock(() => {
        const current = namespaces.get(id);
        if (!current) {
          return null;
        }
        const next = { ...current, attrs: copyAttrs(attrs), blocklist: blocklist === null ? null : [...blocklist] };
        namespaces.set(id, next);
        return copyNamespace(next);
      });
    },
    deleteNamespace(id) {
      return lock(() => {
        const existed = namespaces.delete(id);
        if (!existed) {
          return false;
        }
        for (const slot of [...budgets.keys()]) {
          if (slot.startsWith(`${id}\0`)) {
            budgets.delete(slot);
          }
        }
        active.delete(id);
        policies.delete(id);
        incarnations.set(id, (incarnations.get(id) ?? 1n) + 1n);
        return true;
      });
    },
    listNamespaces(cursor, limit) {
      return lock(() => {
        const ids = [...namespaces.keys()].filter((id) => cursor === null || id > cursor).sort();
        const page = ids.slice(0, limit);
        const nextCursor = ids.length > limit ? page[page.length - 1]! : null;
        return { data: page.map((id) => copyNamespace(namespaces.get(id)!)), nextCursor };
      });
    },
    putBudget(namespace, period, limit, nowMs = Date.now()) {
      return lock(() => {
        requireNamespace(namespace);
        const slot = key(namespace, period);
        const current = budgets.get(slot) ?? { limit: 0n, spent: 0n };
        budgets.set(slot, { limit, spent: current.spent });
        active.set(namespace, period);
        return ledger(namespace, period, nowMs);
      });
    },
    getBudget(namespace, period, nowMs = Date.now()) {
      return lock(() => {
        requireNamespace(namespace);
        return budgets.has(key(namespace, period)) ? ledger(namespace, period, nowMs) : null;
      });
    },
    putBudgetPolicy(input: BudgetPolicyWrite) {
      return lock(() => {
        requireNamespace(input.namespace);
        let period = input.period;
        if (input.cadence === "monthly") {
          period = monthlyPeriod(input.nowMs, input.timezone);
        } else if (!period) {
          period = active.get(input.namespace) ?? null;
          if (!period) {
            throw new GatewayFailure("bad_request", 400, FIXED_CADENCE_NEEDS_PERIOD);
          }
        }
        policies.set(input.namespace, {
          cadence: input.cadence,
          limit: input.limit,
          timezone: input.timezone,
          period: input.cadence === "fixed" ? period : null,
        });
        const slot = key(input.namespace, period);
        const current = budgets.get(slot);
        budgets.set(slot, { limit: input.limit, spent: current?.spent ?? 0n });
        if (input.cadence === "fixed") {
          active.set(input.namespace, period);
        }
        return policyView(input.namespace, input.nowMs)!;
      });
    },
    getBudgetPolicy(namespace, nowMs = Date.now()) {
      return lock(() => {
        requireNamespace(namespace);
        return policyView(namespace, nowMs);
      });
    },
    settle(input: SettleInput) {
      return lock(() => {
        if (usage.has(input.requestId)) {
          return { charged: false };
        }
        const cost = input.cost === null ? null : saturateMicrodollars(input.cost);
        usage.set(input.requestId, {
          requestId: input.requestId,
          namespace: input.namespace,
          period: input.period,
          model: input.model,
          status: input.status,
          cost,
        });
        const incarnation = incarnations.get(input.namespace) ?? 1n;
        if (
          cost === null ||
          input.period === null ||
          !namespaces.has(input.namespace) ||
          incarnation !== input.incarnation
        ) {
          return { charged: false };
        }
        const row = budgets.get(key(input.namespace, input.period));
        if (!row) {
          return { charged: false };
        }
        row.spent = addMicrodollars(row.spent, cost);
        return { charged: true };
      });
    },
    summarizeUsage(namespace, period) {
      return lock(() => {
        requireNamespace(namespace);
        const rows = [];
        for (const row of usage.values()) {
          if (row.namespace === namespace && row.period === period) {
            rows.push({ model: row.model, status: row.status, cost: row.cost });
          }
        }
        return foldUsageSummary(rows);
      });
    },
    listProviderModels() {
      return lock(() => [...models.values()]);
    },
    getProviderModels(provider) {
      return lock(() => models.get(provider) ?? null);
    },
    upsertProviderModels(row) {
      return lock(() => {
        const current = models.get(row.provider);
        if (current && current.source !== row.source && !current.stale) {
          return;
        }
        models.set(row.provider, row);
      });
    },
    markProviderModelsStale(provider) {
      return lock(() => {
        const current = models.get(provider) ?? {
          provider,
          fetchedAt: null,
          stale: true,
          data: [],
          source: null,
        };
        models.set(provider, { ...current, stale: true });
      });
    },
    noteCatalogRefusal() {
      return lock(() => {
        catalogStreak += 1;
        return catalogStreak;
      });
    },
    resetCatalogStreak() {
      return lock(() => {
        catalogStreak = 0;
      });
    },
  };

  function policyView(namespace: string, nowMs: number): BudgetPolicy | null {
    const policy = policies.get(namespace);
    if (!policy) {
      const period = active.get(namespace);
      const row = period ? budgets.get(key(namespace, period)) : undefined;
      if (!period || !row) {
        return null;
      }
      return budgetPolicyFromLedger({
        namespace,
        cadence: "fixed",
        timezone: "UTC",
        period,
        limit: row.limit,
        spent: row.spent,
      });
    }
    const period = policy.cadence === "monthly" ? monthlyPeriod(nowMs, policy.timezone) : (active.get(namespace) ?? "");
    const row = period ? budgets.get(key(namespace, period)) : undefined;
    return budgetPolicyFromLedger({
      namespace,
      cadence: policy.cadence,
      timezone: policy.timezone,
      period,
      limit: row?.limit ?? policy.limit,
      spent: row?.spent ?? 0n,
    });
  }

  return store;
}

export function money(value: bigint): number | string {
  if (value <= BigInt(Number.MAX_SAFE_INTEGER)) {
    return Number(value);
  }
  return value.toString();
}

/** Charge amounts above the signed 64-bit store cap saturate there. */
export function saturateMicrodollars(value: bigint): bigint {
  return value > I64_MAX ? I64_MAX : value;
}

function addMicrodollars(total: bigint, next: bigint): bigint {
  const amount = saturateMicrodollars(next);
  if (total >= I64_MAX) {
    return I64_MAX;
  }
  const room = I64_MAX - total;
  return amount >= room ? I64_MAX : total + amount;
}

/**
 * Group usage the way the Rust store does: model, then status, in UTF-8 byte
 * order. A null cost adds nothing. The count saturates at `u64::MAX` and the
 * cost at `i64::MAX`.
 */
export function foldUsageSummary(
  rows: Iterable<{ model: string; status: string; cost: bigint | null; count?: bigint }>,
): UsageSummaryRow[] {
  const grouped = new Map<string, Map<string, { count: bigint; cost: bigint }>>();
  for (const row of rows) {
    let statuses = grouped.get(row.model);
    if (!statuses) {
      statuses = new Map();
      grouped.set(row.model, statuses);
    }
    const current = statuses.get(row.status) ?? { count: 0n, cost: 0n };
    current.count += row.count ?? 1n;
    if (current.count > U64_MAX) current.count = U64_MAX;
    current.cost = addMicrodollars(current.cost, row.cost ?? 0n);
    statuses.set(row.status, current);
  }
  const summary: UsageSummaryRow[] = [];
  for (const [model, statuses] of grouped) {
    for (const [status, totals] of statuses) {
      summary.push({
        model,
        status,
        count: money(totals.count),
        cost_microdollars: money(totals.cost),
      });
    }
  }
  summary.sort((left, right) => {
    const byModel = compareUtf8(left.model, right.model);
    return byModel !== 0 ? byModel : compareUtf8(left.status, right.status);
  });
  return summary;
}

/** `GET .../usage` body. Amounts above 2^53 stay decimal digits, as serde emits them. */
export function usageSummaryBody(namespace: string, period: string, data: readonly UsageSummaryRow[]): string {
  const rows = data.map(
    (row) =>
      `{"model":${JSON.stringify(row.model)},"status":${JSON.stringify(row.status)},"count":${jsonUint(row.count)},"cost_microdollars":${jsonUint(row.cost_microdollars)}}`,
  );
  return `{"namespace":${JSON.stringify(namespace)},"period":${JSON.stringify(period)},"data":[${rows.join(",")}]}`;
}

function jsonUint(value: number | string | bigint): string {
  if (typeof value === "number") {
    return JSON.stringify(value);
  }
  return typeof value === "bigint" ? value.toString() : value;
}

function compareUtf8(left: string, right: string): number {
  const encoded = new TextEncoder();
  const a = encoded.encode(left);
  const b = encoded.encode(right);
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i += 1) {
    if (a[i] !== b[i]) {
      return a[i]! - b[i]!;
    }
  }
  return a.length - b.length;
}

export function budgetPolicyFromLedger(input: {
  namespace: string;
  cadence: "monthly" | "fixed";
  timezone: string;
  period: string;
  limit: bigint;
  spent: bigint;
}): BudgetPolicy {
  const view = budgetJson({
    namespace: input.namespace,
    period: input.period,
    limit: input.limit,
    spent: input.spent,
    active: true,
  });
  return {
    namespace: input.namespace,
    cadence: input.cadence,
    limit_microdollars: view.limit_microdollars,
    timezone: input.timezone,
    period: input.period,
    spent_microdollars: view.spent_microdollars,
    reserved_microdollars: 0,
    remaining_microdollars: view.remaining_microdollars,
    active: true,
  };
}

export function budgetJson(row: BudgetLedger) {
  const remaining = row.limit > row.spent ? row.limit - row.spent : 0n;
  return {
    namespace: row.namespace,
    period: row.period,
    limit_microdollars: money(row.limit),
    spent_microdollars: money(row.spent),
    reserved_microdollars: 0,
    remaining_microdollars: money(remaining),
    active: row.active,
  };
}

/** `PUT`/`GET` budget ledger. Amounts stay decimal digits, as serde emits `u64`. */
export function budgetRecordBody(row: BudgetLedger): string {
  const remaining = row.limit > row.spent ? row.limit - row.spent : 0n;
  return (
    `{"namespace":${JSON.stringify(row.namespace)},` +
    `"period":${JSON.stringify(row.period)},` +
    `"limit_microdollars":${row.limit.toString()},` +
    `"spent_microdollars":${row.spent.toString()},` +
    `"reserved_microdollars":0,` +
    `"remaining_microdollars":${remaining.toString()},` +
    `"active":${row.active ? "true" : "false"}}`
  );
}

/** `PUT`/`GET` budget policy. Field order matches the Rust `BudgetPolicy` struct. */
export function budgetPolicyBody(row: BudgetPolicy): string {
  return (
    `{"namespace":${JSON.stringify(row.namespace)},` +
    `"cadence":${JSON.stringify(row.cadence)},` +
    `"limit_microdollars":${jsonUint(row.limit_microdollars)},` +
    `"timezone":${JSON.stringify(row.timezone)},` +
    `"period":${JSON.stringify(row.period)},` +
    `"spent_microdollars":${jsonUint(row.spent_microdollars)},` +
    `"reserved_microdollars":${jsonUint(row.reserved_microdollars)},` +
    `"remaining_microdollars":${jsonUint(row.remaining_microdollars)},` +
    `"active":${row.active ? "true" : "false"}}`
  );
}

export function namespaceJson(record: NamespaceWrite) {
  return {
    id: record.id,
    attrs: record.attrs,
    ...(record.blocklist === null ? {} : { blocklist: record.blocklist }),
  };
}

/** Placeholder so the memory store satisfies the SqlValue import used by SQL stores. */
export type { SqlValue };
