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

import { GatewayFailure } from "./errors.ts";
import { monthlyPeriod } from "./namespace.ts";

const I64_MAX = 9223372036854775807n;

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

  function ledger(namespace: string, period: string): BudgetLedger {
    const row = budgets.get(key(namespace, period));
    if (!row) {
      throw new GatewayFailure("unknown_budget", 404, "unknown budget");
    }
    return {
      namespace,
      period,
      limit: row.limit,
      spent: row.spent,
      active: active.get(namespace) === period,
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
    active.set(namespace, period);
    return period;
  }

  let catalogStreak = 0;

  const store: Store = {
    async query(): Promise<QueryResult> {
      throw new GatewayFailure("bad_request", 400, "memory store query is only available to a trusted extension with a SQL store");
    },
    resolveNamespace(id, nowMs) {
      return lock(() => {
        const record = namespaces.get(id);
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
        namespaces.set(record.id, { ...record, attrs: { ...record.attrs } });
        return "created" as const;
      });
    },
    getNamespace(id) {
      return lock(() => namespaces.get(id) ?? null);
    },
    updateNamespace(id, attrs, blocklist) {
      return lock(() => {
        const current = namespaces.get(id);
        if (!current) {
          return null;
        }
        const next = { ...current, attrs: { ...attrs }, blocklist };
        namespaces.set(id, next);
        return next;
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
        return { data: page.map((id) => namespaces.get(id)!), nextCursor };
      });
    },
    putBudget(namespace, period, limit) {
      return lock(() => {
        requireNamespace(namespace);
        const slot = key(namespace, period);
        const current = budgets.get(slot) ?? { limit: 0n, spent: 0n };
        budgets.set(slot, { limit, spent: current.spent });
        active.set(namespace, period);
        return ledger(namespace, period);
      });
    },
    getBudget(namespace, period) {
      return lock(() => {
        requireNamespace(namespace);
        return budgets.has(key(namespace, period)) ? ledger(namespace, period) : null;
      });
    },
    putBudgetPolicy(input: BudgetPolicyWrite) {
      return lock(() => {
        requireNamespace(input.namespace);
        const period = input.cadence === "monthly" ? monthlyPeriod(input.nowMs, input.timezone) : input.period;
        if (!period) {
          throw new GatewayFailure("bad_request", 400, "period is required for cadence \"fixed\"");
        }
        policies.set(input.namespace, {
          cadence: input.cadence,
          limit: input.limit,
          timezone: input.timezone,
          period: input.cadence === "fixed" ? period : null,
        });
        const slot = key(input.namespace, period);
        if (!budgets.has(slot)) {
          budgets.set(slot, { limit: input.limit, spent: 0n });
        } else {
          budgets.get(slot)!.limit = input.limit;
        }
        active.set(input.namespace, period);
        return policyView(input.namespace);
      });
    },
    getBudgetPolicy(namespace) {
      return lock(() => {
        requireNamespace(namespace);
        return policies.has(namespace) ? policyView(namespace) : null;
      });
    },
    settle(input: SettleInput) {
      return lock(() => {
        if (usage.has(input.requestId)) {
          return { charged: false };
        }
        usage.set(input.requestId, {
          requestId: input.requestId,
          namespace: input.namespace,
          period: input.period,
          model: input.model,
          status: input.status,
          cost: input.cost,
        });
        const incarnation = incarnations.get(input.namespace) ?? 1n;
        if (
          input.cost === null ||
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
        const room = I64_MAX - input.cost;
        row.spent = row.spent >= room ? I64_MAX : row.spent + input.cost;
        return { charged: true };
      });
    },
    summarizeUsage(namespace, period) {
      return lock(() => {
        requireNamespace(namespace);
        const grouped = new Map<string, UsageSummaryRow>();
        for (const row of usage.values()) {
          if (row.namespace !== namespace || row.period !== period) {
            continue;
          }
          const slot = `${row.model}\0${row.status}`;
          const current = grouped.get(slot) ?? {
            model: row.model,
            status: row.status,
            count: 0,
            cost_microdollars: 0,
          };
          current.count += 1;
          current.cost_microdollars = Number(BigInt(current.cost_microdollars) + (row.cost ?? 0n));
          grouped.set(slot, current);
        }
        return [...grouped.values()];
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

  function policyView(namespace: string): BudgetPolicy {
    const policy = policies.get(namespace)!;
    const period = active.get(namespace)!;
    const row = ledger(namespace, period);
    return {
      namespace,
      cadence: policy.cadence,
      limit_microdollars: money(policy.limit),
      timezone: policy.timezone,
      period,
      spent_microdollars: money(row.spent),
      reserved_microdollars: 0,
      remaining_microdollars: money(row.limit > row.spent ? row.limit - row.spent : 0n),
      active: true,
    };
  }

  return store;
}

export function money(value: bigint): number | string {
  if (value <= BigInt(Number.MAX_SAFE_INTEGER)) {
    return Number(value);
  }
  return value.toString();
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

export function namespaceJson(record: NamespaceWrite) {
  return {
    id: record.id,
    attrs: record.attrs,
    ...(record.blocklist === null ? {} : { blocklist: record.blocklist }),
  };
}

/** Placeholder so the memory store satisfies the SqlValue import used by SQL stores. */
export type { SqlValue };
