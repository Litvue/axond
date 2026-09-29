import type { Context, MiddlewareHandler } from "hono";

export type { SseEvent, SseTransform } from "./sse.ts";
export { transformSseEvents } from "./sse.ts";

/**
 * Extension contract version. Adding a stage is compatible. Changing when an
 * existing stage runs is a breaking change and requires a new apiVersion.
 */
export const API_VERSION = 1 as const;

export type Stage = "pre-auth" | "post-auth" | "pre-dispatch";

export type InferenceRoute = "chat" | "messages" | "embeddings" | "responses" | "models" | "credentials";

export interface NamespaceRecord {
  id: string;
  attrs: Record<string, unknown>;
  blocklist?: string[];
}

export interface BudgetRecord {
  namespace: string;
  period: string;
  limit_microdollars: number | string;
  spent_microdollars: number | string;
  reserved_microdollars: number | string;
  remaining_microdollars: number | string;
  active: boolean;
}

export interface Settlement {
  requestId: string;
  namespace: string;
  period: string | null;
  model: string;
  status: string;
  costMicrodollars: bigint | null;
  /** True when this call won the usage insert and therefore applied the charge. */
  charged: boolean;
  usage: UsageTokens;
}

export interface UsageTokens {
  inputTokens: bigint;
  outputTokens: bigint;
  reasoningTokens: bigint;
  cacheReadTokens: bigint;
  cacheWriteTokens: bigint;
}

/**
 * Raw request bytes are the default. `json()` does not mark the body modified.
 * `setModel` rewrites only the top-level model string. `setJson` opts out of
 * byte fidelity.
 */
export interface RequestBody {
  raw(): Promise<Uint8Array>;
  json<T = unknown>(): Promise<T>;
  setModel(model: string): void;
  setJson(value: unknown): void;
}

export interface AxondContext {
  requestId: string;
  route: InferenceRoute | "management" | "other";
  subject?: string;
  namespace?: NamespaceRecord;
  target?: { provider: string; model: string };
  body: RequestBody;
  /** Set by a pre-auth extension that already verified a credential. */
  authenticated: boolean;
  /** Per-request spend cap in microdollars, set by an extension. */
  spendCapMicrodollars?: bigint;
  /** Alias globs the caller may use. Absent means unrestricted. */
  aliasGlobs?: string[];
  onSettle(fn: (settlement: Settlement) => Promise<void>): void;
  /**
   * Store handle visible to the extension. Untrusted extensions cannot query
   * rows outside the request namespace. Trusted extensions receive the process
   * store.
   */
  store: ExtensionStore;
}

export interface QueryResult {
  rows: Record<string, unknown>[];
}

/**
 * The public store. `query` is the extension primitive. Semantic methods are
 * the gateway's own surface and are part of the same contract because a
 * replacement budget backend is a Store implementation.
 */
export interface Store {
  query(sql: string, params?: readonly SqlValue[]): Promise<QueryResult>;
  resolveNamespace(id: string, nowMs: number): Promise<ResolvedNamespace | null>;
  putNamespace(record: NamespaceWrite): Promise<"created" | "exists">;
  getNamespace(id: string): Promise<NamespaceWrite | null>;
  updateNamespace(
    id: string,
    attrs: Record<string, unknown>,
    blocklist: string[] | null,
  ): Promise<NamespaceWrite | null>;
  deleteNamespace(id: string): Promise<boolean>;
  listNamespaces(
    cursor: string | null,
    limit: number,
  ): Promise<{ data: NamespaceWrite[]; nextCursor: string | null }>;
  putBudget(namespace: string, period: string, limit: bigint): Promise<BudgetLedger>;
  getBudget(namespace: string, period: string): Promise<BudgetLedger | null>;
  putBudgetPolicy(input: BudgetPolicyWrite): Promise<BudgetPolicy>;
  getBudgetPolicy(namespace: string): Promise<BudgetPolicy | null>;
  settle(input: SettleInput): Promise<{ charged: boolean }>;
  summarizeUsage(namespace: string, period: string): Promise<UsageSummaryRow[]>;
  listProviderModels(): Promise<ProviderModelCache[]>;
  getProviderModels(provider: string): Promise<ProviderModelCache | null>;
  upsertProviderModels(row: ProviderModelCache): Promise<void>;
  markProviderModelsStale(provider: string): Promise<void>;
}

/** What an extension is allowed to call. */
export interface ExtensionStore {
  query(sql: string, params?: readonly SqlValue[]): Promise<QueryResult>;
}

export type SqlValue = string | number | bigint | null;

export interface NamespaceWrite {
  id: string;
  attrs: Record<string, unknown>;
  blocklist: string[] | null;
  allowPlatformFallback: boolean;
  fromConfig: boolean;
}

export interface ResolvedNamespace {
  record: NamespaceWrite;
  period: string | null;
  limit: bigint | null;
  spent: bigint | null;
  incarnation: bigint;
  /** False when there is no budget row or spent >= limit. */
  admitted: boolean;
}

export interface BudgetLedger {
  namespace: string;
  period: string;
  limit: bigint;
  spent: bigint;
  active: boolean;
}

export interface BudgetPolicyWrite {
  namespace: string;
  cadence: "monthly" | "fixed";
  limit: bigint;
  timezone: string;
  period: string | null;
  nowMs: number;
}

export interface BudgetPolicy {
  namespace: string;
  cadence: "monthly" | "fixed";
  limit_microdollars: number | string;
  timezone: string;
  period: string;
  spent_microdollars: number | string;
  reserved_microdollars: number | string;
  remaining_microdollars: number | string;
  active: boolean;
}

export interface SettleInput {
  requestId: string;
  namespace: string;
  period: string | null;
  model: string;
  status: string;
  cost: bigint | null;
  incarnation: bigint;
}

export interface UsageSummaryRow {
  model: string;
  status: string;
  count: number;
  cost_microdollars: number | string;
}

export interface ProviderModelCache {
  provider: string;
  fetchedAt: string | null;
  stale: boolean;
  data: unknown[];
  source: string | null;
}

export interface AxondEnv {
  Bindings: Record<string, unknown>;
  Variables: { axond: AxondContext };
}

export interface AxondExtension {
  name: string;
  apiVersion: typeof API_VERSION;
  stage: Stage;
  middleware: MiddlewareHandler<AxondEnv>;
  routes?: import("hono").Hono<AxondEnv>;
  /** Idempotent SQL. Every created table must be prefixed `axond_ext_<name>_`. */
  migrations?: string[];
  /**
   * First-party extensions the operator reviewed. Untrusted extensions receive
   * a store that refuses queries outside the request namespace.
   */
  trusted?: boolean;
}

export interface AxondOptions {
  store: Store;
  providers: ProviderConfig[] | (() => Promise<ProviderConfig[]>);
  gatewayKey: string | ((c: Context<AxondEnv>) => string | Promise<string>);
  extensions?: AxondExtension[];
  waitUntil?: (promise: Promise<unknown>) => void;
  prices?: PriceRule[];
  blocklist?: string[];
  credentials?: CredentialConfig[];
  defaultNamespace: string;
  /** Namespace ids declared in the deployment file. They cannot be deleted. */
  configNamespaces?: string[];
  transport?: TransportLimits;
  /** Host-supplied original request target, before URL decoding. */
  rawPath?: (c: Context<AxondEnv>) => string;
  clock?: () => number;
  onUsage?: (record: UsageRecord) => void;
  /** When false, /readyz reports draining. */
  serving?: () => boolean;
  /** Catalogue recorder. Absent means the process emits no metrics. */
  metrics?: {
    record(name: string, value: number, attributes?: Record<string, string>): void;
  };
  maxRequestBytes?: number;
}

export interface ProviderConfig {
  id: string;
  kind: "openai" | "openai-compatible" | "anthropic";
  baseUrl: string;
  unpricedModels?: "deny" | "allow";
}

export interface PriceRule {
  provider: string;
  model: string;
  inputMicrodollarsPerMillion: bigint;
  outputMicrodollarsPerMillion: bigint;
  reasoningMicrodollarsPerMillion?: bigint;
  cacheReadMicrodollarsPerMillion?: bigint;
  cacheWriteMicrodollarsPerMillion?: bigint;
}

export interface CredentialConfig {
  namespace: string;
  provider: string;
  secret: string;
  id: string;
}

export interface TransportLimits {
  responseHeaderTimeoutMs: number;
  bufferedBodyTimeoutMs: number;
  streamIdleTimeoutMs: number;
  maxResponseBytes: number;
}

export interface UsageRecord {
  schemaVersion: 2;
  requestId: string;
  namespace: string;
  subject: string;
  model: string;
  targetProvider: string;
  targetModel: string;
  status: string;
  inputTokens: bigint;
  outputTokens: bigint;
  reasoningTokens: bigint;
  cacheReadTokens: bigint;
  cacheWriteTokens: bigint;
  costMicrodollars: bigint | null;
  /** Kept on the wire schema. Empty for the static-key core. */
  catalogVersion: 0;
  priceBook: null;
  priceBookChecksum: null;
  signerKid: null;
}

/**
 * Stability: apiVersion 1 is the first published contract.
 * Deprecation: a field or stage stays for one apiVersion after it is marked
 * deprecated in the changelog, then disappears only in the next apiVersion.
 * Changing the order of `pre-auth`, static-key auth, namespace resolution,
 * `post-auth`, model parse, `pre-dispatch`, dispatch, or settlement is a
 * breaking change.
 */
export const STABILITY_POLICY = {
  apiVersion: API_VERSION,
  stages: ["pre-auth", "post-auth", "pre-dispatch"] as const,
  breaking:
    "Changing when an existing stage runs, or removing a Store method, requires a new apiVersion.",
};
