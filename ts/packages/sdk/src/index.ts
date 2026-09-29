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
  /** When false, /readyz reports draining. Liveness stays ok. */
  serving?: () => boolean;
  /**
   * What a charging route does when the budget read fails. `deny` answers
   * `503 budget_unavailable`. `allow` serves the request without a charge.
   */
  onStoreUnavailable?: "deny" | "allow";
  /**
   * When false, new `/api` and `/ns` requests are refused before authentication.
   * Readiness can fail while this still returns true: that is the drain window.
   */
  admitting?: () => boolean;
  /** Catalogue recorder. Absent means the process emits no metrics. */
  metrics?: {
    record(name: string, value: number, attributes?: Record<string, string>): void;
    points?: readonly {
      name: string;
      value: number;
      attributes: Record<string, string>;
      observations?: number;
      min?: number;
      max?: number;
    }[];
  };
  /** OTLP/HTTP JSON target. Absent means the process exports nothing. */
  telemetry?: {
    endpoint: string;
    instanceId?: string;
    fetch?: typeof fetch;
  };
  /** One JSON object per request. The gateway never puts a body or credential in it. */
  onLog?: (record: {
    msg: "request";
    request_id: string;
    http_method: string;
    http_route: string;
    status_code: number;
    duration_ms: number;
    namespace: string;
    model: string;
    trace_id: string;
  }) => void;
  maxRequestBytes?: number;
  /**
   * Largest estimated input, in tokens, a request may carry. `0` disables.
   * Absent uses the shipped ceiling of 1_000_000. The estimate is the UTF-8
   * length of the parsed JSON divided by four.
   */
  maxPromptTokens?: number;
  /**
   * Largest output allowance a request may ask for. `0` disables. Absent uses
   * the shipped ceiling of 200_000. A larger `max_tokens`,
   * `max_completion_tokens`, or `max_output_tokens` is refused.
   */
  maxOutputTokens?: number;
  /**
   * Total lifetime of one stream, however productive. `0` disables. Absent
   * uses 3_600_000. Distinct from `transport.streamIdleTimeoutMs`, which
   * bounds silence. After a terminal event the bound closes the body
   * successfully. Before that it ends the stream in band.
   */
  maxStreamDurationMs?: number;
  /**
   * Upstream bytes one stream may relay. `0` disables. Absent uses 64 MiB.
   * The chunk that would pass the ceiling is not forwarded.
   */
  maxStreamBytes?: number;
  /**
   * In-memory credential circuit. Absent uses round-robin, a threshold of 2
   * consecutive 429s, and a 30s cooldown before one half-open probe.
   */
  credentialPool?: {
    strategy?: "round-robin" | "weighted";
    failureThreshold?: number;
    cooldownMs?: number;
  };
  /**
   * Node HTTP client that enforces `transport.connectTimeoutMs`. Workers and
   * the Bun binary omit it; their header and failover budgets still bound the attempt.
   */
  upstreamDispatcher?: object;
  /**
   * Concurrent requests this replica admits. `0` disables. Absent uses 1024.
   * Saturation is `503` `gateway_overloaded`.
   */
  maxInFlight?: number;
  /**
   * Concurrent open streams. `0` disables. Absent is 512, clamped down to
   * `maxInFlight` when that ceiling is finite and this one was omitted.
   * Saturation is `503` `stream_capacity_exhausted`.
   */
  maxInFlightStreams?: number;
  /**
   * Requests that may wait for `maxInFlight`. `0` disables. Must be set
   * together with `admissionQueueWaitMs`, and only when `maxInFlight` is finite.
   */
  admissionQueueCapacity?: number;
  /** How long a queued request waits, in milliseconds. `0` disables. */
  admissionQueueWaitMs?: number;
  /**
   * Unsettled charges this replica will carry. `0` disables. Absent is four
   * times `maxInFlight`. Saturation is `503` `settlement_capacity_exhausted`.
   */
  maxPendingSettlements?: number;
  /**
   * Counters shared across apps in one isolate. Absent means this app keeps
   * its own. A Worker passes one gate so a new app per request still sheds.
   */
  admissionControl?: AdmissionControl;
}

export interface AdmissionPermit {
  readonly settlementClaimed: boolean;
  claimSettlement(): void;
  releaseAdmission(): void;
  releaseSettlement(): void;
}

export interface AdmissionControl {
  admit(
    kind: "buffered" | "streamed",
    metrics?: { record(name: string, value: number, attributes?: Record<string, string>): void },
  ): Promise<AdmissionPermit>;
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
  /**
   * When false, `id` came from the env var name. A tenant using platform
   * fallback then sees the credential's state without this label. Absent
   * means the label was set explicitly and stays visible.
   */
  explicitId?: boolean;
  /** Share of traffic when the pool strategy is `weighted`. Absent means 1. */
  weight?: number;
}

export interface TransportLimits {
  responseHeaderTimeoutMs: number;
  bufferedBodyTimeoutMs: number;
  streamIdleTimeoutMs: number;
  maxResponseBytes: number;
  /**
   * Largest provider error body kept for the caller-visible message.
   * A larger body is truncated. Absent uses 65536.
   */
  maxErrorBytes?: number;
  /** TCP connect bound. Absent uses 5000. Enforced when `upstreamDispatcher` is set. */
  connectTimeoutMs?: number;
  /**
   * How long a byte-faithful stream may stay open after its terminal event.
   * Absent uses 1000. Trailing bytes inside the grace are relayed.
   */
  streamTerminalGraceMs?: number;
  /**
   * Failover walk budget from `failover.overall_timeout_ms`. Absent uses 30000.
   * It tightens the header and buffered-body waits and does not cut an open stream.
   */
  overallTimeoutMs?: number;
  /**
   * Target attempts for one request, from `failover.max_attempts`. Absent uses 3.
   * Credential rotation inside the one configured target is not counted.
   * Responses always uses 1.
   */
  maxAttempts?: number;
}

export interface UsageRecord {
  schemaVersion: 2;
  requestId: string;
  /** Inbound W3C trace id. Null when the request carried no valid traceparent. */
  traceId: string | null;
  namespace: string;
  /** Budget period at admission. Null when the request was not held. */
  period: string | null;
  subject: string;
  model: string;
  targetProvider: string;
  targetModel: string;
  /** `platform` when the serving credential belongs to the default namespace. */
  credentialSource: "platform" | "byok";
  /** Non-secret label of the credential that served the request. */
  credentialId: string;
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
  priceCatalog: null;
  signerKid: null;
  /** Milliseconds from request start to settlement. */
  latencyMs: number;
  /** Upstream targets tried. One target means `1` even after credential rotation. */
  attempts: number;
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
