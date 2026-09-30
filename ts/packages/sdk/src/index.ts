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

/** One request, written when the handler returns. */
export interface RequestLog {
  msg: "request";
  request_id: string;
  trace_id: string;
  span_id: string;
  http_method: string;
  http_route: string;
  status_code: number;
  duration_ms: number;
  namespace: string;
  subject: string;
  model: string;
  target_provider?: string;
  target_model?: string;
  credential_source?: string;
  status?: string;
  retry_count?: number;
  input_tokens?: string;
  cache_read_tokens?: string;
  cache_write_tokens?: string;
  output_tokens?: string;
  cost_microdollars?: string | null;
  latency_ms?: number;
  ttft_ms?: number;
}

/** A credential was refused and the pool is opening the next one. */
export interface CredentialRateLimitLog {
  msg: "credential_rate_limited";
  request_id: string;
  provider: string;
  credential_id: string;
}

/** A transport phase exceeded its own bound or the remaining failover budget. */
export interface UpstreamTimeoutLog {
  msg: "upstream_timeout";
  request_id: string;
  provider: string;
  model: string;
  timeout: string;
  bound: string;
}

/**
 * The provider socket failed before a response could be classified.
 * `reason` is a bounded class. The endpoint and the runtime's message stay off this line.
 */
export interface UpstreamTransportLog {
  msg: "upstream_transport";
  request_id: string;
  provider: string;
  model: string;
  phase: "request" | "stream" | "closing";
  reason: "dns" | "refused" | "reset" | "tls" | "other";
  /** Present when the failure happened while a stream body was already open. */
  committed?: boolean;
}

/** An open stream hit its duration or byte cap before a terminal event. */
export interface StreamLimitLog {
  msg: "stream_limit";
  request_id: string;
  provider: string;
  model: string;
  limit: "duration" | "bytes";
}

/**
 * A byte-faithful body stayed open after its terminal event until a close bound.
 * `grace` is `transport.stream_terminal_grace_ms`. `duration` is the total stream
 * bound. The socket address stays off this line. The charge remains `ok`.
 */
export type TerminalRemainLog =
  | {
      msg: "terminal_remain";
      request_id: string;
      provider: string;
      model: string;
      bound: "grace";
      grace_ms: number;
    }
  | {
      msg: "terminal_remain";
      request_id: string;
      provider: string;
      model: string;
      bound: "duration";
    };

/**
 * A charge missed a settlement bound, or the Store rejected the write.
 * `queue_timeout` means the charge never started and spend was not written.
 * `execution_timeout` means Store work outlived its deadline and still finishes.
 * `charge_failed` means the Store write threw and spend was not written.
 * The driver text stays off this line.
 */
export type SettlementFailureLog =
  | {
      msg: "settlement_failure";
      request_id: string;
      reason: "queue_timeout" | "execution_timeout";
      waited_ms: number;
    }
  | {
      msg: "settlement_failure";
      request_id: string;
      reason: "charge_failed";
    };

/**
 * A charging route could not read the budget. `deny` refused the request.
 * `allow` served it and did not charge. The driver text stays off this line.
 */
export interface BudgetUnavailableLog {
  msg: "budget_unavailable";
  request_id: string;
  stance: "deny" | "allow";
}

/** A catalogue fetch was stored, or refused without replacing the active document. */
export interface CatalogueImportLog {
  msg: "catalogue_import";
  outcome: "admitted" | "refused";
  reason: string;
  consecutive_refusals: number;
}

/**
 * A provider `/models` refresh failed. The previous cache stays in place and
 * is marked stale when that write is accepted. The line names the provider id
 * and a bounded reason. The credential, the base URL, and the driver text stay
 * off it.
 */
export interface ProviderDiscoveryLog {
  msg: "provider_discovery";
  provider: string;
  reason: "no_credential" | "unreachable" | "denied" | "not_json" | "not_retained";
}

/**
 * One step of process shutdown. The CLI writes these on stdout.
 * The line names the signal and the phase. The bind address, the store
 * path, and the gateway key stay off it. `spend_unsettled` is written when
 * the settle share ends with a spawned charge or an admitted request still
 * open, and it names the stage counts and the oldest spawned charge's age.
 */
export type ShutdownLog =
  | {
      msg: "shutdown";
      phase: "requested";
      signal: string;
      drain_grace_ms: number;
      deadline_ms: number;
      in_flight: number;
    }
  | {
      msg: "shutdown";
      phase: "second_signal";
      signal: string;
    }
  | {
      msg: "shutdown";
      phase: "admission_closed";
      deadline_ms: number;
      in_flight: number;
    }
  | {
      msg: "shutdown";
      phase: "signal_ignored";
      signal: string;
    }
  | {
      msg: "shutdown";
      phase: "deadline_expired";
      deadline_ms: number;
      in_flight: number;
    }
  | {
      msg: "shutdown";
      phase: "spend_unsettled";
      in_flight: number;
      unsettled: number;
      settlements_queued: number;
      settlements_executing: number;
      settlements_reserved: number;
      oldest_settlement_ms: number;
      settle_share_ms: number;
    };

/**
 * A file-backed gateway key is readable by group or others.
 * The line names the path. The file bytes stay off it.
 */
export interface KeyMaterialLog {
  msg: "key_material";
  path: string;
}

export type AxondLog =
  | RequestLog
  | CredentialRateLimitLog
  | UpstreamTimeoutLog
  | UpstreamTransportLog
  | StreamLimitLog
  | TerminalRemainLog
  | SettlementFailureLog
  | BudgetUnavailableLog
  | CatalogueImportLog
  | ProviderDiscoveryLog
  | ShutdownLog
  | KeyMaterialLog;

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
  /**
   * Extension-owned series. Names must start with `axond.ext.`. Catalogue
   * names are refused. The process drops secret attribute values and new
   * series past its cardinality ceiling.
   */
  metrics: {
    record(name: string, value: number, attributes?: Record<string, string>): void;
    set(name: string, value: number, attributes?: Record<string, string>): void;
  };
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
  /** Add one refused catalogue import and return the durable run length. */
  noteCatalogRefusal(): Promise<number>;
  /** A stored import ends the refusal run. */
  resetCatalogStreak(): Promise<void>;
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
  /**
   * Usage subject for the static gateway key: the env var name or the file path.
   * Absent keeps the label `gateway-key`.
   */
  gatewayKeySubject?: string;
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
    set?(name: string, value: number, attributes?: Record<string, string>): void;
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
  /**
   * JSON logs on stdout. A request line is written when the handler returns.
   * A buffered charge includes status, tokens, and cost. A stream has been
   * routed but has not settled, so those fields are absent. A rate-limit
   * rotation, an upstream timeout, a transport failure, a stream duration or
   * byte cap, a settlement bound, a failed Store write, a budget-store outage,
   * a catalogue import, a failed provider model refresh, and a byte-faithful
   * body that stays open until the post-terminal grace or the stream duration
   * bound are separate lines. The CLI also writes a shutdown line for each
   * phase, and a `key_material` line when a gateway-key file is readable by
   * group or others. That line names the path. A `provider_discovery` line
   * names the provider id and a bounded reason. None of them carry a body, a
   * credential, a driver message, a bind address, or a source URL.
   */
  onLog?: (record: AxondLog) => void;
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
   * Settlements executing against the Store at once. `0` disables. Absent
   * uses 64. A charge waits `settlementQueueWaitMs` for a slot. A wait that
   * expires drops the charge.
   */
  maxInFlightSettlements?: number;
  /** How long a charge waits for an execution slot. `0` waits without a bound. Absent uses 10000. */
  settlementQueueWaitMs?: number;
  /**
   * How long one settlement may run once it holds a slot. `0` disables.
   * Absent uses 10000. The Store call still finishes; the miss is counted.
   */
  settlementTimeoutMs?: number;
  /**
   * Counters shared across apps in one isolate. Absent means this app keeps
   * its own. A Worker passes one gate so a new app per request still sheds.
   */
  admissionControl?: AdmissionControl;
}

export interface AdmissionPermit {
  readonly settlementClaimed: boolean;
  /** `0` means a running settlement is not timed. */
  readonly settlementTimeoutMs: number;
  claimSettlement(): void;
  releaseAdmission(): void;
  releaseSettlement(): void;
  /** `false` means the execution-slot wait expired and the charge must not run. */
  acquireExecution(metrics?: {
    record(name: string, value: number, attributes?: Record<string, string>): void;
  }): Promise<boolean>;
  releaseExecution(metrics?: {
    record(name: string, value: number, attributes?: Record<string, string>): void;
    set?(name: string, value: number, attributes?: Record<string, string>): void;
  }): void;
  beginSpawned(metrics?: {
    record(name: string, value: number, attributes?: Record<string, string>): void;
    set?(name: string, value: number, attributes?: Record<string, string>): void;
  }): void;
  endSpawned(metrics?: {
    record(name: string, value: number, attributes?: Record<string, string>): void;
    set?(name: string, value: number, attributes?: Record<string, string>): void;
  }): void;
}

export interface SettlementBacklog {
  spawned: number;
  oldestAgeMs: number;
  /** Spawned charges waiting for an execution slot. */
  queued: number;
  /** Spawned charges holding an execution slot. */
  executing: number;
  /** Admitted requests whose charge has not been spawned. */
  reserved: number;
}

export interface AdmissionControl {
  admit(
    kind: "buffered" | "streamed",
    metrics?: {
      record(name: string, value: number, attributes?: Record<string, string>): void;
      set?(name: string, value: number, attributes?: Record<string, string>): void;
    },
  ): Promise<AdmissionPermit>;
  oldestPendingAgeMs(): number;
  observeAge(metrics?: {
    set?(name: string, value: number, attributes?: Record<string, string>): void;
  }): void;
  awaitIdle(boundMs: number): Promise<SettlementBacklog>;
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
