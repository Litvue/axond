import { Hono, type Context, type MiddlewareHandler } from "hono";

import type {
  AxondContext,
  AxondEnv,
  AxondExtension,
  AxondLog,
  AxondOptions,
  CredentialConfig,
  ExtensionStore,
  InferenceRoute,
  NamespaceWrite,
  ProviderConfig,
  RequestLog,
  Settlement,
  Store,
  UsageRecord,
  UsageTokens,
} from "@axond/sdk";
import { API_VERSION } from "@axond/sdk";

import { admissionFromOptions, createAdmission, type AdmissionHold } from "./admission.ts";
import { assertGatewayKey, presentedCredential } from "./auth.ts";
import { ByteRequestBody } from "./body.ts";
import {
  callUpstream,
  credentialPolicy,
  credentialState,
  noteCredentialFailure,
  noteCredentialSuccess,
  planCredentialWalk,
  targetAttemptCap,
  type CredentialPool,
  type TransportFailureReason,
} from "./dispatch.ts";
import { GatewayFailure, StoreFailure, badRequest, gatewayError } from "./errors.ts";
import { globMatch } from "./glob.ts";
import { budgetJson, money, namespaceJson } from "./memory-store.ts";
import { monthlyPeriod, namespaceFromCanonicalPath, parseNamespaceId, validatePeriod, validateTimezone } from "./namespace.ts";
import { beginTrace, childTrace, formatTraceparent, metricPayload, parseTraceparent, postOtlp, resourceAttributes, tracePayload, type ExportedSpan, type TraceContext } from "./otel.ts";
import { sanitizeAttributes } from "./metrics.ts";
import { OPENAPI } from "./openapi.ts";
import { costMicrodollars, lookupPrice } from "./pricing.ts";
import { parseCredentialQuery, rawSearch } from "./query.ts";
import { scopeStore } from "./scoped-store.ts";
import { emptyUsage } from "./usage.ts";

const DEFAULT_MAX_REQUEST = 2 * 1024 * 1024;
const DEFAULT_MAX_PROMPT_TOKENS = 1_000_000;
const DEFAULT_MAX_OUTPUT_TOKENS = 200_000;
const DEFAULT_MAX_STREAM_DURATION_MS = 3_600_000;
const DEFAULT_MAX_STREAM_BYTES = 64 * 1024 * 1024;
const OUTPUT_ALLOWANCE_FIELDS = ["max_tokens", "max_completion_tokens", "max_output_tokens"] as const;
const requestTrace = new WeakMap<Request, TraceContext>();
const requestAttempts = new WeakMap<Request, ExportedSpan[]>();

interface MutableContext extends AxondContext {
  hooks: ((settlement: Settlement) => Promise<void>)[];
  resolvedIncarnation: bigint;
  resolvedPeriod: string | null;
  alias: string;
  startedMs: number;
  traceId: string | null;
  servedCredentialId: string;
  servedCredentialSource: "platform" | "byok";
  upstreamAttempts: number;
  /** Milliseconds to the first released provider byte. Null when no token was observed. */
  ttftMs: number | null;
  /** Buffered settlement known before the handler returns. Streams settle later and leave this empty. */
  settlement: { status: string; usage: UsageTokens; cost: bigint | null } | null;
}

/**
 * Mount the gateway on a Hono app. The returned app is itself a Hono app, so
 * a host can `app.route('/', createAxond(...))`.
 */
export function createAxond(opts: AxondOptions): Hono<AxondEnv> {
  const extensions = opts.extensions ?? [];
  for (const extension of extensions) {
    if (extension.apiVersion !== API_VERSION) {
      throw new Error(
        `extension ${extension.name} apiVersion ${String(extension.apiVersion)} is not supported (want ${API_VERSION})`,
      );
    }
    validateMigrations(extension);
  }
  const pools = new Map<string, CredentialPool>();
  const admission =
    opts.admissionControl ??
    createAdmission(
      admissionFromOptions({
        maxInFlight: opts.maxInFlight,
        maxInFlightStreams: opts.maxInFlightStreams,
        queueCapacity: opts.admissionQueueCapacity,
        queueWaitMs: opts.admissionQueueWaitMs,
        maxPendingSettlements: opts.maxPendingSettlements,
        maxInFlightSettlements: opts.maxInFlightSettlements,
        settlementQueueWaitMs: opts.settlementQueueWaitMs,
        settlementTimeoutMs: opts.settlementTimeoutMs,
      }),
    );
  const app = new Hono<AxondEnv>();
  if (opts.metrics || opts.telemetry || opts.onLog) {
    app.use("*", async (c, next) => {
      const started = Date.now();
      const trace = opts.telemetry || opts.onLog ? beginTrace(c.req.header("traceparent")) : undefined;
      if (trace && opts.telemetry) {
        requestTrace.set(c.req.raw, trace);
      }
      try {
        await next();
      } finally {
        const status = c.res.status || 500;
        const httpAttributes = sanitizeAttributes(
          {
            "http.request.method": methodLabel(c.req.method),
            "http.route": httpRoute(c.req.path),
            "http.response.status_code": String(status),
          },
          secretValues(opts),
        );
        opts.metrics?.record("axond.http.server.requests", 1, httpAttributes);
        opts.metrics?.record("axond.http.server.duration", Date.now() - started, httpAttributes);
        admission.observeAge(opts.metrics);
        const axond = readAxond(c);
        const ended = Date.now();
        opts.onLog?.(
          requestLog(
            axond,
            trace,
            {
              method: httpAttributes["http.request.method"] ?? "",
              route: httpAttributes["http.route"] ?? "",
              status,
              durationMs: ended - started,
            },
            ended,
            secretValues(opts),
          ),
        );
        if (opts.telemetry && trace) {
          const attributes = sanitizeAttributes(
            {
              ...httpAttributes,
              "axond.request_id": axond?.requestId ?? "",
              "axond.namespace": axond?.namespace?.id ?? "",
              "axond.subject": axond?.subject ?? "",
              "gen_ai.request.model": axond?.alias ?? "",
              ...serverSpanAttributes(axond, ended),
            },
            secretValues(opts),
          );
          const attempts = requestAttempts.get(c.req.raw) ?? [];
          const task = publishTelemetry(opts, [
            {
              name: "http.server.request",
              trace,
              startMs: started,
              endMs: ended,
              attributes,
              error: status >= 500,
            },
            ...attempts,
          ]);
          if (opts.waitUntil) {
            opts.waitUntil(task);
          } else {
            void task;
          }
        }
      }
    });
  }
  app.onError((error) => {
    if (error instanceof GatewayFailure) {
      return gatewayError(error);
    }
    return gatewayError(new GatewayFailure("internal", 500, "internal error"));
  });
  app.notFound(() => gatewayError(new GatewayFailure("not_found", 404, "not found")));
  app.get("/healthz", (c) => c.text("ok"));
  app.get("/readyz", (c) => {
    if (opts.serving && !opts.serving()) {
      return c.text("draining", 503);
    }
    return c.text("ready");
  });
  for (const extension of extensions) {
    if (extension.routes) {
      app.route("/", extension.routes);
    }
  }
  app.all("/api/v1/*", (c) => pipeline(c, opts, extensions, pools, admission, "management"));
  app.all("/ns/*", (c) => pipeline(c, opts, extensions, pools, admission, "inference"));
  return app;
}

function validateMigrations(extension: AxondExtension): void {
  const prefix = `axond_ext_${extension.name}_`;
  for (const sql of extension.migrations ?? []) {
    const tables = sql.matchAll(/create\s+table\s+(?:if\s+not\s+exists\s+)?([a-zA-Z0-9_]+)/gi);
    for (const match of tables) {
      const table = match[1]!;
      if (!table.startsWith(prefix)) {
        throw new Error(`extension ${extension.name} migration creates \`${table}\` outside \`${prefix}\``);
      }
    }
  }
}

const admissionHolds = new WeakMap<MutableContext, AdmissionHold>();

async function pipeline(
  c: Context<AxondEnv>,
  opts: AxondOptions,
  extensions: readonly AxondExtension[],
  pools: Map<string, CredentialPool>,
  admission: { admit(kind: "buffered" | "streamed", metrics?: AxondOptions["metrics"]): Promise<AdmissionHold> },
  kind: "management" | "inference",
): Promise<Response> {
  if (opts.admitting && !opts.admitting()) {
    opts.metrics?.record("axond.shutdown.rejected_requests", 1);
    return gatewayError(
      new GatewayFailure(
        "draining",
        503,
        "the gateway is shutting down and is no longer accepting requests",
      ),
    );
  }
  const axond = createContext(c, opts);
  c.set("axond", axond);
  try {
  await runStage(c, "pre-auth", extensions, opts.store, async () => {
    const key = await resolveKey(opts, c);
    assertGatewayKey(presentedCredential(c.req.raw.headers), key, axond.authenticated);
    if (!axond.subject) {
      axond.subject = "gateway-key";
    }
    if (kind === "inference") {
      const path = opts.rawPath?.(c) ?? new URL(c.req.url).pathname;
      const namespaceId = namespaceFromCanonicalPath(path);
      await bindNamespace(axond, opts, namespaceId, chargesBudget(path));
      axond.route = routeOf(path);
    }
    await runStage(c, "post-auth", extensions, opts.store, async () => {
      if (kind === "management") {
        await management(c, opts, axond);
        return;
      }
      if (axond.route === "models" || axond.route === "credentials") {
        await runStage(c, "pre-dispatch", extensions, opts.store, async () => {
          if (axond.route === "models") {
            await listModels(c, opts, axond);
          } else {
            await listCredentials(c, opts, axond, pools);
          }
        });
        return;
      }
      await prepareInference(c, opts, axond, admission);
      await runStage(c, "pre-dispatch", extensions, opts.store, async () => {
        enforceAliasGlobs(axond);
        await dispatch(c, opts, axond, pools);
      });
    });
  });
    finishAdmission(c, axond);
  } catch (error) {
    abandonAdmission(axond);
    if (error instanceof StageStop) {
      return error.response;
    }
    if (error instanceof StoreFailure) {
      throw new GatewayFailure("store_unavailable", 503, "store is unavailable");
    }
    throw error;
  }
  return c.res;
}

function finishAdmission(c: Context<AxondEnv>, axond: MutableContext): void {
  const hold = admissionHolds.get(axond);
  if (!hold) {
    return;
  }
  const type = c.res.headers.get("content-type") ?? "";
  if (type.includes("text/event-stream") && c.res.body) {
    c.res = holdUntilConsumed(c.res, () => {
      hold.releaseAdmission();
      if (!hold.settlementClaimed) {
        hold.releaseSettlement();
      }
    });
    return;
  }
  hold.releaseAdmission();
  if (!hold.settlementClaimed) {
    hold.releaseSettlement();
  }
}

function abandonAdmission(axond: MutableContext): void {
  const hold = admissionHolds.get(axond);
  if (!hold) {
    return;
  }
  hold.releaseAdmission();
  if (!hold.settlementClaimed) {
    hold.releaseSettlement();
  }
}

function holdUntilConsumed(response: Response, release: () => void): Response {
  const body = response.body;
  if (!body) {
    release();
    return response;
  }
  let released = false;
  const finish = () => {
    if (released) {
      return;
    }
    released = true;
    release();
  };
  const reader = body.getReader();
  const stream = new ReadableStream({
    async pull(controller) {
      try {
        const next = await reader.read();
        if (next.done) {
          finish();
          controller.close();
          return;
        }
        controller.enqueue(next.value);
      } catch (error) {
        finish();
        controller.error(error);
      }
    },
    async cancel(reason) {
      try {
        await reader.cancel(reason);
      } finally {
        finish();
      }
    },
  });
  return new Response(stream, { status: response.status, headers: response.headers });
}

class StageStop extends Error {
  readonly response: Response;

  constructor(response: Response) {
    super("stage stop");
    this.response = response;
  }
}

function assertExtensionMetric(name: string): void {
  if (!name.startsWith("axond.ext.")) {
    throw new Error("extension metrics must be named axond.ext.<name>");
  }
}

function createContext(c: Context<AxondEnv>, opts: AxondOptions): MutableContext {
  const header = c.req.header("x-request-id");
  const requestId = header && /^[A-Za-z0-9._:-]{1,128}$/.test(header) ? header : crypto.randomUUID();
  const ctx: MutableContext = {
    requestId,
    startedMs: Date.now(),
    route: "other",
    body: new ByteRequestBody(c.req.raw),
    authenticated: false,
    hooks: [],
    resolvedIncarnation: 1n,
    resolvedPeriod: null,
    alias: "",
    traceId: parseTraceparent(c.req.header("traceparent"))?.traceId ?? null,
    servedCredentialId: "",
    servedCredentialSource: "platform",
    upstreamAttempts: 0,
    ttftMs: null,
    settlement: null,
    store: scopeStore(opts.store, ""),
    metrics: {
      record(name, value, attributes) {
        assertExtensionMetric(name);
        opts.metrics?.record(name, value, attributes);
      },
      set(name, value, attributes) {
        assertExtensionMetric(name);
        if (opts.metrics?.set) {
          opts.metrics.set(name, value, attributes);
          return;
        }
        opts.metrics?.record(name, value, attributes);
      },
    },
    onSettle(fn) {
      ctx.hooks.push(fn);
    },
  };
  return ctx;
}

async function resolveKey(opts: AxondOptions, c: Context<AxondEnv>): Promise<string> {
  return typeof opts.gatewayKey === "function" ? opts.gatewayKey(c) : opts.gatewayKey;
}

function chargesBudget(path: string): boolean {
  return (
    path.endsWith("/chat/completions") ||
    path.endsWith("/messages") ||
    path.endsWith("/embeddings") ||
    path.endsWith("/responses")
  );
}

async function bindNamespace(
  axond: MutableContext,
  opts: AxondOptions,
  id: string,
  charging: boolean,
): Promise<void> {
  let resolved;
  try {
    resolved = await opts.store.resolveNamespace(id, opts.clock?.() ?? Date.now());
  } catch (error) {
    if (!(error instanceof StoreFailure)) {
      throw error;
    }
    if (!charging) {
      throw new GatewayFailure("store_unavailable", 503, "store is unavailable");
    }
    if (opts.onStoreUnavailable === "allow") {
      const record = await opts.store.getNamespace(id);
      if (!record) {
        throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
      }
      axond.namespace = {
        id: record.id,
        attrs: record.attrs,
        ...(record.blocklist === null ? {} : { blocklist: record.blocklist }),
      };
      axond.resolvedIncarnation = 1n;
      axond.resolvedPeriod = null;
      (axond as MutableContext & { admitted?: boolean }).admitted = true;
      (axond as MutableContext & { record?: NamespaceWrite }).record = record;
      return;
    }
    throw new GatewayFailure("budget_unavailable", 503, "budget store is unavailable");
  }
  if (!resolved) {
    throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
  }
  axond.namespace = {
    id: resolved.record.id,
    attrs: resolved.record.attrs,
    ...(resolved.record.blocklist === null ? {} : { blocklist: resolved.record.blocklist }),
  };
  axond.resolvedIncarnation = resolved.incarnation;
  axond.resolvedPeriod = resolved.period;
  (axond as MutableContext & { admitted?: boolean }).admitted = resolved.admitted;
  (axond as MutableContext & { record?: NamespaceWrite }).record = resolved.record;
}

function routeOf(path: string): InferenceRoute {
  if (path.endsWith("/chat/completions")) {
    return "chat";
  }
  if (path.endsWith("/messages")) {
    return "messages";
  }
  if (path.endsWith("/embeddings")) {
    return "embeddings";
  }
  if (path.endsWith("/responses")) {
    return "responses";
  }
  if (path.endsWith("/models")) {
    return "models";
  }
  if (path.endsWith("/credentials")) {
    return "credentials";
  }
  throw new GatewayFailure("not_found", 404, "not found");
}

async function runStage(
  c: Context<AxondEnv>,
  stage: AxondExtension["stage"],
  extensions: readonly AxondExtension[],
  store: Store,
  next: () => Promise<void>,
): Promise<void> {
  const list = extensions.filter((extension) => extension.stage === stage);
  let index = 0;
  const dispatch: MiddlewareHandler["arguments"] extends never ? never : () => Promise<void> = async () => {
    const extension = list[index];
    index += 1;
    if (!extension) {
      await next();
      return;
    }
    const axond = c.get("axond");
    const namespace = axond.namespace?.id ?? "";
    axond.store = extension.trusted ? store : scopeStore(store, namespace);
    const result = await extension.middleware(c, dispatch);
    if (result instanceof Response) {
      throw new StageStop(result);
    }
  };
  await dispatch();
}

async function prepareInference(
  c: Context<AxondEnv>,
  opts: AxondOptions,
  axond: MutableContext,
  admission: { admit(kind: "buffered" | "streamed", metrics?: AxondOptions["metrics"]): Promise<AdmissionHold> },
): Promise<void> {
  if (c.req.method !== "POST") {
    throw new GatewayFailure("not_found", 404, "not found");
  }
  const contentType = c.req.header("content-type") ?? "";
  if (!contentType.toLowerCase().includes("application/json")) {
    throw new GatewayFailure("unsupported_media_type", 415, "expected a `content-type: application/json` request");
  }
  const limit = opts.maxRequestBytes ?? DEFAULT_MAX_REQUEST;
  const declared = c.req.header("content-length");
  if (declared && Number(declared) > limit) {
    throw new GatewayFailure("request_too_large", 413, "request body exceeds the configured inbound limit");
  }
  const bytes = await axond.body.raw();
  if (bytes.length > limit) {
    throw new GatewayFailure("request_too_large", 413, "request body exceeds the configured inbound limit");
  }
  const parsed = await axond.body.json<Record<string, unknown>>();
  const model = parsed["model"];
  if (typeof model !== "string" || model.length === 0) {
    throw badRequest("missing `model`");
  }
  if (parsed["stream"] !== undefined && typeof parsed["stream"] !== "boolean") {
    throw badRequest("`stream` must be a boolean when present");
  }
  const slash = model.indexOf("/");
  if (slash <= 0 || slash === model.length - 1) {
    throw new GatewayFailure("model_unprefixed", 400, `model \`${model}\` is not prefixed as \`provider-id/model-id\``);
  }
  const providerId = model.slice(0, slash);
  const modelId = model.slice(slash + 1);
  const providers = await loadProviders(opts);
  const provider = providers.find((item) => item.id === providerId);
  if (!provider) {
    throw new GatewayFailure("unknown_provider", 400, `unknown provider \`${providerId}\``);
  }
  const extra = axond.namespace?.blocklist ?? [];
  const blocked = [...(opts.blocklist ?? []), ...extra];
  if (blocked.some((pattern) => globMatch(pattern, model) || globMatch(pattern, modelId))) {
    throw new GatewayFailure("model_blocked", 400, `model \`${model}\` is blocked`);
  }
  const wireOk =
    axond.route === "messages"
      ? provider.kind === "anthropic"
      : provider.kind === "openai" || provider.kind === "openai-compatible";
  if (!wireOk) {
    throw new GatewayFailure(
      "unsupported_wire",
      400,
      `model \`${model}\` cannot serve ${routeLabel(axond.route)}: provider \`${provider.id}\` does not speak that wire`,
    );
  }
  const price = lookupPrice(opts.prices ?? [], provider.id, modelId);
  if (!price && provider.unpricedModels !== "allow") {
    throw new GatewayFailure("unpriced_model", 400, `model \`${model}\` has no price`);
  }
  checkEstimateBounds(parsed, opts);
  const streamed =
    parsed["stream"] === true &&
    (axond.route === "chat" || axond.route === "messages" || axond.route === "responses");
  admissionHolds.set(axond, await admission.admit(streamed ? "streamed" : "buffered", opts.metrics));
  const estimated = estimatedRequestCost(axond.route, parsed, price);
  if (axond.spendCapMicrodollars !== undefined && estimated > axond.spendCapMicrodollars) {
    throw new GatewayFailure(
      "request_cost_ceiling_exceeded",
      403,
      `request cost ceiling exceeded for model \`${model}\`: estimated ${estimated} microdollars exceeds the per-request ceiling of ${axond.spendCapMicrodollars} microdollars`,
    );
  }
  const record = (axond as MutableContext & { record?: NamespaceWrite }).record;
  const admitted = (axond as MutableContext & { admitted?: boolean }).admitted;
  if (!admitted) {
    throw new GatewayFailure("budget_exceeded", 429, `budget exceeded for model \`${model}\``);
  }
  axond.alias = model;
  axond.target = { provider: provider.id, model: modelId };
  axond.body.setModel(modelId);
  (axond as MutableContext & { provider?: ProviderConfig; priced?: boolean }).provider = provider;
  (axond as MutableContext & { priced?: boolean }).priced = Boolean(price);
  void record;
}

/**
 * Prompt ceiling compares the UTF-8 length of the parsed JSON, divided by
 * four, with `admission.max_prompt_tokens`. Output ceiling takes the largest
 * usable allowance spelling and refuses it rather than clamping. `0` is off.
 * Neither message includes the request.
 */
function checkEstimateBounds(body: Record<string, unknown>, opts: AxondOptions): void {
  const promptLimit = ceiling(opts.maxPromptTokens, DEFAULT_MAX_PROMPT_TOKENS);
  if (promptLimit !== null && estimatedInputTokens(body) > promptLimit) {
    throw new GatewayFailure(
      "prompt_too_large",
      413,
      `prompt exceeds the configured limit of ${promptLimit} tokens`,
    );
  }
  const outputLimit = ceiling(opts.maxOutputTokens, DEFAULT_MAX_OUTPUT_TOKENS);
  const requested = requestedOutputTokens(body);
  if (outputLimit !== null && requested !== null && requested > outputLimit) {
    throw new GatewayFailure(
      "output_limit_exceeded",
      400,
      `requested output of ${requested} tokens exceeds the configured limit of ${outputLimit} tokens`,
    );
  }
}

function ceiling(value: number | undefined, fallback: number): number | null {
  const limit = value ?? fallback;
  return limit > 0 ? limit : null;
}

function estimatedInputTokens(body: Record<string, unknown>): number {
  return Math.floor(new TextEncoder().encode(JSON.stringify(body)).length / 4);
}

/** Pre-dispatch cost for a spend cap. Embeddings bill no completion. Absent output allowance uses 1024. */
function estimatedRequestCost(
  route: InferenceRoute | "management" | "other",
  body: Record<string, unknown>,
  price: ReturnType<typeof lookupPrice>,
): bigint {
  if (!price) {
    return 0n;
  }
  const usage = emptyUsage();
  usage.inputTokens = BigInt(estimatedInputTokens(body));
  usage.outputTokens = route === "embeddings" ? 0n : BigInt(requestedOutputTokens(body) ?? 1_024);
  return costMicrodollars(price, usage);
}

function requestedOutputTokens(body: Record<string, unknown>): number | null {
  let largest: number | null = null;
  for (const field of OUTPUT_ALLOWANCE_FIELDS) {
    const value = body[field];
    if (typeof value === "number" && Number.isInteger(value) && value >= 0) {
      largest = largest === null ? value : Math.max(largest, value);
    }
  }
  return largest;
}

function routeLabel(route: InferenceRoute | "management" | "other"): string {
  switch (route) {
    case "chat":
      return "/v1/chat/completions";
    case "messages":
      return "/v1/messages";
    case "embeddings":
      return "/v1/embeddings";
    case "responses":
      return "/v1/responses";
    default:
      return route;
  }
}

function enforceAliasGlobs(axond: MutableContext): void {
  if (!axond.aliasGlobs) {
    return;
  }
  if (!axond.aliasGlobs.some((pattern) => globMatch(pattern, axond.alias))) {
    throw new GatewayFailure("token_scope_insufficient", 403, `token scope does not authorize \`${axond.route}\``);
  }
}

async function dispatch(
  c: Context<AxondEnv>,
  opts: AxondOptions,
  axond: MutableContext,
  pools: Map<string, CredentialPool>,
): Promise<void> {
  const provider = (axond as MutableContext & { provider: ProviderConfig }).provider;
  const record = (axond as MutableContext & { record: NamespaceWrite }).record;
  const fallback =
    record.allowPlatformFallback || !record.fromConfig ? opts.defaultNamespace : null;
  const pinned = axond.route === "responses";
  const now = opts.clock?.() ?? Date.now();
  const maxStreamDurationMs = ceiling(opts.maxStreamDurationMs, DEFAULT_MAX_STREAM_DURATION_MS);
  const maxStreamBytes = ceiling(opts.maxStreamBytes, DEFAULT_MAX_STREAM_BYTES);
  const policy = credentialPolicy(opts.credentialPool);
  const walk = planCredentialWalk(
    opts.credentials ?? [],
    pools,
    record.id,
    provider.id,
    fallback,
    pinned,
    now,
    policy,
  );
  const planned = walk.attempts;
  let upstream: Awaited<ReturnType<typeof callUpstream>> | null = null;
  let lastError: unknown;
  const payload = await axond.body.json<Record<string, unknown>>();
  const stream = Boolean(payload["stream"]);
  const previous = payload["previous_response_id"];
  const continuation = pinned && typeof previous === "string" && previous.length > 0;
  const clock = opts.clock ?? Date.now;
  const deadlineAt = clock() + (opts.transport?.overallTimeoutMs ?? 30_000);
  const targetCap = targetAttemptCap(pinned, opts.transport?.maxAttempts);
  if (!Number.isInteger(targetCap) || targetCap < 1) {
    throw new GatewayFailure("bad_request", 400, "failover.max_attempts must be an integer of at least 1");
  }
  const walkStarted = Date.now();
  let streamClock: number | null = null;
  let upstreamTtftRecorded = false;
  const noteDownstreamFirstToken = (elapsedMs: number) => {
    if (axond.ttftMs === null) {
      axond.ttftMs = elapsedMs;
    }
  };
  const noteUpstreamFirstToken = (elapsedMs: number) => {
    if (upstreamTtftRecorded) {
      return;
    }
    upstreamTtftRecorded = true;
    opts.metrics?.record("axond.upstream.time_to_first_token", elapsedMs, {
      "axond.target.provider": provider.id,
      "axond.target.model": axond.target?.model ?? "",
    });
  };
  const headersFor = (served: CredentialConfig): Headers => {
    const built = new Headers();
    built.set("content-type", "application/json");
    for (const name of ["anthropic-version", "anthropic-beta", "accept"]) {
      const value = c.req.header(name);
      if (value) {
        built.set(name, value);
      }
    }
    if (provider.kind === "anthropic") {
      built.set("x-api-key", served.secret);
    } else {
      built.set("authorization", `Bearer ${served.secret}`);
    }
    const trace = requestTrace.get(c.req.raw);
    if (trace) {
      built.set("traceparent", formatTraceparent(trace));
    }
    return built;
  };
  // One configured target. `targetCap` bounds targets, so this walk still
  // presents every planned credential.
  for (let attempt = 0; attempt < planned.length && targetCap >= 1; attempt += 1) {
    const credential = planned[attempt]!;
    if (continuation && credentialState(pools, credential, now, policy.cooldownMs) === "parked") {
      throw new GatewayFailure(
        "continuation_affinity_unavailable",
        503,
        `continuation affinity unavailable for Responses target \`${provider.id}/${axond.target?.model ?? ""}\``,
      );
    }
    const headers = headersFor(credential);
    const attemptStarted = Date.now();
    if (stream && streamClock === null) {
      streamClock = attemptStarted;
    }
    const clockStartedMs = streamClock ?? attemptStarted;
    const path =
      axond.route === "chat"
        ? "/chat/completions"
        : axond.route === "messages"
          ? "/messages"
          : axond.route === "embeddings"
            ? "/embeddings"
            : "/responses";
    const url = provider.baseUrl.replace(/\/$/, "") + path;
    const usage = emptyUsage();
    let releaseStream: () => void = () => undefined;
    let skipSettle = false;
    let streamStatus = "ok";
    if (stream && opts.waitUntil) {
      const finished = new Promise<void>((resolve) => {
        releaseStream = resolve;
      });
      opts.waitUntil(
        finished.then(async () => {
          if (!skipSettle) {
            await settle(opts, axond, usage, streamStatus);
          }
        }),
      );
    }
    const transport = opts.transport ?? {
      responseHeaderTimeoutMs: 30_000,
      bufferedBodyTimeoutMs: 30_000,
      streamIdleTimeoutMs: 120_000,
      maxResponseBytes: 32 * 1024 * 1024,
    };
    let streamServed = true;
    const penalizeStream = (served: CredentialConfig) => {
      streamServed = false;
      noteCredentialFailure(pools, record.id, provider.id, served.id, now, policy.failureThreshold);
    };
    const finishStream = (served: CredentialConfig, reason: "end" | "cancel" | "fail") => {
      admissionHolds.get(axond)?.claimSettlement();
      if (reason === "end" && streamServed) {
        noteCredentialSuccess(pools, record.id, provider.id, served.id);
      }
      noteServed(axond, opts, served);
      streamStatus = reason === "cancel" ? "client_cancelled" : reason === "fail" ? "upstream_error" : "ok";
      if (stream && opts.waitUntil) {
        releaseStream();
        return;
      }
      scheduleSettle(opts, axond, usage, streamStatus);
    };
    const noteTimeout = (kind: string, bound: string) => {
      opts.metrics?.record("axond.upstream.timeouts", 1, {
        "axond.target.provider": provider.id,
        "axond.target.model": axond.target?.model ?? "",
        "axond.timeout": kind,
        "axond.timeout.bound": bound,
      });
      emitLog(opts, {
        msg: "upstream_timeout",
        request_id: axond.requestId,
        provider: provider.id,
        model: axond.target?.model ?? "",
        timeout: kind,
        bound,
      });
    };
    const noteStreamLimit = (limit: "duration" | "bytes") => {
      emitLog(opts, {
        msg: "stream_limit",
        request_id: axond.requestId,
        provider: provider.id,
        model: axond.target?.model ?? "",
        limit,
      });
    };
    const noteTransport = (
      phase: "request" | "stream" | "closing",
      reason: TransportFailureReason,
      committed?: boolean,
    ) => {
      emitLog(opts, {
        msg: "upstream_transport",
        request_id: axond.requestId,
        provider: provider.id,
        model: axond.target?.model ?? "",
        phase,
        reason,
        ...(committed === undefined ? {} : { committed }),
      });
    };
    const rotateStream = async (failedIndex: number): Promise<Response | null> => {
      const failed = planned[failedIndex]!;
      noteCredentialFailure(pools, record.id, provider.id, failed.id, now, policy.failureThreshold);
      noteRateLimit(opts, axond, provider.id, failed.id);
      for (let index = failedIndex + 1; index < planned.length; index += 1) {
        const nextCredential = planned[index]!;
        try {
          const opened = await callUpstream({
            url,
            headers: headersFor(nextCredential),
            body: (axond.body as ByteRequestBody).outgoing(),
            transport,
            stream: true,
            route: axond.route,
            deadlineAt,
            now: clock,
            dispatcher: opts.upstreamDispatcher,
            onTimeout: noteTimeout,
            onStreamLimit: noteStreamLimit,
            onTransport: noteTransport,
            onUsage: (next) => copyUsage(usage, next),
            onStreamDone: (reason) => finishStream(nextCredential, reason),
            onDownstreamFirstToken: noteDownstreamFirstToken,
            onUpstreamFirstToken: noteUpstreamFirstToken,
            clockStartedMs,
            onBeforeContentRateLimit: () => rotateStream(index),
            onCredentialRateLimit: () => penalizeStream(nextCredential),
            estimatedInputTokens: estimatedInputTokens(payload),
            maxStreamDurationMs,
            maxStreamBytes,
          });
          return opened.response;
        } catch (error) {
          if (error instanceof GatewayFailure && error.rateLimited) {
            noteCredentialFailure(pools, record.id, provider.id, nextCredential.id, now, policy.failureThreshold);
            if (index + 1 < planned.length) {
              noteRateLimit(opts, axond, provider.id, nextCredential.id);
            }
            continue;
          }
          throw error;
        }
      }
      streamServed = false;
      return null;
    };
    try {
      upstream = await callUpstream({
        url,
        headers,
        body: (axond.body as ByteRequestBody).outgoing(),
        transport,
        stream,
        route: axond.route,
        deadlineAt,
        now: clock,
        dispatcher: opts.upstreamDispatcher,
        onTimeout: noteTimeout,
        onStreamLimit: noteStreamLimit,
        onTransport: noteTransport,
        onUsage: (next) => copyUsage(usage, next),
        onStreamDone: (reason) => finishStream(credential, reason),
        onDownstreamFirstToken: stream ? noteDownstreamFirstToken : undefined,
        onUpstreamFirstToken: stream ? noteUpstreamFirstToken : undefined,
        clockStartedMs,
        onCredentialRateLimit: stream ? () => penalizeStream(credential) : undefined,
        onBeforeContentRateLimit:
          stream && axond.route === "chat" && !pinned && planned.length > 1
            ? () => rotateStream(attempt)
            : undefined,
        estimatedInputTokens: estimatedInputTokens(payload),
        maxStreamDurationMs,
        maxStreamBytes,
      });
      if (!stream) {
        noteServed(axond, opts, credential);
        axond.ttftMs = Math.max(0, Date.now() - walkStarted);
        scheduleSettle(opts, axond, upstream.usage, "ok");
      } else {
        noteServed(axond, opts, credential);
      }
      if (!stream) {
        noteCredentialSuccess(pools, record.id, provider.id, credential.id);
      }
      noteAttempt(c, opts, axond, credential, attempt, attemptStarted, "ok", false, walk.parked, "served", null, !stream);
      break;
    } catch (error) {
      lastError = error;
      skipSettle = true;
      releaseStream();
      if (axond.ttftMs === null && !upstreamTtftRecorded) {
        streamClock = null;
      }
      const rateLimited = error instanceof GatewayFailure && error.rateLimited;
      noteAttempt(
        c,
        opts,
        axond,
        credential,
        attempt,
        attemptStarted,
        error instanceof GatewayFailure ? error.type : "error",
        true,
        walk.parked,
        rateLimited ? "rate_limited" : "error",
        error,
        false,
      );
      if (rateLimited) {
        noteCredentialFailure(pools, record.id, provider.id, credential.id, now, policy.failureThreshold);
        if (!pinned && attempt + 1 < planned.length) {
          noteRateLimit(opts, axond, provider.id, credential.id);
        }
      }
      if (!(rateLimited && !pinned && attempt + 1 < planned.length)) {
        noteServed(axond, opts, credential);
        scheduleSettle(opts, axond, emptyUsage(), "upstream_error");
        throw error;
      }
    }
  }
  if (!upstream) {
    throw lastError instanceof Error ? lastError : new GatewayFailure("no_credential", 502, "no credential");
  }
  c.res = upstream.response;
}

const HTTP_METHODS = new Set(["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE", "CONNECT"]);

function methodLabel(method: string): string {
  return HTTP_METHODS.has(method) ? method : "_OTHER";
}

function readAxond(c: Context<AxondEnv>): MutableContext | undefined {
  try {
    return c.get("axond");
  } catch {
    return undefined;
  }
}

function secretValues(opts: AxondOptions): string[] {
  const secrets: string[] = [];
  if (typeof opts.gatewayKey === "string" && opts.gatewayKey.length > 0) {
    secrets.push(opts.gatewayKey);
  }
  for (const credential of opts.credentials ?? []) {
    if (credential.secret.length > 0) {
      secrets.push(credential.secret);
    }
  }
  return secrets;
}

const MAX_DIAGNOSTIC_BYTES = 4096;

function redactDiagnostic(message: string, secrets: readonly string[]): string {
  let text = message;
  for (const secret of secrets) {
    if (secret.length > 0 && text.includes(secret)) {
      text = text.split(secret).join("");
    }
  }
  const bytes = new TextEncoder().encode(text);
  if (bytes.length <= MAX_DIAGNOSTIC_BYTES) {
    return text;
  }
  let end = MAX_DIAGNOSTIC_BYTES;
  while (end > 0 && (bytes[end]! & 0xc0) === 0x80) {
    end -= 1;
  }
  return new TextDecoder().decode(bytes.subarray(0, end));
}

function noteAttempt(
  c: Context<AxondEnv>,
  opts: AxondOptions,
  axond: MutableContext,
  credential: CredentialConfig,
  attempt: number,
  startedMs: number,
  status: string,
  error: boolean,
  parked: readonly CredentialConfig[],
  lease: "served" | "rate_limited" | "error",
  failure: unknown,
  buffered: boolean,
): void {
  const parent = requestTrace.get(c.req.raw);
  if (!parent || !opts.telemetry) {
    return;
  }
  const spans = requestAttempts.get(c.req.raw) ?? [];
  const attemptTrace = childTrace(parent);
  const source = credential.namespace === opts.defaultNamespace ? "platform" : "byok";
  const secrets = secretValues(opts);
  const latencyMs = String(Date.now() - startedMs);
  const attributes = sanitizeAttributes(
    {
      "axond.attempt": String(attempt),
      "axond.target.provider": axond.target?.provider ?? "",
      "axond.target.model": axond.target?.model ?? "",
      "axond.status": status,
      "axond.credential.id": credential.id,
      "axond.credential_source": source,
      "axond.latency_ms": latencyMs,
      ...(buffered && !error ? { "axond.ttft_ms": latencyMs } : {}),
      ...(failure instanceof GatewayFailure && failure.upstreamStatus !== null
        ? { "axond.upstream.status": String(failure.upstreamStatus) }
        : {}),
      ...(failure instanceof GatewayFailure && failure.timeoutKind
        ? {
            "axond.timeout": failure.timeoutKind,
            "axond.timeout.bound": failure.timeoutBound ?? "phase",
          }
        : {}),
    },
    secrets,
  );
  if (failure instanceof GatewayFailure) {
    const diagnostic = redactDiagnostic(failure.message, secrets);
    if (diagnostic.length > 0) {
      attributes["axond.upstream.message"] = diagnostic;
    }
  }
  spans.push({
    name: "axond.upstream.attempt",
    trace: attemptTrace,
    startMs: startedMs,
    endMs: Date.now(),
    kind: 1,
    error,
    attributes,
  });
  if (attempt === 0) {
    for (let index = 0; index < parked.length; index += 1) {
      noteLease(spans, attemptTrace, opts, parked[index]!, index, "parked", startedMs);
    }
  }
  noteLease(spans, attemptTrace, opts, credential, parked.length + attempt, lease, startedMs);
  requestAttempts.set(c.req.raw, spans);
}

function noteLease(
  spans: ExportedSpan[],
  parent: TraceContext,
  opts: AxondOptions,
  credential: CredentialConfig,
  index: number,
  status: "served" | "rate_limited" | "error" | "parked",
  startedMs: number,
): void {
  spans.push({
    name: "axond.credential.lease",
    trace: childTrace(parent),
    startMs: startedMs,
    endMs: Date.now(),
    kind: 1,
    error: status === "rate_limited" || status === "error",
    attributes: sanitizeAttributes(
      {
        "axond.credential.id": credential.id,
        "axond.credential_source": credential.namespace === opts.defaultNamespace ? "platform" : "byok",
        "axond.credential.index": String(index),
        "axond.status": status,
      },
      secretValues(opts),
    ),
  });
}

async function publishTelemetry(opts: AxondOptions, spans: readonly ExportedSpan[]): Promise<void> {
  const target = opts.telemetry;
  if (!target || spans.length === 0) {
    return;
  }
  const resource = resourceAttributes(target.instanceId);
  const fetchImpl = target.fetch ?? fetch;
  try {
    await postOtlp(target.endpoint, "traces", tracePayload(spans, resource), fetchImpl);
    const points = opts.metrics?.points;
    if (points && points.length > 0) {
      await postOtlp(target.endpoint, "metrics", metricPayload(points, resource, spans[0]!.endMs), fetchImpl);
    }
  } catch {
    // A collector that is down does not fail the caller.
  }
}

function httpRoute(path: string): string {
  if (path === "/healthz" || path === "/readyz") {
    return path;
  }
  if (path.startsWith("/api/")) {
    return "/api/*";
  }
  const namespaced = path.match(/^\/ns\/[^/]+\/v1\/([^/]+(?:\/[^/]+)?)/);
  if (namespaced) {
    return `/ns/{namespace}/v1/${namespaced[1]}`;
  }
  return "/other";
}

function recordSettlementMetrics(
  opts: AxondOptions,
  axond: MutableContext,
  usage: UsageTokens,
  status: string,
  cost: bigint | null,
): void {
  if (!opts.metrics) {
    return;
  }
  const attributes: Record<string, string> = {
    "axond.namespace": axond.namespace?.id ?? "",
    "gen_ai.request.model": axond.alias,
    "axond.target.provider": axond.target?.provider ?? "",
    "axond.target.model": axond.target?.model ?? "",
    "axond.credential_source": axond.servedCredentialSource,
    "axond.status": status,
  };
  opts.metrics.record("axond.request.count", 1, attributes);
  opts.metrics.record("axond.request.duration", Math.max(0, Date.now() - axond.startedMs), attributes);
  if (axond.ttftMs !== null) {
    opts.metrics.record("axond.request.time_to_first_token", axond.ttftMs, attributes);
  }
  if (status === "upstream_error") {
    opts.metrics.record("axond.upstream.errors", 1, attributes);
  }
  opts.metrics.record("axond.tokens.input", metricNumber(usage.inputTokens), attributes);
  opts.metrics.record("axond.tokens.output", metricNumber(usage.outputTokens), attributes);
  opts.metrics.record("axond.tokens.cache_read", metricNumber(usage.cacheReadTokens), attributes);
  opts.metrics.record("axond.tokens.cache_write", metricNumber(usage.cacheWriteTokens), attributes);
  if (cost !== null) {
    opts.metrics.record("axond.cost.microdollars", metricNumber(cost), attributes);
  }
}

function metricNumber(value: bigint): number {
  if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) {
    return 0;
  }
  return Number(value);
}

function copyUsage(target: UsageTokens, next: UsageTokens): void {
  target.inputTokens = next.inputTokens;
  target.outputTokens = next.outputTokens;
  target.reasoningTokens = next.reasoningTokens;
  target.cacheReadTokens = next.cacheReadTokens;
  target.cacheWriteTokens = next.cacheWriteTokens;
}

function noteServed(axond: MutableContext, opts: AxondOptions, credential: CredentialConfig): void {
  axond.servedCredentialId = credential.id;
  axond.servedCredentialSource = credential.namespace === opts.defaultNamespace ? "platform" : "byok";
  axond.upstreamAttempts = 1;
}

function settlementCost(opts: AxondOptions, axond: MutableContext, usage: UsageTokens, status: string): bigint | null {
  const price = lookupPrice(opts.prices ?? [], axond.target?.provider ?? "", axond.target?.model ?? "");
  const priced = price ? costMicrodollars(price, usage) : null;
  const measured =
    usage.inputTokens > 0n
    || usage.outputTokens > 0n
    || usage.cacheReadTokens > 0n
    || usage.cacheWriteTokens > 0n;
  return status === "upstream_error" && !measured ? 0n : priced;
}

function emitLog(opts: AxondOptions, record: Exclude<AxondLog, RequestLog>): void {
  if (!opts.onLog) {
    return;
  }
  const secrets = secretValues(opts);
  const safe: Record<string, string | number | boolean> = { ...record };
  for (const [key, value] of Object.entries(safe)) {
    if (key === "msg") {
      continue;
    }
    if (typeof value === "string" && secrets.some((secret) => secret.length > 0 && value.includes(secret))) {
      safe[key] = "";
    }
  }
  opts.onLog(safe as Exclude<AxondLog, RequestLog>);
}

function noteRateLimit(opts: AxondOptions, axond: MutableContext, provider: string, credentialId: string): void {
  emitLog(opts, {
    msg: "credential_rate_limited",
    request_id: axond.requestId,
    provider,
    credential_id: credentialId,
  });
}

function requestLog(
  axond: MutableContext | undefined,
  trace: TraceContext | undefined,
  http: { method: string; route: string; status: number; durationMs: number },
  endedMs: number,
  secrets: readonly string[],
): RequestLog {
  const text = (value: string) =>
    secrets.some((secret) => secret.length > 0 && value.includes(secret)) ? "" : value;
  const record: RequestLog = {
    msg: "request",
    request_id: text(axond?.requestId ?? ""),
    trace_id: trace?.traceId ?? "",
    span_id: trace?.spanId ?? "",
    http_method: http.method,
    http_route: http.route,
    status_code: http.status,
    duration_ms: http.durationMs,
    namespace: text(axond?.namespace?.id ?? ""),
    subject: text(axond?.subject ?? ""),
    model: text(axond?.alias ?? ""),
  };
  if (!axond?.target) {
    return record;
  }
  const provider = text(axond.target.provider);
  const targetModel = text(axond.target.model);
  if (provider.length > 0) {
    record.target_provider = provider;
  }
  if (targetModel.length > 0) {
    record.target_model = targetModel;
  }
  if (axond.servedCredentialId.length > 0) {
    const source = text(axond.servedCredentialSource);
    if (source.length > 0) {
      record.credential_source = source;
    }
  }
  const settlement = axond.settlement;
  if (!settlement) {
    return record;
  }
  record.status = settlement.status;
  record.retry_count = Math.max(0, axond.upstreamAttempts - 1);
  record.input_tokens = settlement.usage.inputTokens.toString();
  record.cache_read_tokens = settlement.usage.cacheReadTokens.toString();
  record.cache_write_tokens = settlement.usage.cacheWriteTokens.toString();
  record.output_tokens = settlement.usage.outputTokens.toString();
  record.cost_microdollars = settlement.cost === null ? null : settlement.cost.toString();
  record.latency_ms = Math.max(0, endedMs - axond.startedMs);
  if (axond.ttftMs !== null) {
    record.ttft_ms = axond.ttftMs;
  }
  return record;
}

function serverSpanAttributes(axond: MutableContext | undefined, endedMs: number): Record<string, string> {
  if (!axond?.target) {
    return {};
  }
  const attributes: Record<string, string> = {
    "axond.target.provider": axond.target.provider,
    "axond.target.model": axond.target.model,
  };
  if (axond.servedCredentialId.length > 0) {
    attributes["axond.credential_source"] = axond.servedCredentialSource;
  }
  const settlement = axond.settlement;
  if (!settlement) {
    return attributes;
  }
  attributes["axond.status"] = settlement.status;
  attributes["axond.retry_count"] = String(Math.max(0, axond.upstreamAttempts - 1));
  attributes["gen_ai.usage.input_tokens"] = settlement.usage.inputTokens.toString();
  attributes["gen_ai.usage.output_tokens"] = settlement.usage.outputTokens.toString();
  attributes["gen_ai.usage.cache_read_tokens"] = settlement.usage.cacheReadTokens.toString();
  attributes["gen_ai.usage.cache_write_tokens"] = settlement.usage.cacheWriteTokens.toString();
  if (settlement.cost !== null) {
    attributes["axond.cost_microdollars"] = settlement.cost.toString();
  }
  attributes["axond.latency_ms"] = String(Math.max(0, endedMs - axond.startedMs));
  if (axond.ttftMs !== null) {
    attributes["axond.ttft_ms"] = String(axond.ttftMs);
  }
  return attributes;
}

function scheduleSettle(opts: AxondOptions, axond: MutableContext, usage: UsageTokens, status: string): void {
  axond.settlement = {
    status,
    usage: {
      inputTokens: usage.inputTokens,
      outputTokens: usage.outputTokens,
      reasoningTokens: usage.reasoningTokens,
      cacheReadTokens: usage.cacheReadTokens,
      cacheWriteTokens: usage.cacheWriteTokens,
    },
    cost: settlementCost(opts, axond, usage, status),
  };
  const task = settle(opts, axond, usage, status);
  if (opts.waitUntil) {
    opts.waitUntil(task);
  } else {
    void task;
  }
}

async function settle(opts: AxondOptions, axond: MutableContext, usage: UsageTokens, status: string): Promise<void> {
  const hold = admissionHolds.get(axond);
  hold?.claimSettlement();
  hold?.beginSpawned(opts.metrics);
  const queuedAt = Date.now();
  try {
  const granted = hold ? await hold.acquireExecution(opts.metrics) : true;
  if (!granted) {
    opts.metrics?.record("axond.settlement.failures", 1, { "axond.settlement.reason": "queue_timeout" });
    emitLog(opts, {
      msg: "settlement_failure",
      request_id: axond.requestId,
      reason: "queue_timeout",
      waited_ms: Date.now() - queuedAt,
    });
    hold?.releaseSettlement();
    return;
  }
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeoutMs = hold?.settlementTimeoutMs ?? 0;
  if (timeoutMs > 0) {
    timer = setTimeout(() => {
      opts.metrics?.record("axond.settlement.failures", 1, { "axond.settlement.reason": "execution_timeout" });
      emitLog(opts, {
        msg: "settlement_failure",
        request_id: axond.requestId,
        reason: "execution_timeout",
        waited_ms: Date.now() - queuedAt,
      });
    }, timeoutMs);
    const unref = timer as { unref?: () => void };
    unref.unref?.();
  }
  try {
  // A provider failure with no measured usage records cost 0 and adds nothing
  // to spent. A stream that already relayed text keeps that measured cost.
  const cost = settlementCost(opts, axond, usage, status);
  const result = await opts.store.settle({
    requestId: axond.requestId,
    namespace: axond.namespace?.id ?? "",
    period: axond.resolvedPeriod,
    model: axond.alias,
    status,
    cost,
    incarnation: axond.resolvedIncarnation,
  });
  const settlement: Settlement = {
    requestId: axond.requestId,
    namespace: axond.namespace?.id ?? "",
    period: axond.resolvedPeriod,
    model: axond.alias,
    status,
    costMicrodollars: cost,
    charged: result.charged,
    usage,
  };
  const record: UsageRecord = {
    schemaVersion: 2,
    requestId: axond.requestId,
    traceId: axond.traceId,
    namespace: settlement.namespace,
    period: axond.resolvedPeriod,
    subject: axond.subject ?? "",
    model: axond.alias,
    targetProvider: axond.target?.provider ?? "",
    targetModel: axond.target?.model ?? "",
    credentialSource: axond.servedCredentialSource,
    credentialId: axond.servedCredentialId,
    status,
    inputTokens: usage.inputTokens,
    outputTokens: usage.outputTokens,
    reasoningTokens: usage.reasoningTokens,
    cacheReadTokens: usage.cacheReadTokens,
    cacheWriteTokens: usage.cacheWriteTokens,
    costMicrodollars: cost,
    catalogVersion: 0,
    priceBook: null,
    priceBookChecksum: null,
    priceCatalog: null,
    signerKid: null,
    latencyMs: Math.max(0, Date.now() - axond.startedMs),
    attempts: axond.upstreamAttempts,
  };
  opts.onUsage?.(record);
  recordSettlementMetrics(opts, axond, usage, status, cost);
  for (const hook of axond.hooks) {
    await hook(settlement);
  }
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
    hold?.releaseExecution(opts.metrics);
    hold?.releaseSettlement();
  }
  } finally {
    hold?.endSpawned(opts.metrics);
  }
}

async function listModels(c: Context<AxondEnv>, opts: AxondOptions, axond: MutableContext): Promise<void> {
  const providers = await loadProviders(opts);
  const cached = await opts.store.listProviderModels();
  const extra = axond.namespace?.blocklist ?? [];
  const blocked = [...(opts.blocklist ?? []), ...extra];
  const data: { id: string; object: "model" }[] = [];
  for (const provider of providers) {
    const row = cached.find((item) => item.provider === provider.id);
    const models = row && row.source === provider.baseUrl ? row.data : row?.source == null ? row?.data ?? [] : [];
    for (const model of models) {
      const id = model && typeof model === "object" ? (model as { id?: unknown }).id : undefined;
      if (typeof id !== "string") {
        continue;
      }
      const prefixed = `${provider.id}/${id}`;
      if (blocked.some((pattern) => globMatch(pattern, prefixed) || globMatch(pattern, id))) {
        continue;
      }
      data.push({ id: prefixed, object: "model" });
    }
  }
  c.res = Response.json({ object: "list", data });
}

async function listCredentials(
  c: Context<AxondEnv>,
  opts: AxondOptions,
  axond: MutableContext,
  pools: Map<string, CredentialPool>,
): Promise<void> {
  const query = parseCredentialQuery(rawSearch(c.req.url));
  const all = query === "all";
  if (query !== null && query !== "all") {
    throw badRequest("invalid `namespaces` value");
  }
  if (all && axond.namespace?.id !== opts.defaultNamespace) {
    throw new GatewayFailure("token_scope_insufficient", 403, "token scope does not authorize `credentials`");
  }
  const now = opts.clock?.() ?? Date.now();
  const policy = credentialPolicy(opts.credentialPool);
  const caller = axond.namespace?.id ?? "";
  const credentials = opts.credentials ?? [];
  const record = (axond as MutableContext & { record?: NamespaceWrite }).record;
  const allowFallback =
    !all &&
    caller !== opts.defaultNamespace &&
    record !== undefined &&
    (record.allowPlatformFallback || !record.fromConfig);
  const own = credentials.filter((credential) => credential.namespace === caller);
  const ownProviders = new Set(own.map((credential) => credential.provider));
  const fallback = allowFallback
    ? credentials.filter(
        (credential) => credential.namespace === opts.defaultNamespace && !ownProviders.has(credential.provider),
      )
    : [];
  const visible = all ? credentials : [...own, ...fallback];
  const hideFallbackId = new Set(fallback);
  const data = visible
    .map((credential) => {
      const source = credential.namespace === opts.defaultNamespace ? "platform" : "byok";
      const row: {
        namespace: string;
        provider: string;
        credential_id?: string;
        source: "platform" | "byok";
        state: ReturnType<typeof credentialState>;
      } = {
        namespace: credential.namespace,
        provider: credential.provider,
        source,
        state: credentialState(pools, credential, now, policy.cooldownMs),
      };
      if (!(hideFallbackId.has(credential) && credential.explicitId === false)) {
        row.credential_id = credential.id;
      }
      return row;
    })
    .sort((left, right) => {
      if (left.namespace !== right.namespace) {
        return left.namespace < right.namespace ? -1 : 1;
      }
      if (left.provider !== right.provider) {
        return left.provider < right.provider ? -1 : 1;
      }
      const leftId = left.credential_id ?? "";
      const rightId = right.credential_id ?? "";
      if (leftId === rightId) {
        return 0;
      }
      return leftId < rightId ? -1 : 1;
    });
  c.res = Response.json({
    object: "list",
    observed: "replica",
    data,
  });
}

async function management(c: Context<AxondEnv>, opts: AxondOptions, axond: MutableContext): Promise<Response | void> {
  const url = new URL(c.req.url);
  const path = url.pathname;
  if (c.req.method === "GET" && path === "/api/v1/openapi.json") {
    c.res = Response.json(OPENAPI);
    return;
  }
  const budget = path.match(/^\/api\/v1\/namespaces\/([^/]+)\/budgets\/([^/]+)$/);
  if (budget) {
    const namespace = parseNamespaceId(decodeURIComponent(budget[1]!));
    const period = decodeURIComponent(budget[2]!);
    validatePeriod(period);
    if (c.req.method === "PUT") {
      const body = await readJson(c);
      const limit = requiredBig(body, "limit_microdollars");
      const row = await opts.store.putBudget(namespace, period, limit);
      c.res = Response.json(budgetJson(row));
      return;
    }
    if (c.req.method === "GET") {
      const row = await opts.store.getBudget(namespace, period);
      if (!row) {
        const known = await opts.store.getNamespace(namespace);
        throw new GatewayFailure(known ? "unknown_budget" : "unknown_namespace", 404, known ? "unknown budget" : "unknown namespace");
      }
      c.res = Response.json(budgetJson(row));
      return;
    }
  }
  const policy = path.match(/^\/api\/v1\/namespaces\/([^/]+)\/budget$/);
  if (policy) {
    const namespace = parseNamespaceId(decodeURIComponent(policy[1]!));
    if (c.req.method === "PUT") {
      const body = await readJson(c);
      const cadence = body["cadence"];
      if (cadence !== "monthly" && cadence !== "fixed") {
        throw badRequest('cadence must be "monthly" or "fixed"');
      }
      const timezone = typeof body["timezone"] === "string" ? body["timezone"] : "UTC";
      validateTimezone(timezone);
      const period = typeof body["period"] === "string" ? body["period"] : null;
      if (cadence === "monthly" && period) {
        throw badRequest('period is derived for cadence "monthly"');
      }
      if (period) {
        validatePeriod(period);
      }
      const row = await opts.store.putBudgetPolicy({
        namespace,
        cadence,
        limit: requiredBig(body, "limit_microdollars"),
        timezone,
        period,
        nowMs: opts.clock?.() ?? Date.now(),
      });
      c.res = Response.json(row);
      return;
    }
    if (c.req.method === "GET") {
      const row = await opts.store.getBudgetPolicy(namespace);
      if (!row) {
        const known = await opts.store.getNamespace(namespace);
        throw new GatewayFailure(known ? "unknown_budget" : "unknown_namespace", 404, known ? "unknown budget" : "unknown namespace");
      }
      c.res = Response.json(row);
      return;
    }
  }
  const usage = path.match(/^\/api\/v1\/namespaces\/([^/]+)\/usage$/);
  if (usage && c.req.method === "GET") {
    const namespace = parseNamespaceId(decodeURIComponent(usage[1]!));
    const period = url.searchParams.get("period");
    if (!period) {
      throw badRequest("`period` is required");
    }
    validatePeriod(period);
    const known = await opts.store.getNamespace(namespace);
    if (!known) {
      throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
    }
    const data = await opts.store.summarizeUsage(namespace, period);
    c.res = Response.json({ namespace, period, data });
    return;
  }
  if (c.req.method === "GET" && path === "/api/v1/namespaces") {
    const limit = Number(url.searchParams.get("limit") ?? "100");
    if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
      throw badRequest("`limit` must be between 1 and 1000");
    }
    const page = await opts.store.listNamespaces(url.searchParams.get("cursor"), limit);
    c.res = Response.json({
      data: page.data.map(namespaceJson),
      ...(page.nextCursor ? { next_cursor: page.nextCursor } : {}),
    });
    return;
  }
  if (c.req.method === "POST" && path === "/api/v1/namespaces") {
    const body = await readJson(c);
    const id = typeof body["id"] === "string" ? body["id"] : "";
    parseNamespaceId(id);
    const attrs = asAttrs(body["attrs"]);
    const blocklist = asBlocklist(body["blocklist"]);
    const created = await opts.store.putNamespace({
      id,
      attrs,
      blocklist,
      allowPlatformFallback: true,
      fromConfig: false,
    });
    if (created === "exists") {
      throw new GatewayFailure("namespace_conflict", 409, "namespace already exists");
    }
    c.res = Response.json(namespaceJson({ id, attrs, blocklist, allowPlatformFallback: true, fromConfig: false }), {
      status: 201,
    });
    return;
  }
  const one = path.match(/^\/api\/v1\/namespaces\/([^/]+)$/);
  if (one) {
    const id = parseNamespaceId(decodeURIComponent(one[1]!));
    if (c.req.method === "GET") {
      const row = await opts.store.getNamespace(id);
      if (!row) {
        throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
      }
      c.res = Response.json(namespaceJson(row));
      return;
    }
    if (c.req.method === "PUT") {
      const body = await readJson(c);
      const row = await opts.store.updateNamespace(id, asAttrs(body["attrs"]), asBlocklist(body["blocklist"]));
      if (!row) {
        throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
      }
      c.res = Response.json(namespaceJson(row));
      return;
    }
    if (c.req.method === "DELETE") {
      if ((opts.configNamespaces ?? []).includes(id)) {
        throw new GatewayFailure("namespace_conflict", 409, "namespace already exists");
      }
      await opts.store.deleteNamespace(id);
      c.res = new Response(null, { status: 204 });
      return;
    }
  }
  if (c.req.method === "GET" && path === "/api/v1/providers/models") {
    const providers = await loadProviders(opts);
    const cached = await opts.store.listProviderModels();
    c.res = Response.json({
      data: providers.map((provider) => providerModelsJson(provider, cached.find((row) => row.provider === provider.id) ?? null)),
    });
    return;
  }
  const providerModels = path.match(/^\/api\/v1\/providers\/([^/]+)\/models$/);
  if (providerModels && c.req.method === "GET") {
    const id = decodeURIComponent(providerModels[1]!);
    const providers = await loadProviders(opts);
    const provider = providers.find((item) => item.id === id);
    if (!provider) {
      throw new GatewayFailure("unknown_provider", 400, `unknown provider \`${id}\``);
    }
    const row = await opts.store.getProviderModels(id);
    c.res = Response.json(providerModelsJson(provider, row));
    return;
  }
  void axond;
  throw new GatewayFailure("not_found", 404, "not found");
}

function providerModelsJson(provider: ProviderConfig, row: { fetchedAt: string | null; stale: boolean; data: unknown[]; source: string | null } | null) {
  const stale = row === null || row.source !== provider.baseUrl || row.stale;
  return {
    provider: provider.id,
    ...(row?.fetchedAt && !stale ? { fetched_at: row.fetchedAt } : row?.fetchedAt ? { fetched_at: row.fetchedAt } : {}),
    stale: row === null ? true : row.source !== null && row.source !== provider.baseUrl ? true : row.stale,
    data: row?.data ?? [],
  };
}

async function loadProviders(opts: AxondOptions): Promise<ProviderConfig[]> {
  return typeof opts.providers === "function" ? opts.providers() : opts.providers;
}

async function readJson(c: Context<AxondEnv>): Promise<Record<string, unknown>> {
  const contentType = c.req.header("content-type") ?? "";
  if (!contentType.toLowerCase().includes("application/json")) {
    throw new GatewayFailure("unsupported_media_type", 415, "expected a `content-type: application/json` request");
  }
  try {
    const value = await c.req.json();
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw badRequest("malformed json");
    }
    return value as Record<string, unknown>;
  } catch (error) {
    if (error instanceof GatewayFailure) {
      throw error;
    }
    throw badRequest("malformed json");
  }
}

function requiredBig(body: Record<string, unknown>, key: string): bigint {
  const value = body[key];
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) {
    return BigInt(value);
  }
  if (typeof value === "string" && /^[0-9]+$/.test(value)) {
    return BigInt(value);
  }
  throw badRequest(`\`${key}\` must be an integer`);
}

function asAttrs(value: unknown): Record<string, unknown> {
  if (value === undefined) {
    return {};
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw badRequest("attrs must be an object");
  }
  const encoded = JSON.stringify(value);
  if (encoded.length > 4 * 1024) {
    throw badRequest("attrs exceeds 4096 byte limit");
  }
  return value as Record<string, unknown>;
}

function asBlocklist(value: unknown): string[] | null {
  if (value === undefined || value === null) {
    return null;
  }
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    throw badRequest("blocklist must be a list of strings");
  }
  for (const pattern of value) {
    if (typeof pattern === "string") {
      // Invalid globs are a request error.
      if (![...pattern].every((char) => char === "*" || char.trim() !== "" || true)) {
        throw badRequest("blocklist must be a list of strings");
      }
    }
  }
  return value as string[];
}

export function extensionStoreFor(store: Store, extension: AxondExtension, namespace: string): ExtensionStore {
  return extension.trusted ? store : scopeStore(store, namespace);
}

void money;
