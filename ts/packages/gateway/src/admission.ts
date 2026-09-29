import { GatewayFailure } from "./errors.ts";

/** Tokio's semaphore ceiling (`usize::MAX >> 3` on 64-bit), as decimal text. */
const MAX_PERMITS = "2305843009213693951";

const DEFAULT_MAX_IN_FLIGHT = 1024;
const DEFAULT_MAX_IN_FLIGHT_STREAMS = 512;

export interface AdmissionLimits {
  /** `0` disables the replica request ceiling. */
  maxInFlight: number;
  /** `0` disables the open-stream ceiling. */
  maxInFlightStreams: number;
  streamsExplicit: boolean;
  /** `0` disables the queue. Must be set together with `queueWaitMs`. */
  queueCapacity: number;
  queueWaitMs: number;
  /** `0` disables the unsettled-charge ceiling. */
  maxPendingSettlements: number;
  pendingExplicit: boolean;
  /** `0` disables the cap on settlements executing at once. Default 64. */
  maxInFlightSettlements: number;
  /** `0` waits without a bound. Default 10000. */
  settlementQueueWaitMs: number;
  /** `0` disables the execution deadline. Default 10000. The charge still finishes. */
  settlementTimeoutMs: number;
}

export interface AdmissionHold {
  /** True once `settle` has taken responsibility for the settlement slot. */
  readonly settlementClaimed: boolean;
  /** `0` means a running settlement is not timed. */
  readonly settlementTimeoutMs: number;
  claimSettlement(): void;
  releaseAdmission(): void;
  releaseSettlement(): void;
  /**
   * Wait for a settlement execution slot. `false` means the queue wait
   * expired and the charge must not run.
   */
  acquireExecution(metrics?: MetricSink): Promise<boolean>;
  releaseExecution(metrics?: MetricSink): void;
  /** Start the age clock for this request's spawned charge. */
  beginSpawned(metrics?: MetricSink): void;
  /** The spawned charge finished or was dropped. */
  endSpawned(metrics?: MetricSink): void;
}

export interface SettlementBacklog {
  spawned: number;
  oldestAgeMs: number;
}

interface MetricSink {
  record(name: string, value: number, attributes?: Record<string, string>): void;
  set?(name: string, value: number, attributes?: Record<string, string>): void;
}

interface Waiter {
  grant: () => void;
  timer: ReturnType<typeof setTimeout>;
}

/**
 * One replica's admission counters. A ceiling of `0` is off. Per-tenant
 * ceilings are not enforced here.
 */
export function createAdmission(limits: AdmissionLimits) {
  validateAdmission(limits);
  const requestLimit = limits.maxInFlight > 0 ? limits.maxInFlight : null;
  const streamLimit = limits.maxInFlightStreams > 0 ? limits.maxInFlightStreams : null;
  const queueCapacity = limits.queueCapacity > 0 ? limits.queueCapacity : null;
  const settlementLimit = limits.maxPendingSettlements > 0 ? limits.maxPendingSettlements : null;
  const executionLimit = limits.maxInFlightSettlements > 0 ? limits.maxInFlightSettlements : null;
  const executionWaitMs = limits.settlementQueueWaitMs > 0 ? limits.settlementQueueWaitMs : null;
  let requests = 0;
  let streams = 0;
  let pending = 0;
  let queueDepth = 0;
  let executing = 0;
  let sequence = 0;
  const waiters: Waiter[] = [];
  const executionWaiters: { grant: () => void }[] = [];
  const backlog: { id: number; enqueuedAt: number }[] = [];
  const idleWaiters: (() => void)[] = [];

  const record = (metrics: MetricSink | undefined, name: string, value: number, attributes?: Record<string, string>) => {
    metrics?.record(name, value, attributes);
  };
  const acquired = (metrics: MetricSink | undefined, resource: string) => {
    record(metrics, "axond.admission.in_flight", 1, { "axond.admission.resource": resource });
  };
  const released = (metrics: MetricSink | undefined, resource: string) => {
    record(metrics, "axond.admission.in_flight", -1, { "axond.admission.resource": resource });
  };
  const rejected = (metrics: MetricSink | undefined, resource: string, code: string) => {
    record(metrics, "axond.admission.rejections", 1, {
      "axond.admission.resource": resource,
      "axond.error.type": code,
    });
  };

  const releaseRequest = (metrics: MetricSink | undefined) => {
    const waiter = waiters.shift();
    if (waiter) {
      clearTimeout(waiter.timer);
      queueDepth -= 1;
      released(metrics, "queue");
      released(metrics, "request");
      acquired(metrics, "request");
      waiter.grant();
      return;
    }
    requests -= 1;
    released(metrics, "request");
  };

  const stage = (metrics: MetricSink | undefined, name: "reserved" | "queued" | "executing", delta: number) => {
    record(metrics, "axond.settlement.in_flight", delta, { "axond.settlement.stage": name });
  };

  const acquireExecution = (metrics: MetricSink | undefined): Promise<boolean> => {
    if (executionLimit === null) {
      return Promise.resolve(true);
    }
    if (executing < executionLimit) {
      executing += 1;
      stage(metrics, "executing", 1);
      return Promise.resolve(true);
    }
    const started = Date.now();
    stage(metrics, "queued", 1);
    return new Promise((resolve) => {
      let settled = false;
      const finish = (granted: boolean) => {
        if (settled) {
          return;
        }
        settled = true;
        if (timer) {
          clearTimeout(timer);
        }
        stage(metrics, "queued", -1);
        record(metrics, "axond.settlement.queue_wait", Date.now() - started);
        if (granted) {
          executing += 1;
          stage(metrics, "executing", 1);
        }
        resolve(granted);
      };
      const waiter = { grant: () => finish(true) };
      executionWaiters.push(waiter);
      let timer: ReturnType<typeof setTimeout> | undefined;
      if (executionWaitMs !== null) {
        timer = setTimeout(() => {
          const index = executionWaiters.indexOf(waiter);
          if (index >= 0) {
            executionWaiters.splice(index, 1);
          }
          finish(false);
        }, executionWaitMs);
        const unref = timer as { unref?: () => void };
        unref.unref?.();
      }
    });
  };

  const oldestPendingAgeMs = (): number => {
    if (backlog.length === 0) {
      return 0;
    }
    let oldest = backlog[0]!.enqueuedAt;
    for (const entry of backlog) {
      if (entry.enqueuedAt < oldest) {
        oldest = entry.enqueuedAt;
      }
    }
    return Math.max(0, Date.now() - oldest);
  };
  const observeAge = (metrics: MetricSink | undefined) => {
    metrics?.set?.("axond.settlement.oldest_pending_age", oldestPendingAgeMs());
  };
  const wakeIdle = () => {
    if (backlog.length !== 0) {
      return;
    }
    const waiting = idleWaiters.splice(0);
    for (const waiter of waiting) {
      waiter();
    }
  };
  const enqueueSpawned = (): number => {
    const id = sequence;
    sequence += 1;
    backlog.push({ id, enqueuedAt: Date.now() });
    return id;
  };
  const dequeueSpawned = (id: number) => {
    const index = backlog.findIndex((entry) => entry.id === id);
    if (index < 0) {
      return;
    }
    backlog.splice(index, 1);
    wakeIdle();
  };

  const releaseExecutionSlot = (metrics: MetricSink | undefined) => {
    if (executionLimit === null) {
      return;
    }
    const waiter = executionWaiters.shift();
    if (waiter) {
      executing -= 1;
      stage(metrics, "executing", -1);
      waiter.grant();
      return;
    }
    executing -= 1;
    stage(metrics, "executing", -1);
  };

  const acquireRequest = (metrics: MetricSink | undefined): Promise<boolean> => {
    if (requestLimit === null) {
      return Promise.resolve(false);
    }
    if (requests < requestLimit) {
      requests += 1;
      acquired(metrics, "request");
      return Promise.resolve(true);
    }
    if (queueCapacity === null) {
      rejected(metrics, "request", "gateway_overloaded");
      return Promise.reject(shed("gateway_overloaded", "gateway is at its concurrent request limit"));
    }
    if (queueDepth >= queueCapacity) {
      rejected(metrics, "queue", "admission_queue_full");
      return Promise.reject(shed("admission_queue_full", "admission queue is full"));
    }
    queueDepth += 1;
    acquired(metrics, "queue");
    record(metrics, "axond.admission.queue.depth", queueDepth);
    return new Promise((resolve, reject) => {
      const waiter: Waiter = {
        grant: () => resolve(true),
        timer: setTimeout(() => {
          const index = waiters.indexOf(waiter);
          if (index < 0) {
            return;
          }
          waiters.splice(index, 1);
          queueDepth -= 1;
          released(metrics, "queue");
          rejected(metrics, "queue", "admission_queue_timeout");
          reject(shed("admission_queue_timeout", "admission queue wait expired"));
        }, limits.queueWaitMs),
      };
      const timer = waiter.timer as { unref?: () => void };
      timer.unref?.();
      waiters.push(waiter);
    });
  };

  return {
    limits,
    oldestPendingAgeMs,
    inFlightRequests() {
      return requests;
    },
    observeAge,
    awaitIdle(boundMs: number): Promise<SettlementBacklog> {
      const snapshot = (): SettlementBacklog => ({ spawned: backlog.length, oldestAgeMs: oldestPendingAgeMs() });
      if (backlog.length === 0 || boundMs <= 0) {
        return Promise.resolve(snapshot());
      }
      return new Promise((resolve) => {
        let settled = false;
        const finish = () => {
          if (settled) {
            return;
          }
          settled = true;
          clearTimeout(timer);
          const index = idleWaiters.indexOf(waiter);
          if (index >= 0) {
            idleWaiters.splice(index, 1);
          }
          resolve(snapshot());
        };
        const waiter = () => finish();
        const timer = setTimeout(finish, boundMs);
        const unref = timer as { unref?: () => void };
        unref.unref?.();
        idleWaiters.push(waiter);
      });
    },
    async admit(kind: "buffered" | "streamed", metrics?: MetricSink): Promise<AdmissionHold> {
      const requestHeld = await acquireRequest(metrics);
      let streamHeld = false;
      let settlementState: "none" | "held" | "claimed" | "released" = "none";
      const releaseStream = () => {
        if (!streamHeld) {
          return;
        }
        streamHeld = false;
        streams -= 1;
        released(metrics, "stream");
      };
      try {
        if (kind === "streamed" && streamLimit !== null) {
          if (streams >= streamLimit) {
            rejected(metrics, "stream", "stream_capacity_exhausted");
            throw shed("stream_capacity_exhausted", "concurrent stream limit exceeded");
          }
          streams += 1;
          streamHeld = true;
          acquired(metrics, "stream");
        }
        if (settlementLimit !== null) {
          if (pending >= settlementLimit) {
            rejected(metrics, "settlement", "settlement_capacity_exhausted");
            throw shed("settlement_capacity_exhausted", "settlement capacity exhausted");
          }
          pending += 1;
          settlementState = "held";
          acquired(metrics, "settlement");
        }
      } catch (error) {
        releaseStream();
        if (requestHeld) {
          releaseRequest(metrics);
        }
        throw error;
      }
      let admissionReleased = false;
      let executionState: "idle" | "waiting" | "held" = "idle";
      let spawnedId: number | null = null;
      let reservedHeld = true;
      stage(metrics, "reserved", 1);
      const releaseReserved = (callMetrics?: MetricSink) => {
        if (!reservedHeld) {
          return;
        }
        reservedHeld = false;
        stage(callMetrics ?? metrics, "reserved", -1);
      };
      return {
        get settlementClaimed() {
          return settlementState === "claimed" || settlementState === "released";
        },
        claimSettlement() {
          if (settlementState === "held") {
            settlementState = "claimed";
          }
        },
        releaseAdmission() {
          if (admissionReleased) {
            return;
          }
          admissionReleased = true;
          releaseStream();
          if (requestHeld) {
            releaseRequest(metrics);
          }
        },
        releaseSettlement() {
          releaseReserved();
          if (settlementState !== "held" && settlementState !== "claimed") {
            return;
          }
          settlementState = "released";
          pending -= 1;
          released(metrics, "settlement");
        },
        settlementTimeoutMs: limits.settlementTimeoutMs,
        async acquireExecution(callMetrics) {
          if (executionState === "held") {
            return true;
          }
          if (executionState !== "idle") {
            return false;
          }
          executionState = "waiting";
          const granted = await acquireExecution(callMetrics ?? metrics);
          if (granted && executionLimit !== null) {
            executionState = "held";
            return true;
          }
          executionState = "idle";
          return granted;
        },
        releaseExecution(callMetrics) {
          if (executionState !== "held") {
            return;
          }
          executionState = "idle";
          releaseExecutionSlot(callMetrics ?? metrics);
        },
        beginSpawned(callMetrics) {
          if (spawnedId !== null) {
            return;
          }
          releaseReserved(callMetrics);
          spawnedId = enqueueSpawned();
          observeAge(callMetrics ?? metrics);
        },
        endSpawned(callMetrics) {
          if (spawnedId === null) {
            return;
          }
          const id = spawnedId;
          spawnedId = null;
          dequeueSpawned(id);
          observeAge(callMetrics ?? metrics);
        },
      };
    },
  };
}

export type AdmissionControl = ReturnType<typeof createAdmission>;

export function defaultAdmission(): AdmissionLimits {
  return {
    maxInFlight: DEFAULT_MAX_IN_FLIGHT,
    maxInFlightStreams: DEFAULT_MAX_IN_FLIGHT_STREAMS,
    streamsExplicit: false,
    queueCapacity: 0,
    queueWaitMs: 0,
    maxPendingSettlements: DEFAULT_MAX_IN_FLIGHT * 4,
    pendingExplicit: false,
    maxInFlightSettlements: 64,
    settlementQueueWaitMs: 10_000,
    settlementTimeoutMs: 10_000,
  };
}

/** Apply an omitted stream ceiling the way Rust clamps a defaulted one. */
export function clampStreams(maxInFlight: number, written: number | undefined): { value: number; explicit: boolean } {
  if (written !== undefined) {
    return { value: written, explicit: true };
  }
  if (maxInFlight > 0) {
    return { value: Math.min(DEFAULT_MAX_IN_FLIGHT_STREAMS, maxInFlight), explicit: false };
  }
  return { value: DEFAULT_MAX_IN_FLIGHT_STREAMS, explicit: false };
}

export function defaultPending(maxInFlight: number): number {
  const base = maxInFlight > 0 ? maxInFlight : DEFAULT_MAX_IN_FLIGHT;
  if (base > Math.floor(Number.MAX_SAFE_INTEGER / 4)) {
    return Number.MAX_SAFE_INTEGER;
  }
  return base * 4;
}

export function validateAdmission(limits: AdmissionLimits): void {
  for (const [field, value] of [
    ["admission.max_in_flight", limits.maxInFlight],
    ["admission.max_in_flight_streams", limits.maxInFlightStreams],
    ["admission.queue_capacity", limits.queueCapacity],
    ["admission.max_pending_settlements", limits.maxPendingSettlements],
    ["admission.max_in_flight_settlements", limits.maxInFlightSettlements],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < 0) {
      if (typeof value === "number" && value > Number.MAX_SAFE_INTEGER) {
        throw new Error(
          `${field} (${value}) must not exceed ${MAX_PERMITS}: a larger ceiling is not a bound this process can hold`,
        );
      }
      throw new Error(`${field} must be an integer of at least 0`);
    }
  }
  if (!Number.isSafeInteger(limits.queueWaitMs) || limits.queueWaitMs < 0) {
    throw new Error("admission.queue_wait_ms must be an integer of at least 0");
  }
  if (!Number.isSafeInteger(limits.settlementQueueWaitMs) || limits.settlementQueueWaitMs < 0) {
    throw new Error("admission.settlement_queue_wait_ms must be an integer of at least 0");
  }
  if (!Number.isSafeInteger(limits.settlementTimeoutMs) || limits.settlementTimeoutMs < 0) {
    throw new Error("admission.settlement_timeout_ms must be an integer of at least 0");
  }
  if (
    limits.maxInFlight > 0 &&
    limits.streamsExplicit &&
    limits.maxInFlightStreams > 0 &&
    limits.maxInFlightStreams > limits.maxInFlight
  ) {
    throw new Error(
      `admission.max_in_flight_streams (${limits.maxInFlightStreams}) must not exceed admission.max_in_flight (${limits.maxInFlight}): a stream is an in-flight request`,
    );
  }
  if ((limits.queueCapacity === 0) !== (limits.queueWaitMs === 0)) {
    throw new Error(
      "admission.queue_capacity and admission.queue_wait_ms must be set together: a queue without a wait bound is unbounded latency, and a wait without a queue is never used",
    );
  }
  if (limits.queueCapacity > 0 && limits.maxInFlight === 0) {
    throw new Error(
      "admission.queue_capacity requires admission.max_in_flight: nothing queues when the global ceiling is off",
    );
  }
  if (
    limits.maxInFlight > 0 &&
    limits.pendingExplicit &&
    limits.maxPendingSettlements > 0 &&
    limits.maxPendingSettlements < limits.maxInFlight
  ) {
    throw new Error(
      `admission.max_pending_settlements (${limits.maxPendingSettlements}) must be at least admission.max_in_flight (${limits.maxInFlight}): every admitted request reserves one settlement`,
    );
  }
}

export function admissionFromOptions(options: {
  maxInFlight?: number;
  maxInFlightStreams?: number;
  queueCapacity?: number;
  queueWaitMs?: number;
  maxPendingSettlements?: number;
  maxInFlightSettlements?: number;
  settlementQueueWaitMs?: number;
  settlementTimeoutMs?: number;
}): AdmissionLimits {
  const maxInFlight = options.maxInFlight ?? DEFAULT_MAX_IN_FLIGHT;
  const streams = clampStreams(maxInFlight, options.maxInFlightStreams);
  const queueCapacity = options.queueCapacity ?? 0;
  const queueWaitMs = options.queueWaitMs ?? 0;
  const pendingExplicit = options.maxPendingSettlements !== undefined;
  return {
    maxInFlight,
    maxInFlightStreams: streams.value,
    streamsExplicit: streams.explicit,
    queueCapacity,
    queueWaitMs,
    maxPendingSettlements: pendingExplicit ? options.maxPendingSettlements! : defaultPending(maxInFlight),
    pendingExplicit,
    maxInFlightSettlements: options.maxInFlightSettlements ?? 64,
    settlementQueueWaitMs: options.settlementQueueWaitMs ?? 10_000,
    settlementTimeoutMs: options.settlementTimeoutMs ?? 10_000,
  };
}

function shed(type: string, message: string): GatewayFailure {
  const error = new GatewayFailure(type, 503, message);
  error.retryAfter = "1";
  return error;
}
