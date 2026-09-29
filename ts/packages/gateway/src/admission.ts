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
}

export interface AdmissionHold {
  /** True once `settle` has taken responsibility for the settlement slot. */
  readonly settlementClaimed: boolean;
  claimSettlement(): void;
  releaseAdmission(): void;
  releaseSettlement(): void;
}

interface MetricSink {
  record(name: string, value: number, attributes?: Record<string, string>): void;
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
  let requests = 0;
  let streams = 0;
  let pending = 0;
  let queueDepth = 0;
  const waiters: Waiter[] = [];

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
          if (settlementState !== "held" && settlementState !== "claimed") {
            return;
          }
          settlementState = "released";
          pending -= 1;
          released(metrics, "settlement");
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
  };
}

function shed(type: string, message: string): GatewayFailure {
  const error = new GatewayFailure(type, 503, message);
  error.retryAfter = "1";
  return error;
}
