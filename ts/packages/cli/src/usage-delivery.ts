import pg from "pg";

import type { UsageRecord } from "@axond/sdk";
import { usageBatchSize, type UsageSinkConfig } from "../../gateway/src/config.ts";
import type { createMetrics } from "../../gateway/src/metrics.ts";
import { postOtlp, resourceAttributes, usageLogPayload, type TelemetryTarget } from "../../gateway/src/otel.ts";
import { usageLine } from "../../gateway/src/usage.ts";

import { usageMigrationGap, usageSchemaDdl, USAGE_NOT_NULL_COST } from "./usage-sql.ts";

const { Client } = pg;

const I64_MAX = 9223372036854775807n;
const MAX_BIND_PARAMETERS = 65535;
const COLUMNS = [
  "schema_version",
  "request_id",
  "trace_id",
  "namespace",
  "period",
  "subject",
  "signer_kid",
  "model",
  "target_provider",
  "target_model",
  "credential_source",
  "credential_id",
  "status",
  "input_tokens",
  "cache_read_tokens",
  "cache_write_tokens",
  "output_tokens",
  "cost_microdollars",
  "catalog_version",
  "price_book",
  "price_book_checksum",
  "price_catalog",
  "latency_ms",
  "attempts",
  "started_at",
  "recorded_at",
] as const;

const MAX_ROWS_PER_STATEMENT = Math.floor(MAX_BIND_PARAMETERS / COLUMNS.length);

export interface ObservedUsage {
  record: UsageRecord;
  observedAt: Date;
}

export interface UsageDelivery {
  write(record: UsageRecord): void;
  /** True when every sink finished inside the shared bound. */
  flush(timeoutMs: number): Promise<boolean>;
}

type Metrics = ReturnType<typeof createMetrics>;

interface BufferedSink {
  write(record: UsageRecord, observedAt: Date): void;
  flush(timeoutMs: number): Promise<boolean>;
  readonly dropped: number;
}

/**
 * Open the configured sinks. An empty list is the stdout default. A Postgres
 * sink connects here, so a bad DSN or an unmigrated table refuses the boot.
 */
export async function openUsageDelivery(input: {
  sinks: readonly UsageSinkConfig[];
  env: Record<string, string | undefined>;
  telemetry: TelemetryTarget | null;
  metrics: Metrics;
  onLog: (record: unknown) => void;
  onBackground?: (task: Promise<void>) => void;
  fetchImpl?: typeof fetch;
  writeStdout?: (line: string) => void;
}): Promise<UsageDelivery> {
  const configured = input.sinks.length === 0 ? [stdoutSink()] : input.sinks;
  const writers: BufferedSink[] = [];
  for (const sink of configured) {
    const writeStdout = input.writeStdout ?? ((line: string) => {
      process.stdout.write(line);
    });
    if (sink.kind === "stdout") {
      writers.push(immediateWriter("stdout", input.metrics, input.onLog, (record) => {
        writeStdout(`${usageLine(record)}\n`);
      }));
      continue;
    }
    if (sink.kind === "otlp") {
      if (!input.telemetry) {
        throw new Error(
          "usage sink configuration failed: usage sink `otlp`: OTLP export is off; set OTEL_EXPORTER_OTLP_ENDPOINT or remove the sink",
        );
      }
      const telemetry = input.telemetry;
      const fetchImpl = input.fetchImpl ?? fetch;
      writers.push(immediateWriter("otlp", input.metrics, input.onLog, (record, observedAt) => {
        const body = usageLogPayload(record, resourceAttributes(telemetry.instanceId), observedAt.getTime());
        const task = postOtlp(telemetry.endpoint, "logs", body, fetchImpl).then(
          () => undefined,
          () => undefined,
        );
        input.onBackground?.(task);
      }));
      continue;
    }
    writers.push(await openPostgresSink(sink, input.env, input.metrics, input.onLog));
  }
  return {
    write(record) {
      const observedAt = new Date();
      for (const writer of writers) {
        writer.write(record, observedAt);
      }
    },
    async flush(timeoutMs) {
      const deadline = Date.now() + timeoutMs;
      let complete = true;
      for (const writer of writers) {
        const finished = await writer.flush(Math.max(0, deadline - Date.now()));
        complete = complete && finished;
      }
      return complete;
    },
  };
}

function stdoutSink(): UsageSinkConfig {
  return {
    kind: "stdout",
    dsnEnv: null,
    table: "axond_usage",
    createTable: false,
    bufferCapacity: 10_000,
    maxBatch: 500,
    maxBatchExplicit: false,
    flushIntervalMs: 1_000,
  };
}

function immediateWriter(
  name: "stdout" | "otlp",
  metrics: Metrics,
  onLog: (record: unknown) => void,
  emit: (record: UsageRecord, observedAt: Date) => void,
): BufferedSink {
  return {
    dropped: 0,
    write(record, observedAt) {
      emit(record, observedAt);
    },
    async flush() {
      metrics.record("axond.usage.flushes", 1, { "axond.usage_sink": name, "axond.flush_outcome": "flushed" });
      onLog({ msg: "usage_flush", sink: name, outcome: "flushed", records: 0 });
      return true;
    },
  };
}

async function openPostgresSink(
  sink: UsageSinkConfig,
  env: Record<string, string | undefined>,
  metrics: Metrics,
  onLog: (record: unknown) => void,
): Promise<BufferedSink> {
  const dsnEnv = sink.dsnEnv ?? "";
  const dsn = env[dsnEnv];
  if (dsn === undefined || dsn.trim().length === 0) {
    throw new Error(
      `usage sink configuration failed: usage sink \`postgres\`: \`${dsnEnv}\` is unset or empty in the environment`,
    );
  }
  const client = new Client({
    connectionString: dsn,
    connectionTimeoutMillis: 10_000,
    application_name: "axond",
  });
  try {
    await client.connect();
  } catch (error) {
    throw new Error(`usage sink configuration failed: postgres usage sink: ${redact(errorText(error), dsn)}`);
  }
  try {
    if (sink.createTable) {
      await client.query(usageSchemaDdl(sink.table));
    }
    const missing = await missingColumns(client, sink.table);
    const gap = usageMigrationGap(missing);
    if (gap) {
      throw new Error(`usage sink configuration failed: usage sink \`postgres\`: ${gap}`);
    }
    if (await costIsNotNull(client, sink.table)) {
      throw new Error(`usage sink configuration failed: usage sink \`postgres\`: ${USAGE_NOT_NULL_COST}`);
    }
  } catch (error) {
    await client.end().catch(() => undefined);
    throw error;
  }
  const holder = { client };
  return createBufferedUsageSink({
    capacity: sink.bufferCapacity,
    maxBatch: usageBatchSize(sink),
    flushIntervalMs: sink.flushIntervalMs,
    metrics,
    onLog,
    insert: (rows) => insertUsage(holder, sink.table, rows, dsn),
    close: () => holder.client.end().then(() => undefined),
  });
}

export function createBufferedUsageSink(input: {
  capacity: number;
  maxBatch: number;
  flushIntervalMs: number;
  metrics: Metrics;
  onLog: (record: unknown) => void;
  insert: (rows: readonly ObservedUsage[]) => Promise<void>;
  close?: () => Promise<void>;
}): BufferedSink & { dropped: number } {
  const queue: ObservedUsage[] = [];
  let dropped = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let chain: Promise<void> = Promise.resolve();
  let closed = false;

  const noteDrop = (
    reason: "buffer_full" | "shutdown" | "sink_error",
    count: number,
    log: "sampled" | "batch" | "silent",
  ) => {
    if (count <= 0) {
      return;
    }
    dropped += count;
    input.metrics.record("axond.usage.records_dropped", count, {
      "axond.usage_sink": "postgres",
      "axond.drop_reason": reason,
    });
    if (log === "batch") {
      input.onLog({ msg: "usage_dropped", sink: "postgres", reason, records: count });
      return;
    }
    if (log === "sampled" && (dropped === count || dropped % 1_000 < count)) {
      input.onLog({ msg: "usage_dropped", sink: "postgres", reason, dropped });
    }
  };

  const logFlush = (outcome: "flushed" | "failed" | "timeout", count: number) => {
    if (outcome === "timeout") {
      input.onLog({ msg: "usage_flush", sink: "postgres", outcome, abandoned: count });
      return;
    }
    input.onLog({ msg: "usage_flush", sink: "postgres", outcome, records: count });
  };

  const writeRows = (rows: readonly ObservedUsage[]) => {
    chain = chain.then(async () => {
      if (rows.length === 0) {
        return;
      }
      try {
        await input.insert(rows);
        input.metrics.record("axond.usage.records_written", rows.length, { "axond.usage_sink": "postgres" });
      } catch {
        noteDrop("sink_error", rows.length, "batch");
      }
    });
  };

  const take = () => {
    if (timer) {
      clearTimeout(timer);
      timer = undefined;
    }
    if (queue.length === 0) {
      return;
    }
    writeRows(queue.splice(0, input.maxBatch));
  };

  const sink: BufferedSink & { dropped: number } = {
    get dropped() {
      return dropped;
    },
    write(record, observedAt) {
      if (closed || queue.length >= input.capacity) {
        noteDrop(closed ? "shutdown" : "buffer_full", 1, "sampled");
        return;
      }
      queue.push({ record, observedAt });
      if (queue.length >= input.maxBatch) {
        take();
        return;
      }
      if (!timer) {
        timer = setTimeout(take, input.flushIntervalMs);
        const unref = timer as { unref?: () => void };
        unref.unref?.();
      }
    },
    async flush(timeoutMs) {
      closed = true;
      const pending = queue.splice(0);
      if (timer) {
        clearTimeout(timer);
        timer = undefined;
      }
      let settled = false;
      let failed = false;
      const work = chain.then(async () => {
        if (pending.length === 0) {
          settled = true;
          return;
        }
        try {
          await input.insert(pending);
          input.metrics.record("axond.usage.records_written", pending.length, { "axond.usage_sink": "postgres" });
        } catch {
          failed = true;
          noteDrop("sink_error", pending.length, "batch");
        }
        settled = true;
      });
      await Promise.race([work.then(() => undefined), delay(timeoutMs).then(() => undefined)]);
      if (!settled) {
        noteDrop("shutdown", pending.length, "silent");
        input.metrics.record("axond.usage.flushes", 1, {
          "axond.usage_sink": "postgres",
          "axond.flush_outcome": "timeout",
        });
        logFlush("timeout", pending.length);
        return false;
      }
      const outcome = failed ? "failed" : "flushed";
      input.metrics.record("axond.usage.flushes", 1, {
        "axond.usage_sink": "postgres",
        "axond.flush_outcome": outcome,
      });
      logFlush(outcome, pending.length);
      await input.close?.();
      return !failed;
    },
  };
  return sink;
}

async function insertUsage(
  holder: { client: pg.Client },
  table: string,
  rows: readonly ObservedUsage[],
  dsn: string,
): Promise<void> {
  let last: unknown;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const client = holder.client;
    try {
      await client.query("BEGIN");
      for (let index = 0; index < rows.length; index += MAX_ROWS_PER_STATEMENT) {
        const chunk = rows.slice(index, index + MAX_ROWS_PER_STATEMENT);
        await client.query(insertSql(table, chunk.length), bindRows(chunk));
      }
      await client.query("COMMIT");
      return;
    } catch (error) {
      last = error;
      await client.query("ROLLBACK").catch(() => undefined);
      await client.end().catch(() => undefined);
      const replacement = new Client({
        connectionString: dsn,
        connectionTimeoutMillis: 10_000,
        application_name: "axond",
      });
      holder.client = replacement;
      try {
        await replacement.connect();
      } catch (connectError) {
        last = connectError;
      }
    }
  }
  throw new Error(redact(errorText(last), dsn));
}

export function insertSql(table: string, rowCount: number): string {
  let sql = `INSERT INTO ${table} (${COLUMNS.join(", ")}) VALUES `;
  for (let row = 0; row < rowCount; row += 1) {
    if (row > 0) {
      sql += ", ";
    }
    sql += "(";
    for (let column = 0; column < COLUMNS.length; column += 1) {
      if (column > 0) {
        sql += ", ";
      }
      sql += `$${row * COLUMNS.length + column + 1}`;
    }
    sql += ")";
  }
  sql += " ON CONFLICT DO NOTHING";
  return sql;
}

function bindRows(rows: readonly ObservedUsage[]): unknown[] {
  const params: unknown[] = [];
  for (const observed of rows) {
    params.push(...rowValues(observed));
  }
  return params;
}

export function rowValues(observed: ObservedUsage): unknown[] {
  const record = observed.record;
  const recordedAt = observed.observedAt;
  const startedAt =
    record.latencyMs > recordedAt.getTime() ? recordedAt : new Date(recordedAt.getTime() - record.latencyMs);
  return [
    record.schemaVersion,
    record.requestId,
    record.traceId,
    record.namespace,
    record.period,
    record.subject,
    record.signerKid,
    record.model,
    record.targetProvider,
    record.targetModel,
    record.credentialSource,
    record.credentialId,
    record.status,
    clamped(record.inputTokens),
    clamped(record.cacheReadTokens),
    clamped(record.cacheWriteTokens),
    clamped(record.outputTokens),
    record.costMicrodollars === null ? null : clamped(record.costMicrodollars),
    clamped(BigInt(record.catalogVersion)),
    record.priceBook,
    record.priceBookChecksum,
    record.priceCatalog,
    clamped(BigInt(record.latencyMs)),
    clamped(BigInt(record.attempts)),
    startedAt,
    recordedAt,
  ];
}

function clamped(value: bigint): string {
  if (value > I64_MAX) {
    return I64_MAX.toString();
  }
  if (value < 0n) {
    return "0";
  }
  return value.toString();
}

async function missingColumns(client: pg.Client, table: string): Promise<string[]> {
  const result = await client.query(
    "SELECT a.attname FROM pg_attribute AS a WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped",
    [table],
  );
  const present = result.rows.map((row) => String(row.attname));
  if (present.length === 0) {
    return [];
  }
  return COLUMNS.filter((column) => !present.includes(column));
}

async function costIsNotNull(client: pg.Client, table: string): Promise<boolean> {
  const result = await client.query(
    "SELECT a.attnotnull FROM pg_attribute AS a WHERE a.attrelid = to_regclass($1) AND a.attname = 'cost_microdollars' AND a.attnum > 0 AND NOT a.attisdropped",
    [table],
  );
  const row = result.rows[0] as { attnotnull?: boolean } | undefined;
  return row?.attnotnull === true;
}

function redact(message: string, dsn: string): string {
  return dsn.length === 0 ? message : message.split(dsn).join("[REDACTED]");
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : "insert failed";
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
