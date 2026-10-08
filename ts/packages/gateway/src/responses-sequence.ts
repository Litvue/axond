/**
 * Responses stream checks, matching `OpenAiStreamDecoder` on that surface.
 *
 * The relay still sends the provider's bytes. A broken event is reported
 * after those bytes, and a completed `response.completed` or a data-only
 * `[DONE]` ends the check.
 */

const INVALID = "provider stream was invalid: ";
const RATE_LIMITED = "provider stream was rate limited: ";

export interface ResponsesSequence {
  /** Observe one chunk. A string is the caller-facing stream error. */
  push(chunk: string): string | null;
  /** EOF before a terminal event. Whitespace left in the buffer is complete. */
  finish(): string | null;
}

export function createResponsesSequence(
  isRateLimit: (data: string) => boolean,
): ResponsesSequence {
  return new Sequence(isRateLimit);
}

class Sequence {
  private buffer = "";
  private terminal = false;
  private failed = false;
  private readonly isRateLimit: (data: string) => boolean;

  constructor(isRateLimit: (data: string) => boolean) {
    this.isRateLimit = isRateLimit;
  }

  push(chunk: string): string | null {
    for (let offset = 0; offset < chunk.length; offset += 64 * 1024) {
      const error = this.pushPart(chunk.slice(offset, offset + 64 * 1024));
      if (error !== null) return error;
      if (this.terminal || this.failed) break;
    }
    return null;
  }

  private pushPart(chunk: string): string | null {
    if (this.terminal || this.failed) {
      return null;
    }
    this.buffer += chunk;
    for (;;) {
      const end = eventEnd(this.buffer);
      const retained = end === null ? this.buffer : this.buffer.slice(0, end.index + end.delimiter);
      if (new TextEncoder().encode(retained).length > 1024 * 1024) {
        this.failed = true; this.buffer = "";
        return "SSE buffer exceeded 1048576 bytes";
      }
      if (end === null) return null;
      const block = this.buffer.slice(0, end.index).replaceAll("\r", "");
      this.buffer = this.buffer.slice(end.index + end.delimiter);
      const event = parseEvent(block);
      if (!event) {
        continue;
      }
      const error = this.observe(event.event, event.data);
      if (error !== null) {
        this.failed = true;
        return error;
      }
      if (this.terminal) {
        this.buffer = "";
        return null;
      }
    }
  }

  finish(): string | null {
    if (this.terminal || this.failed) {
      return null;
    }
    if (this.buffer.trim().length === 0) {
      return null;
    }
    this.failed = true;
    return "stream ended with an incomplete SSE event";
  }

  private observe(eventName: string | null, data: string): string | null {
    if (data.trim() === "[DONE]") {
      if (eventName !== null) {
        return invalid("Responses [DONE] sentinel must be a data-only SSE event");
      }
      this.terminal = true;
      return null;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(data);
    } catch (error) {
      const message = error instanceof Error ? error.message : "invalid JSON";
      return invalid(message);
    }
    if (this.isRateLimit(data)) {
      const message = pointer(parsed, "error/message");
      return rateLimited(typeof message === "string" ? message : "OpenAI stream rate limited");
    }
    const dataType = isObject(parsed) && typeof parsed["type"] === "string" ? parsed["type"] : undefined;
    if (dataType === undefined) {
      return invalid("Responses SSE event is missing data.type");
    }
    if (eventName !== null && eventName !== dataType) {
      return invalid("Responses SSE event name disagrees with data.type");
    }
    if (dataType === "response.completed") {
      const response = isObject(parsed) ? parsed["response"] : undefined;
      if (!isObject(response)) {
        return invalid("response.completed is missing its completed response object");
      }
      if (response["status"] !== "completed") {
        return invalid("response.completed is missing status=completed");
      }
      this.terminal = true;
    }
    return null;
  }
}

function invalid(message: string): string {
  return INVALID + boundDiagnostic(message);
}

function rateLimited(message: string): string {
  return RATE_LIMITED + boundDiagnostic(message);
}

/** Longest provider diagnostic kept, before the truncation marker. */
const MAX_DIAGNOSTIC_BYTES = 4096;

/** Appended when a diagnostic hit the byte bound, so a short message stays distinct. */
const DIAGNOSTIC_TRUNCATION_MARKER = "… [truncated]";

/** Cut on a UTF-8 boundary and mark the cut. A short diagnostic is unchanged. */
function boundDiagnostic(message: string): string {
  const bytes = new TextEncoder().encode(message);
  if (bytes.length <= MAX_DIAGNOSTIC_BYTES) {
    return message;
  }
  let cut = MAX_DIAGNOSTIC_BYTES;
  while (cut > 0 && (bytes[cut]! & 0xc0) === 0x80) {
    cut -= 1;
  }
  return new TextDecoder().decode(bytes.subarray(0, cut)) + DIAGNOSTIC_TRUNCATION_MARKER;
}

function eventEnd(buffer: string): { index: number; delimiter: number } | null {
  for (let index = 0; index < buffer.length - 1; index += 1) {
    if (buffer[index] === "\n" && buffer[index + 1] === "\n") {
      return { index, delimiter: 2 };
    }
    if (buffer[index] === "\r" && buffer.startsWith("\r\n\r\n", index)) {
      return { index, delimiter: 4 };
    }
  }
  return null;
}

function parseEvent(block: string): { event: string | null; data: string } | null {
  let event: string | null = null;
  const data: string[] = [];
  for (const line of block.split("\n")) {
    if (line.startsWith(":")) {
      continue;
    }
    if (line.startsWith("event:")) {
      const value = line.slice(6);
      event = value.startsWith(" ") ? value.slice(1) : value;
    } else if (line.startsWith("data:")) {
      const value = line.slice(5);
      data.push(value.startsWith(" ") ? value.slice(1) : value);
    }
  }
  if (data.length === 0) {
    return null;
  }
  return { event, data: data.join("\n") };
}

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function pointer(value: unknown, path: string): unknown {
  let current = value;
  for (const key of path.split("/")) {
    if (!isObject(current)) {
      return undefined;
    }
    current = current[key];
  }
  return current;
}
