/**
 * Native Messages stream sequence, matching `NativeMessagesDecoder`.
 *
 * The relay still sends the provider's bytes. A broken sequence is reported
 * after those bytes, and a completed `message_stop` ends the check.
 */

const INVALID = "provider stream was invalid: ";
const RATE_LIMITED = "provider stream was rate limited: ";

export interface NativeMessagesSequence {
  /** Observe one chunk. A string is the caller-facing stream error. */
  push(chunk: string): string | null;
  /** EOF before `message_stop`. Whitespace left in the buffer is complete. */
  finish(): string | null;
}

export function createNativeMessagesSequence(
  isRateLimit: (data: string) => boolean,
): NativeMessagesSequence {
  return new Sequence(isRateLimit);
}

class Sequence {
  private buffer = "";
  private terminal = false;
  private failed = false;
  private readonly decoder: Decoder;

  constructor(isRateLimit: (data: string) => boolean) {
    this.decoder = new Decoder(isRateLimit);
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
      const error = this.decoder.observe(event.event, event.data);
      if (error !== null) {
        this.failed = true;
        return error;
      }
      if (this.decoder.done) {
        this.terminal = true;
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
}

class Decoder {
  done = false;
  private started = false;
  private messageDeltaSeen = false;
  private terminalDeltaSeen = false;
  private readonly seen = new Set<number>();
  private readonly open = new Map<number, { blockType: string; thinkingSignatureSeen: boolean }>();
  private readonly isRateLimit: (data: string) => boolean;

  constructor(isRateLimit: (data: string) => boolean) {
    this.isRateLimit = isRateLimit;
  }

  observe(eventName: string | null, data: string): string | null {
    if (this.done) {
      return invalid("native Messages event arrived after message_stop");
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(data);
    } catch (error) {
      const message = error instanceof Error ? error.message : "invalid JSON";
      return invalid(message);
    }
    const dataType = isObject(parsed) && typeof parsed["type"] === "string" ? parsed["type"] : undefined;
    if (eventName !== null && dataType !== undefined && eventName !== dataType) {
      return invalid("native Messages SSE event name disagrees with data.type");
    }
    const kind = eventName ?? dataType;
    if (this.isRateLimit(data)) {
      const message = pointer(parsed, "error/message");
      return rateLimited(typeof message === "string" ? message : "Anthropic stream rate limited");
    }
    if (kind === "error") {
      const message = pointer(parsed, "error/message");
      return invalid(typeof message === "string" ? message : "Anthropic stream error");
    }
    if (kind === "message_start") {
      const usage = pointer(parsed, "message/usage");
      if (
        dataType !== "message_start"
        || this.started
        || !isObject(pointer(parsed, "message"))
        || (usage !== undefined && !isObject(usage))
      ) {
        return invalid("invalid or duplicate native Messages message_start");
      }
      this.started = true;
      return null;
    }
    if (kind === "content_block_start") {
      const index = asIndex(isObject(parsed) ? parsed["index"] : undefined);
      if (index === null) {
        return invalid("native Messages content_block_start is missing index");
      }
      const blockType = pointer(parsed, "content_block/type");
      if (typeof blockType !== "string") {
        return invalid("native Messages content_block_start is missing block type");
      }
      if (
        dataType !== "content_block_start"
        || !this.started
        || this.messageDeltaSeen
        || !isObject(pointer(parsed, "content_block"))
        || !blockStartValid(blockType, parsed)
        || this.seen.has(index)
      ) {
        return invalid("native Messages content block started out of sequence");
      }
      this.seen.add(index);
      this.open.set(index, { blockType, thinkingSignatureSeen: false });
      return null;
    }
    if (kind === "content_block_delta") {
      const index = asIndex(isObject(parsed) ? parsed["index"] : undefined);
      if (index === null) {
        return invalid("native Messages content_block_delta is missing index");
      }
      const deltaType = pointer(parsed, "delta/type");
      if (typeof deltaType !== "string") {
        return invalid("native Messages content_block_delta is missing delta type");
      }
      const block = this.open.get(index);
      if (!block) {
        return invalid("native Messages content_block_delta targets an unopened block");
      }
      if (
        dataType !== "content_block_delta"
        || !this.started
        || this.messageDeltaSeen
        || !isObject(pointer(parsed, "delta"))
        || !deltaMatches(block.blockType, deltaType)
        || !deltaValid(deltaType, parsed)
        || (block.blockType === "thinking" && block.thinkingSignatureSeen)
      ) {
        return invalid("native Messages content block delta arrived out of sequence");
      }
      if (block.blockType === "thinking" && deltaType === "signature_delta") {
        block.thinkingSignatureSeen = true;
      }
      return null;
    }
    if (kind === "content_block_stop") {
      const index = asIndex(isObject(parsed) ? parsed["index"] : undefined);
      if (index === null) {
        return invalid("native Messages content_block_stop is missing index");
      }
      const block = this.open.get(index);
      this.open.delete(index);
      if (
        dataType !== "content_block_stop"
        || !this.started
        || this.messageDeltaSeen
        || block === undefined
        || (block.blockType === "thinking" && !block.thinkingSignatureSeen)
      ) {
        return invalid("native Messages content block stopped out of sequence");
      }
      return null;
    }
    if (kind === "message_delta") {
      const stop = pointer(parsed, "delta/stop_reason");
      const usage = isObject(parsed) ? parsed["usage"] : undefined;
      if (
        dataType !== "message_delta"
        || !this.started
        || this.terminalDeltaSeen
        || this.open.size > 0
        || !isObject(pointer(parsed, "delta"))
        || (usage !== undefined && !isObject(usage))
        || (stop !== null && typeof stop !== "string")
      ) {
        return invalid("native Messages message_delta arrived out of sequence");
      }
      this.messageDeltaSeen = true;
      if (typeof stop === "string") {
        this.terminalDeltaSeen = true;
      }
      return null;
    }
    if (kind === "message_stop" && dataType === "message_stop") {
      if (!this.started || !this.terminalDeltaSeen || this.open.size > 0) {
        return invalid("native Messages message_stop arrived before a complete message sequence");
      }
      this.done = true;
      return null;
    }
    if (kind === "ping" && eventName === "ping" && dataType === "ping") {
      return null;
    }
    if (kind !== undefined && eventName === kind && dataType === kind) {
      return null;
    }
    return invalid("unsupported or discriminator-less native Messages event");
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

function asIndex(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    return null;
  }
  return value;
}

function blockStartValid(blockType: string, data: unknown): boolean {
  if (blockType === "text") {
    return pointer(data, "content_block/text") === "";
  }
  if (blockType === "tool_use" || blockType === "server_tool_use") {
    return typeof pointer(data, "content_block/id") === "string"
      && typeof pointer(data, "content_block/name") === "string"
      && isObject(pointer(data, "content_block/input"));
  }
  if (blockType === "thinking") {
    return typeof pointer(data, "content_block/thinking") === "string"
      && typeof pointer(data, "content_block/signature") === "string";
  }
  return true;
}

function deltaMatches(blockType: string, deltaType: string): boolean {
  if (blockType === "text") {
    return deltaType === "text_delta" || deltaType === "citations_delta";
  }
  if (blockType === "tool_use" || blockType === "server_tool_use") {
    return deltaType === "input_json_delta";
  }
  if (blockType === "thinking") {
    return deltaType === "thinking_delta" || deltaType === "signature_delta";
  }
  if (blockType === "redacted_thinking" || blockType === "fallback") {
    return false;
  }
  return deltaType !== "text_delta";
}

function deltaValid(deltaType: string, data: unknown): boolean {
  if (deltaType === "text_delta") {
    return typeof pointer(data, "delta/text") === "string";
  }
  if (deltaType === "citations_delta") {
    return isObject(pointer(data, "delta/citation"));
  }
  if (deltaType === "input_json_delta") {
    return typeof pointer(data, "delta/partial_json") === "string";
  }
  if (deltaType === "thinking_delta") {
    return typeof pointer(data, "delta/thinking") === "string";
  }
  if (deltaType === "signature_delta") {
    const signature = pointer(data, "delta/signature");
    return typeof signature === "string" && signature.length > 0;
  }
  return true;
}
