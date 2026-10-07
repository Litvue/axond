/** One Server-Sent Event, including the original bytes when they are known. */
export interface SseEvent {
  /** The `event:` field, or null when the frame omitted it. */
  event: string | null;
  /** Concatenated `data:` lines, without the trailing event delimiter. */
  data: string;
  id: string | null;
  /**
   * The frame exactly as it arrived, delimiter included.
   * An identity transform emits this so chunk boundaries of the payload stay
   * intact when the whole frame fits in one chunk.
   */
  raw: Uint8Array;
}

export type SseTransform = (event: SseEvent) => SseEvent | null;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * Transform SSE frames without splitting an event across the caller's logic.
 *
 * Incomplete frames stay buffered until the delimiter (`\n\n` or `\r\n\r\n`).
 * Returning the same event object writes `raw` unchanged. Returning a new
 * object re-encodes the frame. Returning null drops it.
 */
export function transformSseEvents(
  stream: ReadableStream<Uint8Array>,
  transform: SseTransform,
): ReadableStream<Uint8Array> {
  const reader = stream.getReader();
  let pending = new Uint8Array(0);
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      for (;;) {
        const frame = takeFrame(pending);
        if (frame) {
          pending = frame.rest;
          const parsed = parseFrame(frame.raw);
          const next = transform(parsed);
          if (next === null) {
            continue;
          }
          controller.enqueue(next === parsed ? parsed.raw : encodeFrame(next));
          return;
        }
        const chunk = await reader.read();
        if (chunk.done) {
          if (pending.length > 0) {
            controller.enqueue(pending);
            pending = new Uint8Array(0);
          }
          controller.close();
          return;
        }
        pending = concat(pending, chunk.value);
      }
    },
    cancel(reason) {
      return reader.cancel(reason);
    },
  });
}

function takeFrame(buffer: Uint8Array): { raw: Uint8Array; rest: Uint8Array } | null {
  let lf = -1;
  let crlf = -1;
  for (let index = 0; index < buffer.length - 1; index += 1) {
    if (lf === -1 && buffer[index] === 10 && buffer[index + 1] === 10) {
      lf = index;
    }
    if (
      crlf === -1 &&
      index + 3 < buffer.length &&
      buffer[index] === 13 &&
      buffer[index + 1] === 10 &&
      buffer[index + 2] === 13 &&
      buffer[index + 3] === 10
    ) {
      crlf = index;
    }
    if (lf !== -1 && crlf !== -1) {
      break;
    }
  }
  let at = -1;
  let width = 0;
  if (lf !== -1 && (crlf === -1 || lf < crlf)) {
    at = lf;
    width = 2;
  } else if (crlf !== -1) {
    at = crlf;
    width = 4;
  }
  if (at === -1) {
    return null;
  }
  const end = at + width;
  return { raw: buffer.slice(0, end), rest: buffer.slice(end) };
}

function parseFrame(raw: Uint8Array): SseEvent {
  const text = decoder.decode(raw).replace(/\r\n/g, "\n").replace(/\n+$/, "");
  let event: string | null = null;
  let id: string | null = null;
  const data: string[] = [];
  for (const line of text.split("\n")) {
    if (line.startsWith("data:")) {
      data.push(line.slice(5).replace(/^ /, ""));
    } else if (line.startsWith("event:")) {
      event = line.slice(6).replace(/^ /, "");
    } else if (line.startsWith("id:")) {
      id = line.slice(3).replace(/^ /, "");
    }
  }
  return { event, data: data.join("\n"), id, raw };
}

function encodeFrame(event: SseEvent): Uint8Array {
  const lines: string[] = [];
  if (event.event !== null) {
    lines.push(`event: ${event.event}`);
  }
  if (event.id !== null) {
    lines.push(`id: ${event.id}`);
  }
  for (const line of event.data.split("\n")) {
    lines.push(`data: ${line}`);
  }
  lines.push("", "");
  return encoder.encode(lines.join("\n"));
}

function concat(left: Uint8Array, right: Uint8Array): Uint8Array {
  const out = new Uint8Array(left.length + right.length);
  out.set(left, 0);
  out.set(right, left.length);
  return out;
}
