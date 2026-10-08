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
   * intact when the whole frame fits in one chunk. If a completed CR delimiter
   * ends a chunk, its optional LF is forwarded separately for an identity
   * transform when it arrives.
   */
  raw: Uint8Array;
}

export type SseTransform = (event: SseEvent) => SseEvent | null;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * Transform SSE frames without splitting an event across the caller's logic.
 *
 * Incomplete frames stay buffered until the blank-line delimiter (LF, CRLF, CR, or mixed line endings).
 * One event is bounded to 1 MiB; exceeding that bound cancels the source.
 * Returning the same event object writes `raw` unchanged. Returning a new
 * object re-encodes the frame. Returning null drops it.
 */
export function transformSseEvents(
  stream: ReadableStream<Uint8Array>,
  transform: SseTransform,
): ReadableStream<Uint8Array> {
  const reader = stream.getReader();
  const maxFrameBytes = 1024 * 1024;
  let buffer = new Uint8Array(1024);
  let used = 0;
  let chunk = new Uint8Array(0);
  let at = 0;
  let lineLength = 0;
  let afterCr = false;
  let delimiterCr = false;
  let swallowDelimiterLf = false;
  let forwardDelimiterLf = false;
  let completedFrameBytes = 0;
  let ended = false;
  function append(byte: number): void {
    if (used >= maxFrameBytes) throw new Error("SSE event exceeds the byte limit");
    if (used === buffer.length) {
      const next = new Uint8Array(Math.min(maxFrameBytes, buffer.length * 2));
      next.set(buffer); buffer = next;
    }
    buffer[used++] = byte;
  }
  function emit(controller: ReadableStreamDefaultController<Uint8Array>): boolean {
    const parsed = parseFrame(buffer.slice(0, used));
    if (swallowDelimiterLf) completedFrameBytes = used;
    used = 0; lineLength = 0; afterCr = false; delimiterCr = false;
    const next = transform(parsed);
    if (swallowDelimiterLf) forwardDelimiterLf = next === parsed;
    if (next === null) return false;
    controller.enqueue(next === parsed ? parsed.raw : encodeFrame(next));
    return true;
  }
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        for (;;) {
          if (delimiterCr) {
            if (at < chunk.length) {
              if (chunk[at] === 10) { append(10); at++; }
            } else {
              // CR already completes the blank line. Do not wait for a live
              // provider to send the optional LF before delivering the event.
              swallowDelimiterLf = true;
            }
            if (emit(controller)) return;
            continue;
          }
          if (at === chunk.length && !ended) {
            const next = await reader.read();
            ended = next.done;
            chunk = next.value ?? new Uint8Array(0); at = 0;
            if (!ended && chunk.length === 0) continue;
          }
          if (swallowDelimiterLf) {
            swallowDelimiterLf = false;
            if (!ended && chunk[at] === 10) {
              if (completedFrameBytes >= maxFrameBytes) throw new Error("SSE event exceeds the byte limit");
              at++;
              if (forwardDelimiterLf) { controller.enqueue(new Uint8Array([10])); return; }
            }
          }
          if (at === chunk.length && !ended) continue;
          if (ended) {
            if (used > 0) controller.enqueue(buffer.slice(0, used));
            controller.close(); return;
          }
          const byte = chunk[at++]!;
          append(byte);
          if (afterCr && byte === 10) { afterCr = false; continue; }
          afterCr = false;
          if (byte === 10 || byte === 13) {
            const empty = lineLength === 0;
            lineLength = 0; afterCr = byte === 13;
            if (empty) {
              if (byte === 13) { delimiterCr = true; continue; }
              if (emit(controller)) return;
            }
          } else lineLength++;
        }
      } catch (error) {
        void reader.cancel(error).catch(() => undefined);
        controller.error(error);
      }
    },
    cancel(reason) { return reader.cancel(reason); },
  });
}

function parseFrame(raw: Uint8Array): SseEvent {
  const text = decoder.decode(raw).replace(/\r\n|\r/g, "\n").replace(/\n+$/, "");
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
