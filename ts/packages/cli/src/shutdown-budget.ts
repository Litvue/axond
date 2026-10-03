/**
 * One post-serving deadline. Settlement may use at most half. The usage flush
 * and the telemetry drain share whatever is left, so a slow charge cannot
 * buy the sinks a second full timeout.
 */
export function settleShareMs(flushTimeoutMs: number): number {
  return Math.floor(flushTimeoutMs / 2);
}

export function remainingMs(deadlineMs: number, nowMs: number): number {
  return Math.max(0, deadlineMs - nowMs);
}

/** In-flight exporter posts. `drain` reports whether they finished inside the bound. */
export function createBackgroundDrain() {
  const tasks = new Set<Promise<void>>();
  return {
    track(task: Promise<void>) {
      const tracked = task.then(
        () => undefined,
        () => undefined,
      ).finally(() => {
        tasks.delete(tracked);
      });
      tasks.add(tracked);
    },
    async drain(timeoutMs: number): Promise<boolean> {
      if (tasks.size === 0) {
        return true;
      }
      const pending = [...tasks];
      let finished = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const work = Promise.all(pending).then(() => {
        finished = true;
        if (timer) {
          clearTimeout(timer);
        }
      });
      await Promise.race([
        work,
        new Promise<void>((resolve) => {
          timer = setTimeout(resolve, timeoutMs);
        }),
      ]);
      if (timer) {
        clearTimeout(timer);
      }
      return finished;
    },
  };
}
