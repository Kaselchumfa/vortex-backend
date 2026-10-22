import { Logger } from "@nestjs/common";
import {
  DeadLetter,
  EnqueueOptions,
  JobDefinition,
  JobEvents,
  JobHandler,
  JobQueueDriver,
  JobResult,
  JobSchedule,
  QueueSettings,
  QueueStats,
} from "./jobs.types";

/** Mirrors BullMQ's COMPLETED_RETENTION_SECONDS so both drivers age out results alike. */
const COMPLETED_RETENTION_SECONDS = 86_400;

interface MemJob {
  id: string;
  def: JobDefinition;
  data: unknown;
  attempt: number;
  idempotencyKey?: string;
}

interface MemDeadLetter extends DeadLetter {
  /** Original definition, kept so a retry can re-run with the full attempts budget. */
  def: JobDefinition;
}

interface MemResult {
  status: "pending" | "active" | "completed" | "failed";
  result?: unknown;
  error?: string;
  expiresAt: number;
}

interface MemSchedule {
  timer: NodeJS.Timeout;
  everyMs: number;
}

interface MemQueue {
  settings: QueueSettings;
  waiting: MemJob[];
  delayed: Set<NodeJS.Timeout>;
  delayedCount: number;
  active: Map<string, MemJob>;
  inFlight: Set<Promise<void>>;
  handlers: Map<string, { handler: JobHandler<unknown>; events: JobEvents }>;
  dlq: MemDeadLetter[];
  /** Start timestamps inside the current rate-limit window. */
  rateWindow: number[];
  rateTimer?: NodeJS.Timeout;
}

/**
 * Single-process, non-durable driver for development and tests. Implements
 * the same retry / DLQ / idempotency / concurrency / rate-limit semantics as
 * the BullMQ driver so behaviour can be tested without Redis.
 */
export class MemoryJobQueueDriver implements JobQueueDriver {
  private readonly logger = new Logger(MemoryJobQueueDriver.name);
  private readonly queueMap = new Map<string, MemQueue>();
  private readonly idempotency = new Map<string, string>();
  private readonly schedules = new Map<string, MemSchedule>();
  private readonly results = new Map<string, MemResult>();
  private seq = 0;
  private closing = false;

  defineQueue(queue: string, settings: QueueSettings): void {
    if (this.queueMap.has(queue)) return;
    this.queueMap.set(queue, {
      settings,
      waiting: [],
      delayed: new Set(),
      delayedCount: 0,
      active: new Map(),
      inFlight: new Set(),
      handlers: new Map(),
      dlq: [],
      rateWindow: [],
    });
  }

  async enqueue<T>(def: JobDefinition<T>, data: T, opts: EnqueueOptions = {}): Promise<string> {
    const q = this.queue(def.queue);
    const dedupe = opts.idempotencyKey ? `${def.queue}:${opts.idempotencyKey}` : undefined;
    const existing = dedupe ? this.idempotency.get(dedupe) : undefined;
    if (existing) return existing;

    const id = opts.idempotencyKey ?? `${def.name}:${++this.seq}`;
    if (dedupe) this.idempotency.set(dedupe, id);
    const job: MemJob = { id, def, data, attempt: 1, idempotencyKey: opts.idempotencyKey };
    this.setResult(def.queue, id, { status: "pending" });
    if (opts.delayMs) this.delay(q, def.queue, job, opts.delayMs);
    else {
      q.waiting.push(job);
      this.pump(def.queue);
    }
    return id;
  }

  async schedule<T>(def: JobDefinition<T>, everyMs: number, data: T): Promise<void> {
    const key = `${def.queue}:${def.name}`;
    if (this.schedules.has(key)) return;
    const timer = setInterval(() => {
      // Nothing consumes this queue in this process — don't pile up jobs.
      if (!this.queue(def.queue).handlers.has(def.name) || this.closing) return;
      void this.enqueue(def, data);
    }, everyMs);
    timer.unref?.();
    this.schedules.set(key, { timer, everyMs });
  }

  async listSchedules(queue: string): Promise<JobSchedule[]> {
    const prefix = `${queue}:`;
    const out: JobSchedule[] = [];
    for (const [key, sched] of this.schedules) {
      if (!key.startsWith(prefix)) continue;
      out.push({ name: key.slice(prefix.length), everyMs: sched.everyMs });
    }
    return out;
  }

  async unschedule(queue: string, name: string): Promise<boolean> {
    const key = `${queue}:${name}`;
    const sched = this.schedules.get(key);
    if (!sched) return false;
    clearInterval(sched.timer);
    this.schedules.delete(key);
    return true;
  }

  process<T>(def: JobDefinition<T>, handler: JobHandler<T>, events: JobEvents): void {
    this.queue(def.queue).handlers.set(def.name, {
      handler: handler as JobHandler<unknown>,
      events,
    });
    this.pump(def.queue);
  }

  async stats(queue: string): Promise<QueueStats> {
    const q = this.queue(queue);
    return {
      queue,
      waiting: q.waiting.length,
      active: q.active.size,
      delayed: q.delayedCount,
      deadLetter: q.dlq.length,
    };
  }

  async deadLetters(queue: string, limit = 100): Promise<DeadLetter[]> {
    return this.queue(queue)
      .dlq.slice(-limit)
      .reverse()
      .map(({ def: _def, ...rest }) => rest);
  }

  async retryDeadLetter(queue: string, jobId: string): Promise<boolean> {
    const q = this.queue(queue);
    const idx = q.dlq.findIndex((d) => d.jobId === jobId);
    if (idx === -1) return false;
    const [entry] = q.dlq.splice(idx, 1);
    // Fresh attempt counter so the retry gets the full attempts budget again.
    const job: MemJob = { id: entry.jobId, def: entry.def, data: entry.data, attempt: 1 };
    this.setResult(queue, job.id, { status: "pending" });
    q.waiting.push(job);
    this.pump(queue);
    return true;
  }

  async getResult(queue: string, jobId: string): Promise<JobResult> {
    this.queue(queue);
    const entry = this.results.get(jobId);
    if (!entry || entry.expiresAt <= Date.now()) {
      if (entry) this.results.delete(jobId);
      return { status: "not_found" };
    }
    const { expiresAt: _expiresAt, ...rest } = entry;
    return rest;
  }

  async close(timeoutMs: number): Promise<void> {
    this.closing = true;
    for (const sched of this.schedules.values()) clearInterval(sched.timer);
    const inFlight = [...this.queueMap.values()].flatMap((q) => [...q.inFlight]);
    let timeout: NodeJS.Timeout | undefined;
    await Promise.race([
      Promise.allSettled(inFlight),
      new Promise((resolve) => (timeout = setTimeout(resolve, timeoutMs))),
    ]);
    clearTimeout(timeout);
    for (const [name, q] of this.queueMap) {
      for (const t of q.delayed) clearTimeout(t);
      if (q.rateTimer) clearTimeout(q.rateTimer);
      if (q.active.size > 0) {
        // Return unfinished jobs to the head of the queue.
        q.waiting.unshift(...q.active.values());
        this.logger.warn(`[jobs] returned ${q.active.size} in-flight job(s) on ${name} at shutdown`);
        q.active.clear();
      }
    }
  }

  queues(): string[] {
    return [...this.queueMap.keys()];
  }

  private setResult(queue: string, jobId: string, update: Omit<MemResult, "expiresAt">): void {
    this.evictExpiredResults();
    this.results.set(jobId, {
      ...update,
      expiresAt: Date.now() + COMPLETED_RETENTION_SECONDS * 1000,
    });
  }

  private evictExpiredResults(): void {
    const now = Date.now();
    for (const [id, entry] of this.results) {
      if (entry.expiresAt <= now) this.results.delete(id);
    }
  }

  private queue(name: string): MemQueue {
    const q = this.queueMap.get(name);
    if (!q) throw new Error(`Unknown job queue "${name}" — call defineQueue() first`);
    return q;
  }

  private delay(q: MemQueue, queue: string, job: MemJob, ms: number) {
    q.delayedCount++;
    const timer = setTimeout(() => {
      q.delayed.delete(timer);
      q.delayedCount--;
      q.waiting.push(job);
      this.pump(queue);
    }, ms);
    timer.unref?.();
    q.delayed.add(timer);
  }

  private pump(queue: string): void {
    const q = this.queue(queue);
    while (!this.closing && q.active.size < q.settings.concurrency) {
      const idx = q.waiting.findIndex((j) => q.handlers.has(j.def.name));
      if (idx === -1) return;
      if (!this.takeRateSlot(q, queue)) return;
      const [job] = q.waiting.splice(idx, 1);
      q.active.set(job.id, job);
      const run = this.run(q, queue, job).finally(() => {
        q.inFlight.delete(run);
        this.pump(queue);
      });
      q.inFlight.add(run);
    }
  }

  private takeRateSlot(q: MemQueue, queue: string): boolean {
    const limit = q.settings.rateLimit;
    if (!limit) return true;
    const now = Date.now();
    q.rateWindow = q.rateWindow.filter((t) => now - t < limit.durationMs);
    if (q.rateWindow.length < limit.max) {
      q.rateWindow.push(now);
      return true;
    }
    if (!q.rateTimer) {
      q.rateTimer = setTimeout(() => {
        q.rateTimer = undefined;
        this.pump(queue);
      }, limit.durationMs - (now - q.rateWindow[0]));
      q.rateTimer.unref?.();
    }
    return false;
  }

  private async run(q: MemQueue, queue: string, job: MemJob): Promise<void> {
    const { handler, events } = q.handlers.get(job.def.name)!;
    const started = Date.now();
    this.setResult(queue, job.id, { status: "active" });
    try {
      const result = await handler(job.data, { jobId: job.id, attempt: job.attempt });
      if (!q.active.delete(job.id)) return; // returned to the queue by close()
      this.setResult(queue, job.id, { status: "completed", result });
      events.onComplete(job.def, Date.now() - started);
    } catch (err) {
      if (!q.active.delete(job.id)) return;
      const willRetry = job.attempt < job.def.attempts;
      events.onFailure(job.def, Date.now() - started, willRetry);
      if (willRetry) {
        this.setResult(queue, job.id, { status: "pending" });
        const backoff = job.def.backoffMs * 2 ** (job.attempt - 1);
        this.delay(q, queue, { ...job, attempt: job.attempt + 1 }, backoff);
      } else {
        const error = err instanceof Error ? err.message : String(err);
        this.setResult(queue, job.id, { status: "failed", error });
        q.dlq.push({
          jobId: job.id,
          name: job.def.name,
          data: job.data,
          error,
          attempts: job.attempt,
          failedAt: new Date().toISOString(),
          def: job.def,
        });
      }
    }
  }
}
