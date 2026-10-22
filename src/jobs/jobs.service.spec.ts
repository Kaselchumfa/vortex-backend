import { ConfigService } from "@nestjs/config";
import { AppConfig } from "../config/configuration";
import { MetricsService } from "../metrics/metrics.service";
import { JobsService } from "./jobs.service";
import { defineJob, JobQueueDriver } from "./jobs.types";
import { MemoryJobQueueDriver } from "./memory-job-queue.driver";

function config(overrides: Partial<Pick<AppConfig, "processRole">> = {}, shutdownTimeoutMs = 1_000) {
  const values: Partial<AppConfig> = {
    processRole: "all",
    jobs: { driver: "memory", shutdownTimeoutMs },
    redisUrl: "redis://localhost:6379",
    ...overrides,
  };
  return { get: (key: keyof AppConfig) => values[key] } as unknown as ConfigService<AppConfig, true>;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function waitFor(check: () => boolean | Promise<boolean>, timeoutMs = 2_000) {
  const start = Date.now();
  while (!(await check())) {
    if (Date.now() - start > timeoutMs) throw new Error("condition not met in time");
    await sleep(5);
  }
}

const JOB = defineJob<{ n: number }>("test", "work", { attempts: 3, backoffMs: 10 });

describe("JobsService (memory driver)", () => {
  let service: JobsService;
  let metrics: MetricsService;

  beforeEach(() => {
    metrics = new MetricsService({ get: () => undefined } as unknown as ConfigService<AppConfig, true>);
    service = new JobsService(config(), metrics, new MemoryJobQueueDriver());
    service.defineQueue("test", { concurrency: 2 });
  });

  afterEach(() => service.onApplicationShutdown());

  it("retries with exponential backoff and succeeds", async () => {
    const attemptTimes: number[] = [];
    service.process(JOB, async (_data, ctx) => {
      attemptTimes.push(Date.now());
      if (ctx.attempt < 3) throw new Error("transient");
    });

    await service.enqueue(JOB, { n: 1 });
    await waitFor(() => attemptTimes.length === 3);

    // backoff = 10ms, then 20ms
    expect(attemptTimes[1] - attemptTimes[0]).toBeGreaterThanOrEqual(9);
    expect(attemptTimes[2] - attemptTimes[1]).toBeGreaterThanOrEqual(19);
    await waitFor(async () => (await service.stats())[0].delayed === 0);
    expect((await service.stats())[0].deadLetter).toBe(0);
    expect(await metrics.metrics()).toContain('vortex_jobs_failures_total{queue="test",job="work"} 2');
  });

  it("moves a job to the DLQ after exhausting attempts", async () => {
    const handler = jest.fn().mockRejectedValue(new Error("boom"));
    service.process(JOB, handler);

    const id = await service.enqueue(JOB, { n: 7 });
    await waitFor(async () => (await service.deadLetters("test")).length === 1);

    expect(handler).toHaveBeenCalledTimes(3);
    const [dead] = await service.deadLetters("test");
    expect(dead).toMatchObject({ jobId: id, name: "work", data: { n: 7 }, error: "boom", attempts: 3 });
    const text = await metrics.metrics();
    expect(text).toContain('vortex_jobs_dead_lettered_total{queue="test",job="work"} 1');
    expect(text).toContain('vortex_jobs_queue_depth{queue="test",state="dead_letter"} 1');
  });

  it("deduplicates by idempotency key", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    service.process(JOB, handler);

    const a = await service.enqueue(JOB, { n: 1 }, { idempotencyKey: "k1" });
    const b = await service.enqueue(JOB, { n: 2 }, { idempotencyKey: "k1" });
    await waitFor(() => handler.mock.calls.length >= 1);
    await sleep(20);

    expect(a).toBe(b);
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it("respects per-queue concurrency", async () => {
    let running = 0;
    let peak = 0;
    service.process(JOB, async () => {
      peak = Math.max(peak, ++running);
      await sleep(15);
      running--;
    });

    await Promise.all([1, 2, 3, 4, 5].map((n) => service.enqueue(JOB, { n })));
    await waitFor(async () => {
      const [s] = await service.stats();
      return s.waiting === 0 && s.active === 0;
    });
    expect(peak).toBe(2);
  });

  it("rate-limits job starts per queue", async () => {
    service.defineQueue("limited", { concurrency: 10, rateLimit: { max: 2, durationMs: 50 } });
    const LIMITED = defineJob<{ n: number }>("limited", "work");
    const starts: number[] = [];
    service.process(LIMITED, async () => {
      starts.push(Date.now());
    });

    await Promise.all([1, 2, 3].map((n) => service.enqueue(LIMITED, { n })));
    await waitFor(() => starts.length === 3);
    expect(starts[2] - starts[0]).toBeGreaterThanOrEqual(40);
  });

  it("does not consume jobs in the api role (producers only)", async () => {
    const api = new JobsService(config({ processRole: "api" }), undefined, new MemoryJobQueueDriver());
    api.defineQueue("test", { concurrency: 1 });
    const handler = jest.fn();
    api.process(JOB, handler);

    await api.enqueue(JOB, { n: 1 });
    await sleep(20);

    expect(handler).not.toHaveBeenCalled();
    expect((await api.stats())[0].waiting).toBe(1);
    await api.onApplicationShutdown();
  });
});

describe("JobsService dead-letter retry", () => {
  let service: JobsService;
  let driver: MemoryJobQueueDriver;

  beforeEach(() => {
    driver = new MemoryJobQueueDriver();
    service = new JobsService(config(), undefined, driver);
    service.defineQueue("test", { concurrency: 1 });
  });

  afterEach(() => service.onApplicationShutdown());

  it("re-enqueues a dead-lettered job with a fresh attempt budget", async () => {
    let fail = true;
    const attempts: number[] = [];
    service.process(JOB, async (_data, ctx) => {
      attempts.push(ctx.attempt);
      if (fail) throw new Error("boom");
    });

    const id = await service.enqueue(JOB, { n: 1 });
    await waitFor(async () => (await service.deadLetters("test")).length === 1);
    expect(attempts).toEqual([1, 2, 3]);

    fail = false;
    const retried = await service.retryDeadLetter("test", id);
    expect(retried).toBe(true);

    await waitFor(() => attempts.length === 4);
    // attempt counter reset: the retry starts again at attempt 1
    expect(attempts[3]).toBe(1);
    await waitFor(async () => (await service.deadLetters("test")).length === 0);
    expect(await service.retryDeadLetter("test", id)).toBe(false);
  });

  it("returns false for an unknown job id", async () => {
    expect(await service.retryDeadLetter("test", "does-not-exist")).toBe(false);
  });
});

describe("JobsService result lookup", () => {
  let service: JobsService;

  beforeEach(() => {
    service = new JobsService(config(), undefined, new MemoryJobQueueDriver());
    service.defineQueue("test", { concurrency: 1 });
  });

  afterEach(() => service.onApplicationShutdown());

  it("reports pending, completed and failed outcomes", async () => {
    const RESULT_JOB = defineJob<{ n: number }, { doubled: number }>("test", "double");
    const FAIL_JOB = defineJob<{ n: number }>("test", "fail");

    let release: () => void = () => undefined;
    const gate = new Promise<void>((r) => (release = r));
    service.process(RESULT_JOB, async (data) => {
      await gate;
      return { doubled: data.n * 2 };
    });
    service.process(FAIL_JOB, async () => {
      throw new Error("nope");
    });

    const okId = await service.enqueue(RESULT_JOB, { n: 21 });
    expect((await service.getResult("test", okId)).status).toBe("pending");
    release();
    await waitFor(async () => (await service.getResult("test", okId)).status === "completed");
    expect(await service.getResult("test", okId)).toMatchObject({ status: "completed", result: { doubled: 42 } });

    const failId = await service.enqueue(FAIL_JOB, { n: 1 });
    await waitFor(async () => (await service.getResult("test", failId)).status === "failed");
    expect(await service.getResult("test", failId)).toMatchObject({ status: "failed", error: "nope" });

    expect((await service.getResult("test", "unknown")).status).toBe("pending");
  });
});

describe("JobsService schedule management", () => {
  let service: JobsService;

  beforeEach(() => {
    service = new JobsService(config(), undefined, new MemoryJobQueueDriver());
    service.defineQueue("test", { concurrency: 1 });
  });

  afterEach(() => service.onApplicationShutdown());

  it("lists and cancels schedules", async () => {
    const handler = jest.fn();
    service.process(JOB, handler);
    await service.schedule(JOB, { everyMs: 10_000, data: { n: 1 } });

    expect(await service.listSchedules("test")).toEqual([{ name: "work", everyMs: 10_000 }]);
    expect(await service.unschedule("test", "work")).toBe(true);
    expect(await service.listSchedules("test")).toEqual([]);
    expect(await service.unschedule("test", "work")).toBe(false);
  });
});

describe("JobQueueDriver contract (memory driver)", () => {
  let driver: JobQueueDriver;

  beforeEach(() => {
    driver = new MemoryJobQueueDriver();
  });

  afterEach(async () => {
    await driver.close();
  });

  it("retries a dead letter and removes it from the DLQ", async () => {
    const JOB = defineJob<{ n: number }>("q", "work", { attempts: 1 });
    const handler = jest.fn().mockRejectedValue(new Error("boom"));
    driver.register("q", JOB, handler);
    await driver.start("q", { concurrency: 1 });

    const id = await driver.enqueue("q", JOB, { n: 1 });
    await waitFor(async () => (await driver.deadLetters("q")).length === 1);

    handler.mockResolvedValue(undefined);
    expect(await driver.retryDeadLetter("q", id)).toBe(true);
    await waitFor(() => handler.mock.calls.length === 2);
    await waitFor(async () => (await driver.deadLetters("q")).length === 0);
    expect(await driver.retryDeadLetter("q", id)).toBe(false);
  });

  it("reports not-found for an unknown dead letter", async () => {
    expect(await driver.retryDeadLetter("q", "missing")).toBe(false);
  });

  it("captures job results and reports status", async () => {
    const JOB = defineJob<{ n: number }, number>("q", "work");
    driver.register("q", JOB, async (data) => data.n + 1);
    await driver.start("q", { concurrency: 1 });

    const id = await driver.enqueue("q", JOB, { n: 1 });
    await waitFor(async () => (await driver.getResult("q", id)).status === "completed");
    expect(await driver.getResult("q", id)).toMatchObject({ status: "completed", result: 2 });
    expect((await driver.getResult("q", "missing")).status).toBe("pending");
  });

  it("lists and cancels schedules", async () => {
    const JOB = defineJob<{ n: number }>("q", "work");
    driver.register("q", JOB, jest.fn());
    await driver.schedule("q", JOB, { everyMs: 5_000, data: { n: 1 } });

    expect(await driver.listSchedules("q")).toEqual([{ name: "work", everyMs: 5_000 }]);
    expect(await driver.unschedule("q", "work")).toBe(true);
    expect(await driver.listSchedules("q")).toEqual([]);
    expect(await driver.unschedule("q", "work")).toBe(false);
  });
});

describe("JobsService graceful shutdown", () => {
  it("waits for in-flight jobs to finish", async () => {
    const service = new JobsService(config({}, 1_000), undefined, new MemoryJobQueueDriver());
    service.defineQueue("test", { concurrency: 1 });
    let finished = false;
    service.process(JOB, async () => {
      await sleep(30);
      finished = true;
    });

    await service.enqueue(JOB, { n: 1 });
    await waitFor(async () => (await service.stats())[0].active === 1);
    await service.onApplicationShutdown();

    expect(finished).toBe(true);
    expect((await service.stats())[0]).toMatchObject({ waiting: 0, active: 0 });
  });

  it("returns jobs that outlive the timeout to the queue and stops pulling new ones", async () => {
    const service = new JobsService(config({}, 20), undefined, new MemoryJobQueueDriver());
    service.defineQueue("test", { concurrency: 1 });
    const handler = jest.fn(async () => {
      await sleep(200);
    });
    service.process(JOB, handler);

    await service.enqueue(JOB, { n: 1 });
    await service.enqueue(JOB, { n: 2 });
    await waitFor(async () => (await service.stats())[0].active === 1);
    await service.onApplicationShutdown();

    expect((await service.stats())[0]).toMatchObject({ waiting: 2, active: 0 });
    expect(handler).toHaveBeenCalledTimes(1);
  });
});
