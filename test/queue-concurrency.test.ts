import { describe, expect, it } from "vitest";
import { AsyncQueue } from "../src/core/async-queue";
import { IncomingFrameQueue } from "../src/transport/incoming";

const bytes = (...values: number[]) => Uint8Array.from(values);

describe("queue concurrency and closure", () => {
  it("AsyncQueue serves multiple pending next calls in FIFO order", async () => {
    const queue = new AsyncQueue<number>(), iterator = queue[Symbol.asyncIterator]();
    const pending = Array.from({ length: 8 }, () => iterator.next());
    for (let value = 0; value < pending.length; value++) queue.push(value);
    expect((await Promise.all(pending)).map(result => result.value)).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
    expect(queue.size).toBe(0);
    queue.end();
    await expect(iterator.next()).resolves.toEqual({ value: undefined, done: true });
    expect(() => queue.push(8)).toThrow("closed AsyncQueue");
  });

  it("AsyncQueue drains retained items before terminal errors and discards only when requested", async () => {
    const failure = new Error("queue failed"), retained = new AsyncQueue<number>();
    retained.push(1); retained.push(2); retained.end(failure);
    expect(retained.size).toBe(2);
    await expect(retained.next()).resolves.toEqual({ value: 1, done: false });
    await expect(retained.next()).resolves.toEqual({ value: 2, done: false });
    await expect(retained.next()).rejects.toBe(failure);
    const discarded = new AsyncQueue<number>();
    discarded.push(1); discarded.end(failure, true);
    expect(discarded.size).toBe(0);
    await expect(discarded.next()).rejects.toBe(failure);
  });

  it("AsyncQueue closes or rejects every pending consumer and keeps the first terminal result", async () => {
    for (const failure of [undefined, new Error("terminal")]) {
      const queue = new AsyncQueue<number>();
      const settled = Promise.allSettled([queue.next(), queue.next(), queue.next()]);
      queue.end(failure); queue.end(new Error("late terminal"));
      const results = await settled;
      if (failure) expect(results).toEqual(Array.from({ length: 3 }, () => ({ status: "rejected", reason: failure })));
      else expect(results).toEqual(Array.from({ length: 3 }, () => ({ status: "fulfilled", value: { value: undefined, done: true } })));
    }
  });

  it("IncomingFrameQueue hands a resumed producer to the oldest pending next call", async () => {
    const queue = new IncomingFrameQueue(8, 1, 8), iterator = queue[Symbol.asyncIterator]();
    const a = bytes(1), b = bytes(2), c = bytes(3);
    queue.pushOrThrow(a);
    const writingB = queue.pushWait(b); // Full queue: wait for capacity.
    const readingA = iterator.next(); // Frees capacity and schedules producer B.
    const readingB = iterator.next(); // Registers a consumer before B's continuation.
    try {
      await writingB;
      const bufferedBeforeThirdRead = queue.bufferedFrames;
      const readingC = iterator.next();
      queue.pushOrThrow(c); // Also makes an unfixed stale waiter settle, without a timeout.
      const received = await Promise.all([readingA, readingB, readingC]);
      expect(received.map(result => Array.from(result.value!))).toEqual([[1], [2], [3]]);
      expect(bufferedBeforeThirdRead, "A waiting consumer must receive B instead of leaving it buffered").toBe(0);
      expect(queue.bufferedFrames).toBe(0);
      expect(queue.bufferedBytes).toBe(0);
    } finally {
      queue.end(undefined, true);
      await Promise.allSettled([writingB, readingA, readingB]);
    }
  });

  it("IncomingFrameQueue supports the production-style sequential for-await consumer", async () => {
    const queue = new IncomingFrameQueue(8, 1, 8);
    queue.pushOrThrow(bytes(0));
    const producer = (async () => {
      try { for (let value = 1; value < 1000; value++) await queue.pushWait(bytes(value >> 8, value & 255)); }
      finally { queue.end(); }
    })();
    const received: number[] = [];
    for await (const value of queue) received.push(value.length === 1 ? value[0]! : value[0]! * 256 + value[1]!);
    await producer;
    expect(received).toEqual(Array.from({ length: 1000 }, (_, index) => index));
    expect(queue.bufferedFrames).toBe(0);
    expect(queue.bufferedBytes).toBe(0);
  });

  it("IncomingFrameQueue releases blocked producers on normal close and failure", async () => {
    for (const failure of [undefined, new Error("incoming failed")]) {
      const queue = new IncomingFrameQueue(8, 1, 8), a = bytes(1, 2);
      queue.pushOrThrow(a);
      const writing = queue.pushWait(bytes(3));
      const settled = Promise.allSettled([writing]);
      queue.end(failure, failure !== undefined);
      const result = (await settled)[0]!;
      expect(result.status).toBe("rejected");
      if (failure) {
        expect(result).toEqual({ status: "rejected", reason: failure });
        expect(queue.bufferedFrames).toBe(0); expect(queue.bufferedBytes).toBe(0);
        await expect(queue.next()).rejects.toBe(failure);
      } else {
        await expect(queue.next()).resolves.toEqual({ value: a, done: false });
        await expect(queue.next()).resolves.toEqual({ value: undefined, done: true });
      }
      expect(() => queue.tryPush(bytes(4))).toThrow();
    }
  });

  it("IncomingFrameQueue bounds both frame count and bytes while waiting for sufficient capacity", async () => {
    const queue = new IncomingFrameQueue(4, 2, 5);
    queue.pushOrThrow(bytes(1, 2, 3));
    expect(queue.tryPush(bytes(4, 5, 6))).toBe(false);
    expect(queue.tryPush(bytes(4, 5))).toBe(true);
    expect([queue.bufferedFrames, queue.bufferedBytes]).toEqual([2, 5]);
    expect(() => queue.pushOrThrow(bytes(9))).toThrow("backlog");
    expect(() => queue.tryPush(bytes(0, 0, 0, 0, 0))).toThrow("exceeds 4 bytes");
    let admitted = false;
    const writing = queue.pushWait(bytes(6, 7, 8, 9)).then(() => { admitted = true; });
    try {
      await expect(queue.next()).resolves.toEqual({ value: bytes(1, 2, 3), done: false });
      await Promise.resolve();
      expect(admitted).toBe(false); // One slot is free, but two buffered bytes still prevent a 4-byte frame.
      await expect(queue.next()).resolves.toEqual({ value: bytes(4, 5), done: false });
      await writing;
      expect([queue.bufferedFrames, queue.bufferedBytes]).toEqual([1, 4]);
      await expect(queue.next()).resolves.toEqual({ value: bytes(6, 7, 8, 9), done: false });
      expect([queue.bufferedFrames, queue.bufferedBytes]).toEqual([0, 0]);
    } finally { queue.end(undefined, true); await Promise.allSettled([writing]); }
  });

  it("IncomingFrameQueue retains every frame when another producer directly serves the waiting consumer", async () => {
    const queue = new IncomingFrameQueue(8, 1, 8), iterator = queue[Symbol.asyncIterator]();
    queue.pushOrThrow(bytes(1));
    const waitingProducer = queue.pushWait(bytes(2));
    const first = iterator.next(), second = iterator.next();
    // This producer runs synchronously before the blocked producer resumes.
    // Competing producer calls have no documented invocation-order guarantee;
    // verify conservation rather than imposing an additional fairness policy.
    expect(queue.tryPush(bytes(3))).toBe(true);
    try {
      await waitingProducer;
      const third = iterator.next();
      const results = await Promise.all([first, second, third]);
      expect(results.map(result => result.value![0]!).sort()).toEqual([1, 2, 3]);
      expect(queue.bufferedFrames).toBe(0); expect(queue.bufferedBytes).toBe(0);
    } finally { queue.end(undefined, true); await Promise.allSettled([waitingProducer, first, second]); }
  });
});
