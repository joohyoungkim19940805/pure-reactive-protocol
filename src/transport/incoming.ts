import { deferred, type Deferred } from "../core/async-queue";
import { ProtocolViolationError } from "../core/errors";
import { DEFAULT_PROTOCOL_LIMITS } from "../core/limits";

const MAX_BUFFERED_FRAMES = 64;
const MAX_BUFFERED_BYTES = 64 * 1024 * 1024;

const bufferBytesFor = (maxFrameBytes: number): number =>
  Math.max(maxFrameBytes, 64 * 1024, Math.min(MAX_BUFFERED_BYTES, maxFrameBytes * 2));

export class IncomingFrameQueue implements AsyncIterable<Uint8Array> {
  private readonly values: Uint8Array[] = [];
  private readonly waiters: Deferred<IteratorResult<Uint8Array>>[] = [];
  private readonly spaceWaiters: Deferred<void>[] = [];
  private bufferedBytesValue = 0;
  private ended = false;
  private failure: unknown;

  constructor(
    private readonly maxFrameBytes = DEFAULT_PROTOCOL_LIMITS.maxFrameBytes,
    private readonly maxBufferedFrames = MAX_BUFFERED_FRAMES,
    private readonly maxBufferedBytes = bufferBytesFor(maxFrameBytes)
  ) {}

  get bufferedFrames(): number { return this.values.length; }
  get bufferedBytes(): number { return this.bufferedBytesValue; }

  pushOrThrow(value: Uint8Array): void {
    this.assertOpen();
    if (value.byteLength > this.maxFrameBytes) throw new ProtocolViolationError(`Transport frame exceeds ${this.maxFrameBytes} bytes.`);
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter.resolve({ value, done: false });
      return;
    }
    if (!this.hasSpace(value.byteLength)) throw new ProtocolViolationError("Transport incoming frame backlog exceeded its bounded queue.");
    this.values.push(value);
    this.bufferedBytesValue += value.byteLength;
  }

  /** Best-effort enqueue. Returns false when the bounded backlog is full without failing the queue. */
  tryPush(value: Uint8Array): boolean {
    this.assertOpen();
    if (value.byteLength > this.maxFrameBytes) throw new ProtocolViolationError(`Transport frame exceeds ${this.maxFrameBytes} bytes.`);
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter.resolve({ value, done: false });
      return true;
    }
    if (!this.hasSpace(value.byteLength)) return false;
    this.values.push(value);
    this.bufferedBytesValue += value.byteLength;
    return true;
  }

  async pushWait(value: Uint8Array): Promise<void> {
    this.assertOpen();
    if (value.byteLength > this.maxFrameBytes) throw new ProtocolViolationError(`Transport frame exceeds ${this.maxFrameBytes} bytes.`);
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter.resolve({ value, done: false });
      return;
    }
    while (!this.hasSpace(value.byteLength)) {
      const space = deferred<void>();
      this.spaceWaiters.push(space);
      await space.promise;
      this.assertOpen();
    }
    this.values.push(value);
    this.bufferedBytesValue += value.byteLength;
  }

  end(error?: unknown, discardValues = false): void {
    if (this.ended) return;
    this.ended = true;
    this.failure = error;
    if (discardValues) {
      this.values.length = 0;
      this.bufferedBytesValue = 0;
    }
    for (const waiter of this.waiters.splice(0)) {
      if (error === undefined) waiter.resolve({ value: undefined as never, done: true });
      else waiter.reject(error);
    }
    for (const waiter of this.spaceWaiters.splice(0)) {
      if (error === undefined) waiter.reject(new Error("Incoming frame queue is closed."));
      else waiter.reject(error);
    }
  }

  async next(): Promise<IteratorResult<Uint8Array>> {
    if (this.values.length > 0) {
      const value = this.values.shift()!;
      this.bufferedBytesValue -= value.byteLength;
      this.notifySpace();
      return { value, done: false };
    }
    if (this.ended) {
      if (this.failure !== undefined) throw this.failure;
      return { value: undefined as never, done: true };
    }
    const waiter = deferred<IteratorResult<Uint8Array>>();
    this.waiters.push(waiter);
    return waiter.promise;
  }

  [Symbol.asyncIterator](): AsyncIterator<Uint8Array> { return { next: () => this.next() }; }

  private hasSpace(bytes: number): boolean {
    return this.values.length < this.maxBufferedFrames && this.bufferedBytesValue + bytes <= this.maxBufferedBytes;
  }

  private notifySpace(): void {
    for (const waiter of this.spaceWaiters.splice(0)) waiter.resolve();
  }

  private assertOpen(): void {
    if (!this.ended) return;
    if (this.failure !== undefined) throw this.failure;
    throw new Error("Cannot push into a closed incoming frame queue.");
  }
}

export const incomingFrameBufferBytes = (maxFrameBytes = DEFAULT_PROTOCOL_LIMITS.maxFrameBytes): number => bufferBytesFor(maxFrameBytes);
