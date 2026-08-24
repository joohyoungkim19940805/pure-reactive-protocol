export interface Deferred<T> {
  readonly promise: Promise<T>;
  resolve(value: T | PromiseLike<T>): void;
  reject(reason?: unknown): void;
}

export const deferred = <T>(): Deferred<T> => {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

export class AsyncQueue<T> implements AsyncIterable<T> {
  private readonly values: T[] = [];
  private readonly waiters: Deferred<IteratorResult<T>>[] = [];
  private ended = false;
  private failure: unknown;

  get size(): number {
    return this.values.length;
  }

  push(value: T): void {
    if (this.ended) throw new Error("Cannot push into a closed AsyncQueue.");
    const waiter = this.waiters.shift();
    if (waiter) waiter.resolve({ value, done: false });
    else this.values.push(value);
  }

  end(error?: unknown, discardValues = false): void {
    if (this.ended) return;
    this.ended = true;
    this.failure = error;
    if (discardValues) this.values.length = 0;
    for (const waiter of this.waiters.splice(0)) {
      if (error === undefined) waiter.resolve({ value: undefined as T, done: true });
      else waiter.reject(error);
    }
  }

  async next(): Promise<IteratorResult<T>> {
    if (this.values.length > 0) return { value: this.values.shift()!, done: false };
    if (this.ended) {
      if (this.failure !== undefined) throw this.failure;
      return { value: undefined as T, done: true };
    }
    const waiter = deferred<IteratorResult<T>>();
    this.waiters.push(waiter);
    return waiter.promise;
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return { next: () => this.next() };
  }
}
