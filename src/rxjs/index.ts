import { Observable, from, type ObservableInput, type Subscription } from "rxjs";
import { AsyncQueue } from "../core/async-queue";
import type { ReactiveSession } from "../core/session";
import { RpcPeer, type RpcCallOptions, type RpcContext, type RpcPeerOptions } from "../profile/rpc";

export interface ObservableBridgeOptions {
  readonly maxBufferedItems?: number;
  readonly signal?: AbortSignal;
}

const abortReason = (signal: AbortSignal): unknown => signal.reason ?? Object.assign(new Error("Aborted"), { name: "AbortError" });

const linkedAbort = (signal?: AbortSignal): { readonly controller: AbortController; dispose(): void } => {
  const controller = new AbortController();
  if (!signal) return { controller, dispose() {} };
  const abort = () => { if (!controller.signal.aborted) controller.abort(abortReason(signal)); };
  if (signal.aborted) abort();
  else signal.addEventListener("abort", abort, { once: true });
  return { controller, dispose: () => signal.removeEventListener("abort", abort) };
};

export const toObservable = <T>(input: AsyncIterable<T>): Observable<T> => from(input);

export const toAsyncIterable = <T>(
  input: ObservableInput<T>,
  options: ObservableBridgeOptions = {}
): AsyncIterable<T> => ({
  [Symbol.asyncIterator](): AsyncIterator<T> {
    const maxBufferedItems = options.maxBufferedItems ?? 64;
    if (!Number.isInteger(maxBufferedItems) || maxBufferedItems <= 0) throw new RangeError("maxBufferedItems must be a positive integer.");
    const queue = new AsyncQueue<T>();
    let stopped = false;
    let subscription: Subscription | undefined;

    const stop = (error?: unknown): void => {
      if (stopped) return;
      stopped = true;
      options.signal?.removeEventListener("abort", aborted);
      if (error === undefined) queue.end();
      else queue.end(error);
      subscription?.unsubscribe();
    };
    const aborted = () => stop(options.signal ? abortReason(options.signal) : undefined);

    if (options.signal?.aborted) stop(abortReason(options.signal));
    else options.signal?.addEventListener("abort", aborted, { once: true });

    if (!stopped) {
      subscription = from(input).subscribe({
        next(value: T) {
          if (stopped) return;
          if (queue.size >= maxBufferedItems) {
            stop(Object.assign(
              new Error(`RxJS bridge exceeded maxBufferedItems=${maxBufferedItems}.`),
              { code: "BACKPRESSURE_OVERFLOW" }
            ));
            return;
          }
          queue.push(value);
        },
        error: stop,
        complete: () => stop()
      });
      if (stopped) subscription?.unsubscribe();
    }

    return {
      next: () => queue.next(),
      return: async () => {
        stop();
        return { value: undefined as T, done: true };
      },
      throw: async (error) => {
        stop(error);
        throw error;
      }
    };
  }
});

export interface RxRpcHandlers<I = unknown, O = unknown> {
  readonly requestResponse?: (input: I, context: RpcContext) => ObservableInput<O>;
  readonly fireAndForget?: (input: I, context: RpcContext) => void | Promise<void>;
  readonly requestStream?: (input: I, context: RpcContext) => ObservableInput<O>;
  readonly requestChannel?: (input: Observable<I>, context: RpcContext) => ObservableInput<O>;
}

export interface RxRpcCallOptions extends RpcCallOptions, ObservableBridgeOptions {}

export class RxRpcPeer {
  readonly rpc: RpcPeer;

  constructor(session: ReactiveSession, options?: RpcPeerOptions) {
    this.rpc = new RpcPeer(session, options);
  }

  register<I = unknown, O = unknown>(target: string, handlers: RxRpcHandlers<I, O>): this {
    this.rpc.register<I, O>(target, {
      ...(handlers.requestResponse === undefined ? {} : {
        requestResponse: async (input, context) => {
          const iterator = toAsyncIterable(handlers.requestResponse!(input, context), { signal: context.signal })[Symbol.asyncIterator]();
          try {
            const first = await iterator.next();
            if (first.done) throw new Error("RxJS unary RPC handler completed without a value.");
            return first.value;
          } finally {
            await iterator.return?.();
          }
        }
      }),
      ...(handlers.fireAndForget === undefined ? {} : { fireAndForget: handlers.fireAndForget }),
      ...(handlers.requestStream === undefined ? {} : {
        requestStream: (input, context) => toAsyncIterable(handlers.requestStream!(input, context), { signal: context.signal })
      }),
      ...(handlers.requestChannel === undefined ? {} : {
        requestChannel: (input, context) => toAsyncIterable(
          handlers.requestChannel!(toObservable(input), context),
          { signal: context.signal }
        )
      })
    });
    return this;
  }

  unregister(target: string): void {
    this.rpc.unregister(target);
  }

  requestResponse<I = unknown, O = unknown>(target: string, input: I, options: RpcCallOptions = {}): Observable<O> {
    return new Observable<O>((subscriber) => {
      const linked = linkedAbort(options.signal);
      void this.rpc.requestResponse<I, O>(target, input, { signal: linked.controller.signal }).then(
        (value) => { if (!subscriber.closed) { subscriber.next(value); subscriber.complete(); } },
        (error) => { if (!subscriber.closed) subscriber.error(error); }
      ).finally(linked.dispose);
      return () => {
        if (!linked.controller.signal.aborted) linked.controller.abort(Object.assign(new Error("RxJS subscription cancelled."), { name: "AbortError" }));
        linked.dispose();
      };
    });
  }

  fireAndForget<I = unknown>(target: string, input: I, options: RpcCallOptions = {}): Observable<void> {
    return new Observable<void>((subscriber) => {
      const linked = linkedAbort(options.signal);
      void this.rpc.fireAndForget(target, input, { signal: linked.controller.signal }).then(
        () => { if (!subscriber.closed) { subscriber.next(); subscriber.complete(); } },
        (error) => { if (!subscriber.closed) subscriber.error(error); }
      ).finally(linked.dispose);
      return () => {
        if (!linked.controller.signal.aborted) linked.controller.abort(Object.assign(new Error("RxJS subscription cancelled."), { name: "AbortError" }));
        linked.dispose();
      };
    });
  }

  requestStream<I = unknown, O = unknown>(target: string, input: I, options: RpcCallOptions = {}): Observable<O> {
    return new Observable<O>((subscriber) => {
      const linked = linkedAbort(options.signal);
      const subscription = from(this.rpc.requestStream<I, O>(target, input, { signal: linked.controller.signal })).subscribe(subscriber);
      return () => {
        subscription.unsubscribe();
        if (!linked.controller.signal.aborted) linked.controller.abort(Object.assign(new Error("RxJS subscription cancelled."), { name: "AbortError" }));
        linked.dispose();
      };
    });
  }

  requestChannel<I = unknown, O = unknown>(target: string, input: ObservableInput<I>, options: RxRpcCallOptions = {}): Observable<O> {
    return new Observable<O>((subscriber) => {
      const linked = linkedAbort(options.signal);
      const subscription = from(this.rpc.requestChannel<I, O>(
        target,
        toAsyncIterable(input, {
          ...(options.maxBufferedItems === undefined ? {} : { maxBufferedItems: options.maxBufferedItems }),
          signal: linked.controller.signal
        }),
        { signal: linked.controller.signal }
      )).subscribe(subscriber);
      return () => {
        subscription.unsubscribe();
        if (!linked.controller.signal.aborted) linked.controller.abort(Object.assign(new Error("RxJS subscription cancelled."), { name: "AbortError" }));
        linked.dispose();
      };
    });
  }

  dispose(): void {
    this.rpc.dispose();
  }
}
