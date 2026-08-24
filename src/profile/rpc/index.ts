import { attribute, attributesById, bytes, strictText } from "../../core/attributes";
import { CapabilityMismatchError, PureReactiveProtocolError } from "../../core/errors";
import { getNegotiatedCapabilities, registerStreamAcceptor, type ReactiveSession, type ReactiveStream, type StreamMessage } from "../../core/session";
import type { ProtocolExtension } from "../../core/capabilities";
import { jsonCodec, type PayloadCodec } from "./codec";

export const RPC_PROFILE_ATTRIBUTE = "prp.profile";
export const RPC_TARGET_ATTRIBUTE = "prp.rpc.target";
export const RPC_PATTERN_ATTRIBUTE = "prp.rpc.pattern";
const PROFILE_ATTRIBUTE = RPC_PROFILE_ATTRIBUTE;
const TARGET_ATTRIBUTE = RPC_TARGET_ATTRIBUTE;
const PATTERN_ATTRIBUTE = RPC_PATTERN_ATTRIBUTE;
export const RPC_PROFILE_ID = "rpc/1";
export const RPC_CAPABILITY_ID = "prp.profile.rpc";
const RPC_OPEN_ATTRIBUTES = new Set([PROFILE_ATTRIBUTE, TARGET_ATTRIBUTE, PATTERN_ATTRIBUTE]);

type RpcPattern = "unary" | "notify" | "server-stream" | "duplex";
type Awaitable<T> = T | PromiseLike<T>;

export interface RpcContext {
  readonly session: ReactiveSession;
  readonly stream: ReactiveStream;
  readonly target: string;
  readonly signal: AbortSignal;
}

export interface RpcHandlers<I = unknown, O = unknown> {
  readonly requestResponse?: (input: I, context: RpcContext) => Awaitable<O>;
  readonly fireAndForget?: (input: I, context: RpcContext) => Awaitable<void>;
  readonly requestStream?: (input: I, context: RpcContext) => AsyncIterable<O>;
  readonly requestChannel?: (input: AsyncIterable<I>, context: RpcContext) => AsyncIterable<O>;
}

export interface RpcPeerOptions {
  /** Optional assertion. The codec implementation is installed by rpcProfile(); ids must match. */
  readonly codec?: PayloadCodec;
}

export interface RpcProfileOptions {
  readonly codec?: PayloadCodec;
}

export class RpcCapabilityError extends CapabilityMismatchError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "RpcCapabilityError";
  }
}

export interface RpcCallOptions {
  readonly signal?: AbortSignal;
}

interface RpcProfileState {
  readonly codec: PayloadCodec;
  readonly handlers: Map<string, RpcHandlers>;
  readonly disposeAcceptor: () => void;
}

const rpcStates = new WeakMap<ReactiveSession, RpcProfileState>();

const decodeCodecId = (value: Uint8Array | undefined): string => {
  if (!value || value.byteLength === 0) throw new RpcCapabilityError("RPC/1 capability is missing its codec id.");
  let id: string;
  try { id = strictText(value); }
  catch (cause) { throw new RpcCapabilityError("RPC/1 capability codec id is not valid UTF-8.", { cause }); }
  if (!id) throw new RpcCapabilityError("RPC/1 capability codec id must not be empty.");
  return id;
};

const assertCodec = (codec: PayloadCodec): void => {
  if (!codec.id) throw new TypeError("RPC codec id must not be empty.");
  bytes(codec.id);
  if (typeof codec.encode !== "function" || typeof codec.decode !== "function") throw new TypeError("RPC codec must implement encode() and decode().");
};

export const attachRpcProfile = (session: ReactiveSession, codec: PayloadCodec = jsonCodec): (() => void) => {
  assertCodec(codec);
  if (!session.supports(RPC_CAPABILITY_ID)) throw new RpcCapabilityError(`RPC/1 capability was not negotiated: ${RPC_CAPABILITY_ID}`);
  if (rpcStates.has(session)) throw new TypeError("RPC/1 profile is already attached to this session.");
  const negotiated = getNegotiatedCapabilities(session).get(RPC_CAPABILITY_ID);
  if (!negotiated) throw new RpcCapabilityError(`RPC/1 capability was not negotiated: ${RPC_CAPABILITY_ID}`);
  const remoteCodecId = decodeCodecId(negotiated.remote.parameters);
  if (remoteCodecId !== codec.id) {
    throw new RpcCapabilityError(`RPC/1 codec mismatch: local=${codec.id}, remote=${remoteCodecId}.`);
  }
  const handlers = new Map<string, RpcHandlers>();
  const state = {} as RpcProfileState;
  const disposeAcceptor = registerStreamAcceptor(session, {
    accepts: (stream) => {
      const profiles = attributesById(stream.attributes, PROFILE_ATTRIBUTE);
      if (profiles.length !== 1) return false;
      try { return strictText(profiles[0]!.value) === RPC_PROFILE_ID; }
      catch { return false; }
    },
    handle: (stream) => handleIncomingRpc(session, state, stream)
  });
  Object.assign(state, { codec, handlers, disposeAcceptor });
  rpcStates.set(session, state);
  return () => {
    if (rpcStates.get(session) !== state) return;
    rpcStates.delete(session);
    handlers.clear();
    disposeAcceptor();
  };
};

export const rpcProfile = (options: RpcProfileOptions = {}): ProtocolExtension => {
  const codec = options.codec ?? jsonCodec;
  assertCodec(codec);
  return Object.freeze({
    capability: Object.freeze({
      id: RPC_CAPABILITY_ID,
      minVersion: 1,
      maxVersion: 1,
      parameters: bytes(codec.id)
    }),
    attach: (session: ReactiveSession) => attachRpcProfile(session, codec)
  });
};

const abortError = (signal: AbortSignal): unknown => signal.reason ?? Object.assign(new Error("Aborted"), { name: "AbortError" });

const throwIfAborted = (signal?: AbortSignal): void => {
  if (signal?.aborted) throw abortError(signal);
};

const validateTarget = (target: string): void => {
  if (!target) throw new TypeError("RPC target must not be empty.");
  // attribute() validates Unicode well-formedness as part of the public protocol contract.
  attribute(TARGET_ATTRIBUTE, target);
};

const profileAttributes = (target: string, pattern: RpcPattern) => [
  attribute(PROFILE_ATTRIBUTE, RPC_PROFILE_ID, { required: true }),
  attribute(TARGET_ATTRIBUTE, target, { required: true }),
  attribute(PATTERN_ATTRIBUTE, pattern, { required: true })
] as const;

const singletonText = (stream: ReactiveStream, id: string): string | undefined => {
  const values = attributesById(stream.attributes, id);
  if (values.length > 1) throw new PureReactiveProtocolError(`RPC/1 OPEN contains duplicate ${id} attributes.`, "RPC_INVALID_OPEN");
  if (values.length === 0) return undefined;
  try { return strictText(values[0]!.value); }
  catch (cause) { throw new PureReactiveProtocolError(`RPC/1 OPEN attribute ${id} is not valid UTF-8.`, "RPC_INVALID_OPEN", { cause }); }
};

const decodeRpcItem = <T>(message: StreamMessage, codec: PayloadCodec): T => {
  const unsupported = message.attributes.find((item) => item.required);
  if (unsupported) {
    throw new PureReactiveProtocolError(
      `RPC/1 does not understand required DATA attribute ${unsupported.id}.`,
      "RPC_REQUIRED_ATTRIBUTE_UNSUPPORTED"
    );
  }
  return codec.decode(message.data) as T;
};

const autoDemand = async function* <T>(stream: ReactiveStream, codec: PayloadCodec): AsyncGenerator<T> {
  for await (const message of stream) yield decodeRpcItem<T>(message, codec);
};

const one = async <T>(stream: ReactiveStream, codec: PayloadCodec): Promise<T> => {
  const iterator = stream[Symbol.asyncIterator]();
  const next = await iterator.next();
  if (next.done) throw new PureReactiveProtocolError("RPC peer completed without the required input item.", "RPC_INPUT_REQUIRED");
  const value = decodeRpcItem<T>(next.value, codec);
  const terminal = await iterator.next();
  if (!terminal.done) {
    await iterator.return?.();
    throw new PureReactiveProtocolError("RPC peer produced more than one input item.", "RPC_TOO_MANY_INPUTS");
  }
  return value;
};

const watchAbort = (stream: ReactiveStream, signal?: AbortSignal): (() => void) => {
  if (!signal) return () => {};
  const aborted = () => { void stream.cancel(abortError(signal) instanceof Error ? abortError(signal) as Error : new Error(String(abortError(signal)))); };
  if (signal.aborted) aborted();
  else signal.addEventListener("abort", aborted, { once: true });
  return () => signal.removeEventListener("abort", aborted);
};

const handleIncomingRpc = async (session: ReactiveSession, state: RpcProfileState, stream: ReactiveStream): Promise<void> => {
    const unsupportedRequired = stream.attributes.find((item) => item.required && !RPC_OPEN_ATTRIBUTES.has(item.id));
    if (unsupportedRequired) {
      await stream.fail(new PureReactiveProtocolError(
        `RPC/1 does not understand required OPEN attribute ${unsupportedRequired.id}.`,
        "RPC_REQUIRED_ATTRIBUTE_UNSUPPORTED"
      ));
      return;
    }

    let profile: string | undefined;
    let target: string | undefined;
    let pattern: RpcPattern | undefined;
    try {
      profile = singletonText(stream, PROFILE_ATTRIBUTE);
      target = singletonText(stream, TARGET_ATTRIBUTE);
      pattern = singletonText(stream, PATTERN_ATTRIBUTE) as RpcPattern | undefined;
    } catch (error) {
      await stream.fail(error);
      return;
    }
    if (profile !== RPC_PROFILE_ID || !target || !pattern || !( ["unary", "notify", "server-stream", "duplex"] as const).includes(pattern)) {
      await stream.fail(new PureReactiveProtocolError("RPC stream has invalid profile, target, or pattern attributes.", "RPC_INVALID_OPEN"));
      return;
    }

    const handlers = state.handlers.get(target);
    if (!handlers) {
      await stream.fail(new PureReactiveProtocolError(`No RPC handler is registered for ${target}.`, "RPC_NOT_FOUND"));
      return;
    }

    const context: RpcContext = { session: session, stream, target, signal: stream.signal };
    switch (pattern) {
      case "unary": {
        if (!handlers.requestResponse) return stream.fail(new PureReactiveProtocolError(`RPC target ${target} does not support unary requests.`, "RPC_PATTERN_UNSUPPORTED"));
        const response = await handlers.requestResponse(await one(stream, state.codec), context);
        await stream.send(state.codec.encode(response));
        await stream.complete();
        return;
      }
      case "notify": {
        if (!handlers.fireAndForget) return stream.fail(new PureReactiveProtocolError(`RPC target ${target} does not support notifications.`, "RPC_PATTERN_UNSUPPORTED"));
        await handlers.fireAndForget(await one(stream, state.codec), context);
        await stream.complete();
        return;
      }
      case "server-stream": {
        if (!handlers.requestStream) return stream.fail(new PureReactiveProtocolError(`RPC target ${target} does not support server streams.`, "RPC_PATTERN_UNSUPPORTED"));
        for await (const value of handlers.requestStream(await one(stream, state.codec), context)) await stream.send(state.codec.encode(value));
        await stream.complete();
        return;
      }
      case "duplex": {
        if (!handlers.requestChannel) return stream.fail(new PureReactiveProtocolError(`RPC target ${target} does not support duplex streams.`, "RPC_PATTERN_UNSUPPORTED"));
        for await (const value of handlers.requestChannel(autoDemand(stream, state.codec), context)) await stream.send(state.codec.encode(value));
        await stream.complete();
        return;
      }
    }
  };

export class RpcPeer {
  private readonly codec: PayloadCodec;
  private readonly handlers: Map<string, RpcHandlers>;
  private readonly ownedTargets = new Set<string>();

  constructor(readonly session: ReactiveSession, options: RpcPeerOptions = {}) {
    if (!session.supports(RPC_CAPABILITY_ID)) {
      throw new RpcCapabilityError(`RPC/1 capability was not negotiated: ${RPC_CAPABILITY_ID}`);
    }
    const state = rpcStates.get(session);
    if (!state) throw new PureReactiveProtocolError("RPC/1 implementation is not attached to this session.", "RPC_PROFILE_NOT_ATTACHED");
    if (options.codec && options.codec.id !== state.codec.id) {
      throw new RpcCapabilityError(`RpcPeer codec assertion ${options.codec.id} does not match negotiated ${state.codec.id}.`);
    }
    this.codec = state.codec;
    this.handlers = state.handlers;
  }

  register<I = unknown, O = unknown>(target: string, handlers: RpcHandlers<I, O>): this {
    validateTarget(target);
    this.handlers.set(target, handlers as RpcHandlers);
    this.ownedTargets.add(target);
    return this;
  }

  unregister(target: string): void {
    if (!this.ownedTargets.delete(target)) return;
    this.handlers.delete(target);
  }

  dispose(): void {
    for (const target of this.ownedTargets) this.handlers.delete(target);
    this.ownedTargets.clear();
  }

  async requestResponse<I = unknown, O = unknown>(target: string, input: I, options: RpcCallOptions = {}): Promise<O> {
    validateTarget(target);
    throwIfAborted(options.signal);
    const stream = await this.session.open(profileAttributes(target, "unary"));
    const stopWatching = watchAbort(stream, options.signal);
    try {
      throwIfAborted(options.signal);
      await stream.send(this.codec.encode(input));
      await stream.complete();
      const iterator = stream[Symbol.asyncIterator]();
      const response = await iterator.next();
      if (response.done) throw new PureReactiveProtocolError("RPC response completed without a value.", "RPC_RESPONSE_REQUIRED");
      const value = decodeRpcItem<O>(response.value, this.codec);
      const terminal = await iterator.next();
      if (!terminal.done) {
        await iterator.return?.();
        throw new PureReactiveProtocolError("Unary RPC produced more than one response.", "RPC_TOO_MANY_RESPONSES");
      }
      return value;
    } finally {
      stopWatching();
      if (options.signal?.aborted && !stream.closed) await stream.cancel(abortError(options.signal) instanceof Error ? abortError(options.signal) as Error : new Error(String(abortError(options.signal))));
    }
  }

  async fireAndForget<I = unknown>(target: string, input: I, options: RpcCallOptions = {}): Promise<void> {
    validateTarget(target);
    throwIfAborted(options.signal);
    const stream = await this.session.open(profileAttributes(target, "notify"));
    const stopWatching = watchAbort(stream, options.signal);
    try {
      throwIfAborted(options.signal);
      await stream.send(this.codec.encode(input));
      await stream.complete();
    } finally {
      stopWatching();
      if (options.signal?.aborted && !stream.closed) await stream.cancel(abortError(options.signal) instanceof Error ? abortError(options.signal) as Error : new Error(String(abortError(options.signal))));
    }
  }

  requestStream<I = unknown, O = unknown>(target: string, input: I, options: RpcCallOptions = {}): AsyncIterable<O> {
    validateTarget(target);
    const session = this.session;
    const codec = this.codec;
    return {
      async *[Symbol.asyncIterator]() {
        throwIfAborted(options.signal);
        const stream = await session.open(profileAttributes(target, "server-stream"));
        const stopWatching = watchAbort(stream, options.signal);
        try {
          throwIfAborted(options.signal);
          await stream.send(codec.encode(input));
          await stream.complete();
          yield* autoDemand<O>(stream, codec);
        } finally {
          stopWatching();
          if (!stream.closed) {
            await stream.cancel(options.signal?.aborted
              ? (abortError(options.signal) instanceof Error ? abortError(options.signal) as Error : new Error(String(abortError(options.signal))))
              : "RPC response consumer stopped before completion.");
          }
        }
      }
    };
  }

  requestChannel<I = unknown, O = unknown>(target: string, input: AsyncIterable<I>, options: RpcCallOptions = {}): AsyncIterable<O> {
    validateTarget(target);
    const session = this.session;
    const codec = this.codec;
    return {
      async *[Symbol.asyncIterator]() {
        throwIfAborted(options.signal);
        const stream = await session.open(profileAttributes(target, "duplex"));
        const stopWatching = watchAbort(stream, options.signal);
        const inputIterator = input[Symbol.asyncIterator]();
        const stopInput = () => { void inputIterator.return?.(); };
        stream.signal.addEventListener("abort", stopInput, { once: true });
        let pumpDone = false;
        const pump = (async () => {
          try {
            while (true) {
              throwIfAborted(options.signal);
              const next = await inputIterator.next();
              if (next.done) break;
              await stream.send(codec.encode(next.value));
            }
            await stream.complete();
          } catch (error) {
            if (!stream.closed) await stream.fail(error);
            throw error;
          } finally {
            pumpDone = true;
          }
        })();
        void pump.catch(() => {});

        try {
          yield* autoDemand<O>(stream, codec);
          if (pumpDone) await pump;
          else if (!stream.closed) await stream.cancel("RPC duplex response completed before outbound input finished.");
        } finally {
          stopWatching();
          stream.signal.removeEventListener("abort", stopInput);
          void inputIterator.return?.();
          if (!stream.closed) {
            await stream.cancel(options.signal?.aborted
              ? (abortError(options.signal) instanceof Error ? abortError(options.signal) as Error : new Error(String(abortError(options.signal))))
              : "RPC duplex consumer stopped before completion.");
          }
          void pump.catch(() => {});
        }
      }
    };
  }


}

export { binaryCodec, jsonCodec } from "./codec";
export type { PayloadCodec } from "./codec";
