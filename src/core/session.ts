import { AsyncQueue, deferred, type Deferred } from "./async-queue";
import { attribute, attributeTextStrict, attributesById, firstAttribute, strictText, type ProtocolAttribute } from "./attributes";
import {
  CapabilitySet,
  CORE_LIMITS_CAPABILITY_ID,
  CORE_LIVENESS_CAPABILITY_ID,
  capabilitiesFromAttributes,
  capabilitiesToAttributes,
  negotiateCapabilities
} from "./capabilities";
import { decodeFrame, encodeFrame, measureAttributeBytes } from "./codec";
import {
  CapabilityMismatchError,
  ConnectionLostError,
  LivenessTimeoutError,
  ProtocolViolationError,
  PureReactiveProtocolError,
  StreamClosedError
} from "./errors";
import { DATA_FRAGMENTED_FLAG, FRAME_HEADER_BYTES, FrameKind, type ProtocolFrame } from "./frame";
import { BOOTSTRAP_PROTOCOL_LIMITS, decodePeerProtocolLimits, type ProtocolLimits } from "./limits";
import { decodeLivenessParameters, decodeProbe, encodeProbe } from "./liveness";
import { ProtocolRuntime } from "./runtime";
import { RELIABLE_ORDERED_LANE, type ReactiveTransport, type TransportConnection, type TransportLane } from "../transport/types";

const SESSION_ID = "prp.session.id";
const ERROR_CODE = "prp.error.code";
const FRAGMENTATION_CAPABILITY_ID = "prp.core.fragmentation";
const MAX_U64 = 0xffffffffffffffffn;
const MAX_ERROR_REASON_BYTES = 1024;
const encoder = new TextEncoder();

export interface StreamMessage {
  readonly data: Uint8Array;
  readonly attributes: readonly ProtocolAttribute[];
}

export interface SessionSignal {
  readonly data: Uint8Array;
  readonly attributes: readonly ProtocolAttribute[];
}

export type SessionState = "idle" | "connecting" | "ready" | "detached" | "closed";
export type SessionOrigin = "initiator" | "acceptor";

export interface ReactiveStream extends AsyncIterable<StreamMessage> {
  readonly id: bigint;
  readonly attributes: readonly ProtocolAttribute[];
  readonly closed: boolean;
  readonly signal: AbortSignal;
  request(count?: number): Promise<void>;
  send(data?: Uint8Array, attributes?: readonly ProtocolAttribute[]): Promise<void>;
  complete(): Promise<void>;
  cancel(reason?: string | Error): Promise<void>;
  fail(error: unknown): Promise<void>;
}

export interface ReactiveSession extends AsyncIterable<ReactiveStream> {
  readonly state: SessionState;
  readonly sessionId: string;
  readonly signals: AsyncIterable<SessionSignal>;
  supports(capabilityId: string): boolean;
  onStateChange(listener: (state: SessionState) => void): () => void;
  open(attributes?: readonly ProtocolAttribute[]): Promise<ReactiveStream>;
  signal(attributes?: readonly ProtocolAttribute[], payload?: Uint8Array): Promise<void>;
  close(reason?: string): Promise<void>;
}

export interface StreamAcceptor {
  accepts(stream: ReactiveStream): boolean;
  handle(stream: ReactiveStream): void | Promise<void>;
}

export interface SignalAcceptor {
  accepts(signal: SessionSignal): boolean;
  handle(signal: SessionSignal): void | Promise<void>;
}

interface ReassemblyState {
  readonly data: Uint8Array;
  readonly attributes: readonly ProtocolAttribute[];
  offset: number;
}

interface StreamState {
  readonly id: bigint;
  readonly attributes: readonly ProtocolAttribute[];
  readonly incoming: AsyncQueue<StreamMessage>;
  readonly abortController: AbortController;
  readonly retainedAttributeBytes: number;
  readonly initiatedLocally: boolean;
  reassembly?: ReassemblyState;
  inboundCredit: number;
  outboundCredit: number;
  outboundClosed: boolean;
  inboundClosed: boolean;
  cancelled: boolean;
  writeTail: Promise<void>;
  readonly creditWaiters: Deferred<void>[];
}

interface StreamHost {
  state(): SessionState;
  write(frame: Omit<ProtocolFrame, "sequence">): Promise<void>;
  writeData(streamId: bigint, data: Uint8Array, attributes: readonly ProtocolAttribute[]): Promise<void>;
  release(state: StreamState): void;
  cancel(state: StreamState, error: unknown, allowLateData: boolean): void;
}

interface AttachOptions {
  readonly origin: SessionOrigin;
  readonly signal?: AbortSignal;
}

interface QueuedSignal {
  readonly signal: SessionSignal;
  readonly bytes: number;
}

export const SESSION_INTEGRATION = Symbol.for("@byeolnaerim/pure-reactive-protocol/session-integration");

export interface SessionIntegration {
  registerAcceptor(acceptor: StreamAcceptor): () => void;
  registerSignalAcceptor(acceptor: SignalAcceptor): () => void;
  capabilities(): CapabilitySet;
}

const randomId = (): string => {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") return crypto.randomUUID();
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
};

const addCredit = (current: number, amount: number): number => Math.min(0xffffffff, current + amount);

const snapshotAttributes = (attributes: readonly ProtocolAttribute[]): readonly ProtocolAttribute[] =>
  attributes.map((item) => ({ id: item.id, value: item.value.slice(), ...(item.required === undefined ? {} : { required: item.required }) }));

const attributeMemoryBytes = (attributes: readonly ProtocolAttribute[]): number =>
  attributes.reduce((total, item) => total + encoder.encode(item.id).byteLength + item.value.byteLength + 7, 0);

const boundedErrorPayload = (message: string): Uint8Array => {
  const output = new Uint8Array(MAX_ERROR_REASON_BYTES);
  const { written } = encoder.encodeInto(message, output);
  return output.slice(0, written);
};

const abortError = (signal: AbortSignal): unknown => signal.reason ?? Object.assign(new Error("Aborted"), { name: "AbortError" });

const waitWithAbort = async <T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> => {
  if (!signal) return promise;
  if (signal.aborted) throw abortError(signal);
  let rejectAbort!: (reason?: unknown) => void;
  const aborted = new Promise<never>((_resolve, reject) => { rejectAbort = reject; });
  const listener = () => rejectAbort(abortError(signal));
  signal.addEventListener("abort", listener, { once: true });
  try { return await Promise.race([promise, aborted]); }
  finally { signal.removeEventListener("abort", listener); }
};

const remoteDiagnostic = (frame: ProtocolFrame, fallbackMessage: string, fallbackCode: string): PureReactiveProtocolError => {
  const attributes = frame.attributes ?? [];
  const codeAttributes = attributesById(attributes, ERROR_CODE);
  if (codeAttributes.length > 1) throw new ProtocolViolationError(`ERROR contains duplicate ${ERROR_CODE} attributes.`);
  for (const item of attributes) {
    if (item.required && item.id !== ERROR_CODE) throw new ProtocolViolationError(`Unknown required ERROR attribute: ${item.id}`);
  }
  let code = fallbackCode;
  if (codeAttributes.length === 1) {
    try { code = attributeTextStrict(codeAttributes, ERROR_CODE) ?? fallbackCode; }
    catch (cause) { throw new ProtocolViolationError(`${ERROR_CODE} is not valid UTF-8.`, { cause }); }
    if (!code) throw new ProtocolViolationError(`${ERROR_CODE} must not be empty.`);
  }
  let message = fallbackMessage;
  if (frame.payload !== undefined) {
    try { message = strictText(frame.payload) || fallbackMessage; }
    catch (cause) { throw new ProtocolViolationError("Remote error reason is not valid UTF-8.", { cause }); }
  }
  return new PureReactiveProtocolError(message, code);
};

const remoteCancel = (frame: ProtocolFrame): PureReactiveProtocolError => {
  let message = "Remote cancelled the stream.";
  if (frame.payload !== undefined) {
    try { message = strictText(frame.payload) || message; }
    catch (cause) { throw new ProtocolViolationError("Remote cancellation reason is not valid UTF-8.", { cause }); }
  }
  return new PureReactiveProtocolError(message, "REMOTE_CANCEL");
};

const remoteCloseReason = (frame: ProtocolFrame): string => {
  if (frame.payload === undefined) return "Remote closed the session.";
  try { return strictText(frame.payload) || "Remote closed the session."; }
  catch (cause) { throw new ProtocolViolationError("Remote session close reason is not valid UTF-8.", { cause }); }
};

class ReactiveStreamImpl implements ReactiveStream {
  constructor(private readonly host: StreamHost, private readonly streamState: StreamState) {}

  get id(): bigint { return this.streamState.id; }
  get attributes(): readonly ProtocolAttribute[] { return this.streamState.attributes; }
  get signal(): AbortSignal { return this.streamState.abortController.signal; }
  get closed(): boolean { return this.streamState.cancelled || (this.streamState.inboundClosed && this.streamState.outboundClosed); }

  async request(count = 1): Promise<void> {
    if (!Number.isInteger(count) || count <= 0 || count > 0xffffffff) throw new RangeError("Demand must be an integer between 1 and 2^32-1.");
    if (this.streamState.cancelled || this.host.state() === "closed" || this.host.state() === "detached") throw new StreamClosedError();
    if (this.streamState.inboundClosed) return;
    this.streamState.inboundCredit = addCredit(this.streamState.inboundCredit, count);
    try { await this.host.write({ kind: FrameKind.DEMAND, streamId: this.id, credit: count }); }
    catch (error) {
      if (!this.streamState.cancelled && !this.streamState.inboundClosed) this.streamState.inboundCredit = Math.max(0, this.streamState.inboundCredit - count);
      throw error;
    }
  }

  send(data: Uint8Array = new Uint8Array(0), attributes: readonly ProtocolAttribute[] = []): Promise<void> {
    this.ensureOutboundOpen();
    const payload = data.slice();
    const messageAttributes = snapshotAttributes(attributes);
    const operation = this.streamState.writeTail.then(async () => {
      await this.waitForOutboundCredit();
      this.ensureOutboundOpen();
      this.streamState.outboundCredit -= 1;
      try {
        await this.host.writeData(this.id, payload, messageAttributes);
      } catch (error) {
        if (this.host.state() === "ready" && !this.streamState.cancelled && !this.streamState.outboundClosed) {
          this.streamState.outboundCredit = addCredit(this.streamState.outboundCredit, 1);
        }
        throw error;
      }
    });
    this.streamState.writeTail = operation.catch(() => {});
    return operation;
  }

  complete(): Promise<void> {
    if (this.streamState.outboundClosed || this.streamState.cancelled) return Promise.resolve();
    const operation = this.streamState.writeTail.then(async () => {
      if (this.streamState.outboundClosed || this.streamState.cancelled) return;
      await this.host.write({ kind: FrameKind.COMPLETE, streamId: this.id });
      this.streamState.outboundClosed = true;
      this.host.release(this.streamState);
    });
    this.streamState.writeTail = operation.catch(() => {});
    return operation;
  }

  async cancel(reason: string | Error = "cancelled"): Promise<void> {
    if (this.closed) return;
    const error = reason instanceof Error ? reason : new PureReactiveProtocolError(reason, "CANCELLED");
    this.host.cancel(this.streamState, error, true);
    try {
      await this.host.write({
        kind: FrameKind.CANCEL,
        streamId: this.id,
        ...(error.message ? { payload: boundedErrorPayload(error.message) } : {})
      });
    } catch {
      if (this.host.state() === "ready") await this.host.write({ kind: FrameKind.CANCEL, streamId: this.id }).catch(() => {});
    }
  }

  async fail(error: unknown): Promise<void> {
    if (this.closed) return;
    const normalized = error instanceof Error ? error : new Error(String(error));
    this.host.cancel(this.streamState, normalized, true);
    try {
      await this.host.write({
        kind: FrameKind.ERROR,
        streamId: this.id,
        attributes: [attribute(ERROR_CODE, error instanceof PureReactiveProtocolError ? error.code : "APPLICATION_ERROR")],
        ...(normalized.message ? { payload: boundedErrorPayload(normalized.message) } : {})
      });
    } catch {
      if (this.host.state() === "ready") await this.host.write({ kind: FrameKind.ERROR, streamId: this.id }).catch(() => {});
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<StreamMessage> {
    const incoming = this.streamState.incoming[Symbol.asyncIterator]();
    return {
      next: async () => {
        if (!this.streamState.cancelled && !this.streamState.inboundClosed) await this.request(1);
        return incoming.next();
      },
      return: async () => {
        if (!this.closed) await this.cancel("Stream consumer stopped before completion.");
        return { value: undefined as never, done: true };
      },
      throw: async (error) => {
        if (!this.closed) await this.cancel(error instanceof Error ? error : new Error(String(error)));
        throw error;
      }
    };
  }

  private ensureOutboundOpen(): void {
    if (this.streamState.cancelled || this.host.state() === "closed" || this.host.state() === "detached") throw new StreamClosedError();
    if (this.streamState.outboundClosed) throw new StreamClosedError("The outbound direction has completed.");
  }

  private async waitForOutboundCredit(): Promise<void> {
    while (this.streamState.outboundCredit <= 0) {
      if (this.streamState.cancelled || this.streamState.outboundClosed) throw new StreamClosedError();
      const waiter = deferred<void>();
      this.streamState.creditWaiters.push(waiter);
      await waiter.promise;
    }
  }
}

class ReactiveSessionImpl implements ReactiveSession {
  private readonly incomingStreams = new AsyncQueue<ReactiveStream>();
  private readonly incomingSignals = new AsyncQueue<QueuedSignal>();
  private readonly streams = new Map<bigint, StreamState>();
  private readonly retiredStreams = new Map<bigint, boolean>();
  private readonly retiredOrder: bigint[] = [];
  private readonly acceptors = new Set<StreamAcceptor>();
  private readonly signalAcceptors = new Set<SignalAcceptor>();
  private readonly stateListeners = new Set<(state: SessionState) => void>();
  private readonly extensionDisposers: Array<() => void | Promise<void>> = [];
  private ready = deferred<void>();
  private lane: TransportLane | undefined;
  private connection: TransportConnection | undefined;
  private origin: SessionOrigin | undefined;
  private nextStreamId = 0n;
  private highestRemoteStreamId = 0n;
  private outgoingSequence = 1n;
  private expectedIncomingSequence = 1n;
  private writeTail: Promise<void> = Promise.resolve();
  private openTail: Promise<void> = Promise.resolve();
  private sessionState: SessionState = "idle";
  private logicalSessionId = randomId();
  private negotiated = new CapabilitySet([]);
  private outboundLimits: Readonly<ProtocolLimits>;
  private remoteMaxInboundStreams = 0xffffffff;
  private remoteMaxInboundItemBytes = 0xffffffff;
  private inFlightReassemblyBytes = 0;
  private localActiveStreams = 0;
  private remoteActiveStreams = 0;
  private retainedStreamAttributeBytes = 0;
  private pendingSignalBytes = 0;
  private activeSignalTasks = 0;
  private livenessTimer: ReturnType<typeof setTimeout> | undefined;
  private lastInboundAt = Date.now();
  private nextProbeId = 1n;

  readonly [SESSION_INTEGRATION]: SessionIntegration = {
    registerAcceptor: (acceptor) => { this.acceptors.add(acceptor); return () => this.acceptors.delete(acceptor); },
    registerSignalAcceptor: (acceptor) => { this.signalAcceptors.add(acceptor); return () => this.signalAcceptors.delete(acceptor); },
    capabilities: () => this.negotiated
  };

  constructor(private readonly runtime: ProtocolRuntime) {
    this.outboundLimits = runtime.limits;
  }

  get state(): SessionState { return this.sessionState; }
  get sessionId(): string { return this.logicalSessionId; }
  get signals(): AsyncIterable<SessionSignal> {
    const queue = this.incomingSignals;
    return {
      [Symbol.asyncIterator]: () => ({
        next: async () => {
          const next = await queue.next();
          if (next.done) return { value: undefined as never, done: true };
          this.pendingSignalBytes = Math.max(0, this.pendingSignalBytes - next.value.bytes);
          return { value: next.value.signal, done: false };
        }
      })
    };
  }

  supports(capabilityId: string): boolean { return this.negotiated.has(capabilityId); }
  onStateChange(listener: (state: SessionState) => void): () => void { this.stateListeners.add(listener); return () => this.stateListeners.delete(listener); }

  async attach(transport: ReactiveTransport, options: AttachOptions): Promise<this> {
    if (this.sessionState === "closed") throw new PureReactiveProtocolError("Session is closed.", "SESSION_CLOSED");
    if (this.sessionState !== "idle") throw new PureReactiveProtocolError("This alpha does not reattach a detached logical session. Create a fresh session until resume/migration is implemented.", "SESSION_REATTACH_UNSUPPORTED");
    this.transition("connecting");
    this.origin = options.origin;
    this.nextStreamId = options.origin === "initiator" ? 1n : 2n;
    this.highestRemoteStreamId = 0n;
    this.outgoingSequence = 1n;
    this.expectedIncomingSequence = 1n;
    this.lastInboundAt = Date.now();
    this.nextProbeId = 1n;
    this.stopLiveness();
    this.writeTail = Promise.resolve();
    this.openTail = Promise.resolve();
    this.ready = deferred<void>();
    void this.ready.promise.catch(() => {});

    try {
      const connectionPromise = transport.connect(options.signal);
      if (options.signal) void connectionPromise.then((connection) => { if (options.signal?.aborted) void Promise.resolve(connection.close(undefined, "aborted")).catch(() => {}); }).catch(() => {});
      this.connection = await waitWithAbort(connectionPromise, options.signal);
      const lanePromise = this.connection.openLane({
        ...RELIABLE_ORDERED_LANE,
        maxFrameBytes: Math.max(this.runtime.limits.maxFrameBytes, BOOTSTRAP_PROTOCOL_LIMITS.maxFrameBytes)
      });
      if (options.signal) {
        void lanePromise.then((lane) => {
          if (options.signal?.aborted) void Promise.resolve(lane.close("aborted")).catch(() => {});
        }).catch(() => {});
      }
      this.lane = await waitWithAbort(lanePromise, options.signal);
      void this.readLoop(this.lane);
      void this.connection.closed.catch(() => {});

      if (options.origin === "initiator") {
        await this.writeFrame({
          kind: FrameKind.HELLO,
          streamId: 0n,
          attributes: [
            attribute(SESSION_ID, this.logicalSessionId, { required: true }),
            ...capabilitiesToAttributes(this.runtime.capabilities, this.runtime.policy)
          ]
        });
      }
      let timeout: ReturnType<typeof setTimeout> | undefined;
      const handshakeTimeout = new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(() => reject(new PureReactiveProtocolError(
          `PRP session negotiation exceeded ${this.runtime.handshakeTimeoutMs}ms.`,
          "HANDSHAKE_TIMEOUT"
        )), this.runtime.handshakeTimeoutMs);
      });
      try {
        await waitWithAbort(Promise.race([this.ready.promise, handshakeTimeout]), options.signal);
      } finally {
        if (timeout !== undefined) clearTimeout(timeout);
      }
      return this;
    } catch (error) {
      if (this.state !== "detached" && this.state !== "closed") this.detach(error);
      throw error;
    }
  }

  open(attributes: readonly ProtocolAttribute[] = []): Promise<ReactiveStream> {
    const streamAttributes = snapshotAttributes(attributes);
    const operation = this.openTail.then(() => this.openNext(streamAttributes));
    this.openTail = operation.then(() => {}, () => {});
    return operation;
  }

  private async openNext(attributes: readonly ProtocolAttribute[]): Promise<ReactiveStream> {
    this.assertReady();
    if (this.nextStreamId > MAX_U64) throw new PureReactiveProtocolError("Logical stream id space is exhausted for this session.", "STREAM_ID_EXHAUSTED");
    if (this.localActiveStreams >= this.remoteMaxInboundStreams) throw new PureReactiveProtocolError("Peer inbound stream limit reached.", "RESOURCE_EXHAUSTED");
    const streamId = this.nextStreamId;
    const streamState = this.createStreamState(streamId, attributes, true);
    if (this.retainedStreamAttributeBytes + streamState.retainedAttributeBytes > this.runtime.limits.maxRetainedStreamAttributeBytes) throw new PureReactiveProtocolError("Retained stream attribute budget reached.", "RESOURCE_EXHAUSTED");
    this.streams.set(streamId, streamState);
    this.localActiveStreams += 1;
    this.retainedStreamAttributeBytes += streamState.retainedAttributeBytes;
    try {
      await this.writeFrame({ kind: FrameKind.OPEN, streamId, ...(attributes.length === 0 ? {} : { attributes }) });
      this.nextStreamId += 2n;
      return this.createStream(streamState);
    } catch (error) {
      if (this.streams.delete(streamId)) {
        this.localActiveStreams = Math.max(0, this.localActiveStreams - 1);
        this.retainedStreamAttributeBytes = Math.max(0, this.retainedStreamAttributeBytes - streamState.retainedAttributeBytes);
      }
      if (!streamState.abortController.signal.aborted) streamState.abortController.abort(error);
      throw error;
    }
  }

  async signal(attributes: readonly ProtocolAttribute[] = [], payload?: Uint8Array): Promise<void> {
    this.assertReady();
    const signalAttributes = snapshotAttributes(attributes);
    const signalPayload = payload?.slice();
    await this.writeFrame({
      kind: FrameKind.SIGNAL,
      streamId: 0n,
      ...(signalAttributes.length === 0 ? {} : { attributes: signalAttributes }),
      ...(signalPayload === undefined ? {} : { payload: signalPayload })
    });
  }

  async close(reason = "closed"): Promise<void> {
    if (this.sessionState === "closed") return;
    if (this.sessionState === "ready" && this.lane) {
      try {
        await this.writeFrame({
          kind: FrameKind.CLOSE,
          streamId: 0n,
          ...(reason ? { payload: boundedErrorPayload(reason) } : {})
        });
      } catch {
        if (this.sessionState === "ready") await this.writeFrame({ kind: FrameKind.CLOSE, streamId: 0n }).catch(() => {});
      }
    }
    await this.finishClosed(reason);
  }

  [Symbol.asyncIterator](): AsyncIterator<ReactiveStream> { return this.incomingStreams[Symbol.asyncIterator](); }

  private createStream(streamState: StreamState): ReactiveStream {
    const host: StreamHost = {
      state: () => this.sessionState,
      write: (frame) => this.writeFrame(frame),
      writeData: (streamId, data, attributes) => this.writeDataItem(streamId, data, attributes),
      release: (state) => this.releaseStream(state),
      cancel: (state, error, allowLateData) => this.cancelStream(state, error, allowLateData)
    };
    return new ReactiveStreamImpl(host, streamState);
  }

  private async writeDataItem(
    streamId: bigint,
    data: Uint8Array,
    attributes: readonly ProtocolAttribute[]
  ): Promise<void> {
    if (data.byteLength > this.remoteMaxInboundItemBytes) {
      throw new PureReactiveProtocolError(
        `Logical DATA item exceeds peer maxInboundItemBytes (${this.remoteMaxInboundItemBytes}).`,
        "ITEM_TOO_LARGE"
      );
    }

    const attributeBytes = measureAttributeBytes(attributes, this.outboundLimits);
    const firstPayloadCapacity = this.outboundLimits.maxFrameBytes - FRAME_HEADER_BYTES - attributeBytes;
    if (firstPayloadCapacity < 0) throw new RangeError(`DATA attributes do not fit the negotiated ${this.outboundLimits.maxFrameBytes}-byte frame limit.`);

    const streamState = this.streams.get(streamId);
    if (!streamState) throw new StreamClosedError();

    if (data.byteLength <= firstPayloadCapacity) {
      const frame: Omit<ProtocolFrame, "sequence"> = {
        kind: FrameKind.DATA,
        streamId,
        ...(attributes.length === 0 ? {} : { attributes }),
        ...(data.byteLength === 0 ? {} : { payload: data })
      };
      encodeFrame({ ...frame, sequence: 1n }, this.outboundLimits);
      await this.writeDataFrames(streamState, 1, () => frame);
      return;
    }

    if (!this.negotiated.has(FRAGMENTATION_CAPABILITY_ID)) {
      throw new CapabilityMismatchError(`Required capability was not negotiated: ${FRAGMENTATION_CAPABILITY_ID}`);
    }
    const fragmentPayloadCapacity = this.outboundLimits.maxFrameBytes - FRAME_HEADER_BYTES;
    if (fragmentPayloadCapacity <= 0) throw new RangeError("Negotiated frame limit leaves no payload space for FRAGMENT frames.");

    const firstPayloadBytes = Math.max(0, firstPayloadCapacity);
    const firstFrame: Omit<ProtocolFrame, "sequence"> = {
      kind: FrameKind.DATA,
      streamId,
      flags: DATA_FRAGMENTED_FLAG,
      fragmentLength: data.byteLength,
      ...(attributes.length === 0 ? {} : { attributes }),
      ...(firstPayloadBytes === 0 ? {} : { payload: data.subarray(0, firstPayloadBytes) })
    };
    const remainingBytes = data.byteLength - firstPayloadBytes;
    const fragmentCount = Math.ceil(remainingBytes / fragmentPayloadCapacity);
    const frameCount = 1 + fragmentCount;

    // Validate all distinct frame shapes before the first byte reaches the transport without building
    // an array proportional to fragment count.
    encodeFrame({ ...firstFrame, sequence: 1n }, this.outboundLimits);
    if (fragmentCount > 0) {
      encodeFrame({
        kind: FrameKind.FRAGMENT,
        streamId,
        sequence: 1n,
        payload: data.subarray(firstPayloadBytes, Math.min(data.byteLength, firstPayloadBytes + fragmentPayloadCapacity))
      }, this.outboundLimits);
    }

    await this.writeDataFrames(streamState, frameCount, (index) => {
      if (index === 0) return firstFrame;
      const offset = firstPayloadBytes + (index - 1) * fragmentPayloadCapacity;
      return {
        kind: FrameKind.FRAGMENT,
        streamId,
        payload: data.subarray(offset, Math.min(data.byteLength, offset + fragmentPayloadCapacity))
      };
    });
  }

  private writeDataFrames(
    streamState: StreamState,
    frameCount: number,
    frameAt: (index: number) => Omit<ProtocolFrame, "sequence">
  ): Promise<void> {
    const operation = this.writeTail.then(async () => {
      if (!Number.isSafeInteger(frameCount) || frameCount <= 0) throw new RangeError("Fragment frame count must be a positive safe integer.");
      if (this.outgoingSequence + BigInt(frameCount) - 1n > MAX_U64) {
        const error = new PureReactiveProtocolError("Peer sequence space is exhausted for this attachment.", "SEQUENCE_EXHAUSTED");
        this.detach(error);
        throw error;
      }
      for (let index = 0; index < frameCount; index += 1) {
        if (streamState.cancelled || streamState.outboundClosed) throw new StreamClosedError();
        await this.writeFrameNow(frameAt(index));
      }
    });
    this.writeTail = operation.catch(() => {});
    return operation;
  }

  private writeFrame(frame: Omit<ProtocolFrame, "sequence">): Promise<void> {
    return this.writeFrames([frame]);
  }

  private writeFrames(frames: readonly Omit<ProtocolFrame, "sequence">[]): Promise<void> {
    const operation = this.writeTail.then(async () => {
      if (frames.length === 0) return;
      if (this.outgoingSequence + BigInt(frames.length) - 1n > MAX_U64) {
        const error = new PureReactiveProtocolError("Peer sequence space is exhausted for this attachment.", "SEQUENCE_EXHAUSTED");
        this.detach(error);
        throw error;
      }
      for (const frame of frames) await this.writeFrameNow(frame);
    });
    this.writeTail = operation.catch(() => {});
    return operation;
  }

  private async writeFrameNow(frame: Omit<ProtocolFrame, "sequence">): Promise<void> {
    if (!this.lane) throw new ConnectionLostError("No physical lane is attached to the session.");
    if (this.outgoingSequence > MAX_U64) {
      const error = new PureReactiveProtocolError("Peer sequence space is exhausted for this attachment.", "SEQUENCE_EXHAUSTED");
      this.detach(error);
      throw error;
    }
    const complete: ProtocolFrame = { ...frame, sequence: this.outgoingSequence };
    const bootstrapFrame = frame.kind === FrameKind.HELLO || frame.kind === FrameKind.WELCOME ||
      (frame.kind === FrameKind.ERROR && frame.streamId === 0n && this.sessionState === "connecting");
    const encoded = encodeFrame(complete, bootstrapFrame ? BOOTSTRAP_PROTOCOL_LIMITS : this.outboundLimits);
    try {
      await this.lane.write(encoded);
      this.outgoingSequence += 1n;
    } catch (cause) {
      const error = cause instanceof PureReactiveProtocolError
        ? cause
        : cause instanceof Error ? new ConnectionLostError(cause.message, { cause }) : new ConnectionLostError();
      this.detach(error);
      throw error;
    }
  }

  private releaseStream(streamState: StreamState): void {
    if (!((streamState.inboundClosed && streamState.outboundClosed) || streamState.cancelled)) return;
    if (this.streams.delete(streamState.id)) {
      if (streamState.initiatedLocally) this.localActiveStreams = Math.max(0, this.localActiveStreams - 1);
      else this.remoteActiveStreams = Math.max(0, this.remoteActiveStreams - 1);
      this.retainedStreamAttributeBytes = Math.max(0, this.retainedStreamAttributeBytes - streamState.retainedAttributeBytes);
    }
    this.retire(streamState.id, false);
  }

  private cancelStream(streamState: StreamState, error: unknown, allowLateData: boolean): void {
    if (streamState.cancelled) return;
    this.clearReassembly(streamState);
    streamState.cancelled = true;
    streamState.inboundClosed = true;
    streamState.outboundClosed = true;
    streamState.incoming.end(error, true);
    if (!streamState.abortController.signal.aborted) streamState.abortController.abort(error);
    for (const waiter of streamState.creditWaiters.splice(0)) waiter.reject(error);
    if (this.streams.delete(streamState.id)) {
      if (streamState.initiatedLocally) this.localActiveStreams = Math.max(0, this.localActiveStreams - 1);
      else this.remoteActiveStreams = Math.max(0, this.remoteActiveStreams - 1);
      this.retainedStreamAttributeBytes = Math.max(0, this.retainedStreamAttributeBytes - streamState.retainedAttributeBytes);
    }
    this.retire(streamState.id, allowLateData);
  }

  private clearReassembly(streamState: StreamState): void {
    if (!streamState.reassembly) return;
    this.inFlightReassemblyBytes = Math.max(0, this.inFlightReassemblyBytes - streamState.reassembly.data.byteLength);
    delete streamState.reassembly;
  }

  private retire(streamId: bigint, allowLateData: boolean): void {
    if (this.retiredStreams.has(streamId)) return;
    this.retiredStreams.set(streamId, allowLateData);
    this.retiredOrder.push(streamId);
    if (this.retiredOrder.length > this.runtime.limits.maxRetiredStreams) {
      const oldest = this.retiredOrder.shift();
      if (oldest !== undefined) this.retiredStreams.delete(oldest);
    }
  }

  private isPastStreamId(streamId: bigint): boolean {
    if (streamId <= 0n || !this.origin) return false;
    const localParity = this.origin === "initiator" ? 1n : 0n;
    if (streamId % 2n === localParity) return streamId < this.nextStreamId;
    return streamId <= this.highestRemoteStreamId;
  }

  private createStreamState(streamId: bigint, attributes: readonly ProtocolAttribute[], initiatedLocally: boolean): StreamState {
    const streamAttributes = snapshotAttributes(attributes);
    return {
      id: streamId,
      attributes: streamAttributes,
      incoming: new AsyncQueue<StreamMessage>(),
      abortController: new AbortController(),
      retainedAttributeBytes: attributeMemoryBytes(streamAttributes),
      initiatedLocally,
      inboundCredit: 0,
      outboundCredit: 0,
      outboundClosed: false,
      inboundClosed: false,
      cancelled: false,
      writeTail: Promise.resolve(),
      creditWaiters: []
    };
  }

  private async readLoop(lane: TransportLane): Promise<void> {
    try {
      for await (const raw of lane.incoming) {
        const frame = decodeFrame(raw, this.sessionState === "connecting" ? BOOTSTRAP_PROTOCOL_LIMITS : this.runtime.limits);
        if (frame.sequence !== this.expectedIncomingSequence) throw new ProtocolViolationError(`Expected peer sequence ${this.expectedIncomingSequence}, received ${frame.sequence}.`);
        this.expectedIncomingSequence += 1n;
        this.lastInboundAt = Date.now();
        await this.handleFrame(frame);
      }
      if (this.sessionState !== "closed") this.detach();
    } catch (error) {
      this.detach(error);
    }
  }

  private async handleFrame(frame: ProtocolFrame): Promise<void> {
    if (frame.kind === FrameKind.HELLO) return this.handleHello(frame);
    if (frame.kind === FrameKind.WELCOME) return this.handleWelcome(frame);
    if (frame.kind === FrameKind.ERROR && frame.streamId === 0n) throw remoteDiagnostic(frame, "Remote session error.", "REMOTE_ERROR");
    if (this.sessionState !== "ready") throw new ProtocolViolationError(`${FrameKind[frame.kind]} arrived before session negotiation completed.`);
    if (frame.kind === FrameKind.PING) return this.handlePing(frame);
    if (frame.kind === FrameKind.PONG) return this.handlePong(frame);
    if (frame.kind === FrameKind.CLOSE) return this.finishClosed(remoteCloseReason(frame));
    if (frame.kind === FrameKind.OPEN) return this.handleOpen(frame);
    if (frame.kind === FrameKind.SIGNAL) return this.handleSignal(frame);

    const streamState = this.streams.get(frame.streamId);
    if (!streamState) {
      const retired = this.retiredStreams.get(frame.streamId);
      const pastStream = this.isPastStreamId(frame.streamId);
      if (pastStream && (frame.kind === FrameKind.DEMAND || frame.kind === FrameKind.COMPLETE || frame.kind === FrameKind.CANCEL || frame.kind === FrameKind.ERROR)) return;
      if ((frame.kind === FrameKind.DATA || frame.kind === FrameKind.FRAGMENT) && (retired === true || (retired === undefined && pastStream))) return;
      throw new ProtocolViolationError(`Frame references unknown stream ${frame.streamId}.`);
    }

    switch (frame.kind) {
      case FrameKind.DATA:
        await this.handleData(streamState, frame);
        return;
      case FrameKind.FRAGMENT:
        this.handleFragment(streamState, frame);
        return;
      case FrameKind.DEMAND:
        if (!frame.credit) throw new ProtocolViolationError("DEMAND frame has no positive credit.");
        streamState.outboundCredit = addCredit(streamState.outboundCredit, frame.credit);
        for (const waiter of streamState.creditWaiters.splice(0)) waiter.resolve();
        return;
      case FrameKind.COMPLETE:
        if (streamState.reassembly) throw new ProtocolViolationError(`COMPLETE interrupted a fragmented DATA item on stream ${frame.streamId}.`);
        streamState.inboundClosed = true;
        streamState.incoming.end();
        this.releaseStream(streamState);
        return;
      case FrameKind.CANCEL:
        this.cancelStream(streamState, remoteCancel(frame), false);
        return;
      case FrameKind.ERROR:
        this.cancelStream(streamState, remoteDiagnostic(frame, "Remote stream error.", "REMOTE_ERROR"), false);
        return;
      default:
        throw new ProtocolViolationError(`Unexpected frame kind ${FrameKind[frame.kind]} on stream ${frame.streamId}.`);
    }
  }

  private async handleData(streamState: StreamState, frame: ProtocolFrame): Promise<void> {
    if (streamState.inboundClosed || streamState.cancelled) throw new ProtocolViolationError(`DATA received for closed stream ${frame.streamId}.`);
    if (streamState.reassembly) throw new ProtocolViolationError(`New DATA arrived before fragmented item completion on stream ${frame.streamId}.`);
    if (streamState.inboundCredit <= 0) throw new ProtocolViolationError(`Peer exceeded granted demand on stream ${frame.streamId}.`);
    streamState.inboundCredit -= 1;

    const fragmented = (frame.flags ?? 0) === DATA_FRAGMENTED_FLAG;
    if (!fragmented) {
      const payload = frame.payload ?? new Uint8Array(0);
      if (payload.byteLength > this.runtime.limits.maxInboundItemBytes) {
        throw new ProtocolViolationError(`Peer DATA item exceeds maxInboundItemBytes (${this.runtime.limits.maxInboundItemBytes}).`);
      }
      streamState.incoming.push({
        data: payload.slice(),
        attributes: snapshotAttributes(frame.attributes ?? [])
      });
      return;
    }

    const totalBytes = frame.fragmentLength ?? 0;
    if (totalBytes > this.runtime.limits.maxInboundItemBytes) {
      throw new ProtocolViolationError(`Peer fragmented DATA item exceeds maxInboundItemBytes (${this.runtime.limits.maxInboundItemBytes}).`);
    }
    if (this.inFlightReassemblyBytes + totalBytes > this.runtime.limits.maxInFlightReassemblyBytes) {
      await this.failInboundStream(streamState, new PureReactiveProtocolError(
        "Fragment reassembly byte budget is exhausted.",
        "RESOURCE_EXHAUSTED"
      ));
      return;
    }

    let data: Uint8Array;
    try { data = new Uint8Array(totalBytes); }
    catch (cause) {
      await this.failInboundStream(streamState, new PureReactiveProtocolError(
        "Unable to reserve memory for fragmented DATA item.",
        "RESOURCE_EXHAUSTED",
        { cause }
      ));
      return;
    }
    const first = frame.payload ?? new Uint8Array(0);
    data.set(first, 0);
    streamState.reassembly = {
      data,
      offset: first.byteLength,
      attributes: snapshotAttributes(frame.attributes ?? [])
    };
    this.inFlightReassemblyBytes += totalBytes;
  }

  private handleFragment(streamState: StreamState, frame: ProtocolFrame): void {
    if (streamState.inboundClosed || streamState.cancelled) throw new ProtocolViolationError(`FRAGMENT received for closed stream ${frame.streamId}.`);
    const reassembly = streamState.reassembly;
    if (!reassembly) throw new ProtocolViolationError(`FRAGMENT arrived without an active fragmented DATA item on stream ${frame.streamId}.`);
    const fragment = frame.payload ?? new Uint8Array(0);
    if (reassembly.offset + fragment.byteLength > reassembly.data.byteLength) {
      throw new ProtocolViolationError(`FRAGMENT exceeds declared logical DATA length on stream ${frame.streamId}.`);
    }
    reassembly.data.set(fragment, reassembly.offset);
    reassembly.offset += fragment.byteLength;
    if (reassembly.offset !== reassembly.data.byteLength) return;

    const message: StreamMessage = { data: reassembly.data, attributes: reassembly.attributes };
    this.clearReassembly(streamState);
    streamState.incoming.push(message);
  }

  private async failInboundStream(streamState: StreamState, error: PureReactiveProtocolError): Promise<void> {
    this.cancelStream(streamState, error, true);
    try {
      await this.writeFrame({
        kind: FrameKind.ERROR,
        streamId: streamState.id,
        attributes: [attribute(ERROR_CODE, error.code)],
        ...(error.message ? { payload: boundedErrorPayload(error.message) } : {})
      });
    } catch {
      if (this.sessionState === "ready") await this.writeFrame({ kind: FrameKind.ERROR, streamId: streamState.id }).catch(() => {});
    }
  }

  private async handleHello(frame: ProtocolFrame): Promise<void> {
    if (this.origin !== "acceptor" || this.sessionState !== "connecting") throw new ProtocolViolationError("Unexpected HELLO frame.");
    try {
      const attributes = frame.attributes ?? [];
      const remoteSessionId = this.validateHandshakeAttributes(attributes, "HELLO");
      this.logicalSessionId = remoteSessionId;
      this.negotiated = negotiateCapabilities(this.runtime.capabilities, capabilitiesFromAttributes(attributes), this.runtime.policy);
      this.applyPeerLimits();
      await this.attachExtensions();
      await this.writeFrame({
        kind: FrameKind.WELCOME,
        streamId: 0n,
        attributes: [
          attribute(SESSION_ID, this.logicalSessionId, { required: true }),
          ...capabilitiesToAttributes(this.negotiated.toArray().map((item) => ({
            id: item.id,
            minVersion: item.version,
            maxVersion: item.version,
            ...(item.local.parameters === undefined ? {} : { parameters: item.local.parameters })
          })), this.runtime.policy)
        ]
      });
      this.transition("ready");
      this.startLiveness();
      this.ready.resolve();
    } catch (error) {
      this.ready.reject(error);
      await this.writeSessionError(error);
      throw error;
    }
  }

  private async handleWelcome(frame: ProtocolFrame): Promise<void> {
    if (this.origin !== "initiator" || this.sessionState !== "connecting") throw new ProtocolViolationError("Unexpected WELCOME frame.");
    try {
      const attributes = frame.attributes ?? [];
      const echoedSessionId = this.validateHandshakeAttributes(attributes, "WELCOME");
      if (echoedSessionId !== this.logicalSessionId) throw new ProtocolViolationError("WELCOME session id does not match HELLO.");
      this.negotiated = negotiateCapabilities(this.runtime.capabilities, capabilitiesFromAttributes(attributes), this.runtime.policy);
      for (const required of this.runtime.policy.require ?? []) if (!this.negotiated.has(required)) throw new CapabilityMismatchError(`Required capability was not negotiated: ${required}`);
      this.applyPeerLimits();
      await this.attachExtensions();
      this.transition("ready");
      this.startLiveness();
      this.ready.resolve();
    } catch (error) {
      this.ready.reject(error);
      await this.writeSessionError(error);
      throw error;
    }
  }

  private validateHandshakeAttributes(attributes: readonly ProtocolAttribute[], frameName: string): string {
    const sessionIds = attributesById(attributes, SESSION_ID);
    if (sessionIds.length !== 1 || sessionIds[0]?.required !== true) throw new ProtocolViolationError(`${frameName} must contain exactly one required ${SESSION_ID}.`);
    for (const item of attributes) {
      if (!item.required) continue;
      if (item.id === SESSION_ID || item.id.startsWith("prp.capability/")) continue;
      throw new CapabilityMismatchError(`Unknown required handshake attribute: ${item.id}`);
    }
    let value: string;
    try { value = attributeTextStrict(sessionIds, SESSION_ID) ?? ""; }
    catch (cause) { throw new ProtocolViolationError(`${SESSION_ID} is not valid UTF-8.`, { cause }); }
    if (!value) throw new ProtocolViolationError(`${frameName} is missing a non-empty ${SESSION_ID}.`);
    return value;
  }

  private applyPeerLimits(): void {
    const capability = this.negotiated.get(CORE_LIMITS_CAPABILITY_ID);
    if (!capability) throw new CapabilityMismatchError(`Required capability was not negotiated: ${CORE_LIMITS_CAPABILITY_ID}`);
    let remote;
    try { remote = decodePeerProtocolLimits(capability.remote.parameters); }
    catch (cause) { throw new ProtocolViolationError("Remote prp.core.limits parameters are malformed.", { cause }); }
    this.remoteMaxInboundStreams = remote.maxInboundStreams;
    this.remoteMaxInboundItemBytes = remote.maxInboundItemBytes;
    this.outboundLimits = Object.freeze({
      ...this.runtime.limits,
      maxFrameBytes: Math.min(this.runtime.limits.maxFrameBytes, remote.maxFrameBytes),
      maxAttributeBytes: Math.min(this.runtime.limits.maxAttributeBytes, remote.maxAttributeBytes),
      maxAttributes: Math.min(this.runtime.limits.maxAttributes, remote.maxAttributes),
      maxAttributeIdBytes: Math.min(this.runtime.limits.maxAttributeIdBytes, remote.maxAttributeIdBytes),
      maxAttributeValueBytes: Math.min(this.runtime.limits.maxAttributeValueBytes, remote.maxAttributeValueBytes)
    });
    const liveness = this.negotiated.get(CORE_LIVENESS_CAPABILITY_ID);
    if (!liveness) throw new CapabilityMismatchError(`Required capability was not negotiated: ${CORE_LIVENESS_CAPABILITY_ID}`);
    try { decodeLivenessParameters(liveness.remote.parameters); }
    catch (cause) { throw new ProtocolViolationError("Remote prp.core.liveness parameters are malformed.", { cause }); }
  }

  private async attachExtensions(): Promise<void> {
    for (const extension of this.runtime.extensions) {
      if (!this.negotiated.has(extension.capability.id)) continue;
      const disposer = await extension.attach(this);
      if (typeof disposer !== "function") continue;
      if (this.sessionState === "closed" || this.sessionState === "detached") {
        await Promise.resolve().then(disposer).catch(() => {});
        continue;
      }
      this.extensionDisposers.push(disposer);
    }
  }

  private async disposeExtensions(): Promise<void> {
    for (const dispose of this.extensionDisposers.splice(0).reverse()) await Promise.resolve().then(dispose).catch(() => {});
  }

  private async handleOpen(frame: ProtocolFrame): Promise<void> {
    const expectedRemoteStreamId = this.highestRemoteStreamId === 0n
      ? (this.origin === "initiator" ? 2n : 1n)
      : this.highestRemoteStreamId + 2n;
    if (frame.streamId !== expectedRemoteStreamId) throw new ProtocolViolationError(`Peer must allocate stream ids contiguously by parity; expected ${expectedRemoteStreamId}, received ${frame.streamId}.`);
    this.highestRemoteStreamId = frame.streamId;
    if (this.remoteActiveStreams >= this.runtime.limits.maxInboundStreams) {
      await this.rejectRemoteOpen(frame.streamId, new PureReactiveProtocolError("Inbound stream limit reached.", "RESOURCE_EXHAUSTED"));
      return;
    }
    const streamState = this.createStreamState(frame.streamId, frame.attributes ?? [], false);
    if (this.retainedStreamAttributeBytes + streamState.retainedAttributeBytes > this.runtime.limits.maxRetainedStreamAttributeBytes) {
      await this.rejectRemoteOpen(frame.streamId, new PureReactiveProtocolError("Retained stream attribute budget reached.", "RESOURCE_EXHAUSTED"));
      return;
    }
    const stream = this.createStream(streamState);
    for (const acceptor of this.acceptors) {
      let accepted = false;
      try { accepted = acceptor.accepts(stream); }
      catch (error) { await stream.fail(error); return; }
      if (!accepted) continue;
      this.streams.set(frame.streamId, streamState);
      this.remoteActiveStreams += 1;
      this.retainedStreamAttributeBytes += streamState.retainedAttributeBytes;
      void Promise.resolve().then(() => acceptor.handle(stream)).catch(async (error) => {
        if (!stream.closed) await stream.fail(error).catch(() => {});
      });
      return;
    }
    if (this.incomingStreams.size >= this.runtime.limits.maxPendingIncomingStreams) {
      await this.rejectRemoteOpen(frame.streamId, new PureReactiveProtocolError("Pending incoming stream limit reached.", "RESOURCE_EXHAUSTED"));
      return;
    }
    this.streams.set(frame.streamId, streamState);
    this.remoteActiveStreams += 1;
    this.retainedStreamAttributeBytes += streamState.retainedAttributeBytes;
    this.incomingStreams.push(stream);
  }

  private async rejectRemoteOpen(streamId: bigint, error: PureReactiveProtocolError): Promise<void> {
    this.retire(streamId, false);
    try {
      await this.writeFrame({
        kind: FrameKind.ERROR,
        streamId,
        attributes: [attribute(ERROR_CODE, error.code)],
        ...(error.message ? { payload: boundedErrorPayload(error.message) } : {})
      });
    } catch {
      if (this.sessionState === "ready") await this.writeFrame({ kind: FrameKind.ERROR, streamId }).catch(() => {});
    }
  }

  private handleSignal(frame: ProtocolFrame): void {
    const signal: SessionSignal = { data: frame.payload?.slice() ?? new Uint8Array(0), attributes: snapshotAttributes(frame.attributes ?? []) };
    const bytes = signal.data.byteLength + attributeMemoryBytes(signal.attributes);
    if (bytes > this.runtime.limits.maxInFlightSignalBytes || this.pendingSignalBytes + bytes > this.runtime.limits.maxInFlightSignalBytes) throw new ProtocolViolationError("Session signal byte budget exceeded.");
    for (const acceptor of this.signalAcceptors) {
      let accepted = false;
      try { accepted = acceptor.accepts(signal); }
      catch (error) { this.detach(error); return; }
      if (!accepted) continue;
      if (this.activeSignalTasks >= this.runtime.limits.maxPendingSignals) throw new ProtocolViolationError("Too many in-flight session signal handlers.");
      this.activeSignalTasks += 1;
      this.pendingSignalBytes += bytes;
      void Promise.resolve().then(() => acceptor.handle(signal)).catch((error) => this.detach(error)).finally(() => {
        this.activeSignalTasks = Math.max(0, this.activeSignalTasks - 1);
        this.pendingSignalBytes = Math.max(0, this.pendingSignalBytes - bytes);
      });
      return;
    }
    if (this.incomingSignals.size >= this.runtime.limits.maxPendingSignals) throw new ProtocolViolationError("Pending session signal limit reached.");
    this.pendingSignalBytes += bytes;
    this.incomingSignals.push({ signal, bytes });
  }

  private async handlePing(frame: ProtocolFrame): Promise<void> {
    try { decodeProbe(frame.payload); }
    catch (cause) { throw new ProtocolViolationError("PING contains an invalid probe id.", { cause }); }
    await this.writeFrame({ kind: FrameKind.PONG, streamId: 0n, payload: frame.payload! });
  }

  private handlePong(frame: ProtocolFrame): void {
    try { decodeProbe(frame.payload); }
    catch (cause) { throw new ProtocolViolationError("PONG contains an invalid probe id.", { cause }); }
  }

  private startLiveness(): void {
    this.stopLiveness();
    const tick = async (): Promise<void> => {
      if (this.sessionState !== "ready") return;
      const now = Date.now();
      const silentFor = now - this.lastInboundAt;
      if (silentFor >= this.runtime.liveness.timeoutMs) {
        this.detach(new LivenessTimeoutError(`No inbound PRP frame was received for ${silentFor}ms (timeout ${this.runtime.liveness.timeoutMs}ms).`));
        return;
      }
      if (silentFor >= this.runtime.liveness.intervalMs) {
        const probe = this.nextProbeId;
        this.nextProbeId = probe >= MAX_U64 ? 1n : probe + 1n;
        await this.writeFrame({ kind: FrameKind.PING, streamId: 0n, payload: encodeProbe(probe) }).catch(() => {});
      }
      if (this.sessionState === "ready") this.scheduleLiveness(tick);
    };
    this.scheduleLiveness(tick);
  }

  private scheduleLiveness(tick: () => Promise<void>): void {
    this.livenessTimer = setTimeout(() => { void tick(); }, this.runtime.liveness.intervalMs);
    (this.livenessTimer as unknown as { unref?: () => void }).unref?.();
  }

  private stopLiveness(): void {
    if (this.livenessTimer !== undefined) clearTimeout(this.livenessTimer);
    this.livenessTimer = undefined;
  }

  private async writeSessionError(error: unknown): Promise<void> {
    const normalized = error instanceof Error ? error : new Error(String(error));
    await this.writeFrame({
      kind: FrameKind.ERROR,
      streamId: 0n,
      attributes: [attribute(ERROR_CODE, error instanceof PureReactiveProtocolError ? error.code : "NEGOTIATION_ERROR")],
      ...(normalized.message ? { payload: boundedErrorPayload(normalized.message) } : {})
    }).catch(() => {});
  }

  private async finishClosed(reason: string): Promise<void> {
    if (this.sessionState === "closed") return;
    this.stopLiveness();
    const failure = new StreamClosedError(reason);
    for (const streamState of [...this.streams.values()]) this.cancelStream(streamState, failure, false);
    this.incomingStreams.end(undefined, true);
    this.incomingSignals.end(undefined, true);
    this.pendingSignalBytes = 0;
    const lane = this.lane;
    const connection = this.connection;
    this.lane = undefined;
    this.connection = undefined;
    this.transition("closed");
    await this.disposeExtensions();
    void Promise.resolve(lane?.close(reason)).catch(() => {});
    void Promise.resolve(connection?.close(undefined, reason)).catch(() => {});
  }

  private detach(error?: unknown): void {
    if (this.sessionState === "closed" || this.sessionState === "detached") return;
    this.stopLiveness();
    const failure = error instanceof PureReactiveProtocolError
      ? error
      : error instanceof Error ? new ConnectionLostError(error.message, { cause: error }) : new ConnectionLostError();
    if (this.sessionState === "connecting") this.ready.reject(failure);
    for (const streamState of [...this.streams.values()]) this.cancelStream(streamState, failure, false);
    this.incomingStreams.end(failure, true);
    this.incomingSignals.end(failure, true);
    this.pendingSignalBytes = 0;
    const lane = this.lane;
    const connection = this.connection;
    this.lane = undefined;
    this.connection = undefined;
    this.transition("detached");
    void this.disposeExtensions();
    void Promise.resolve(lane?.close("detached")).catch(() => {});
    void Promise.resolve(connection?.close(undefined, "detached")).catch(() => {});
  }

  private assertReady(): void {
    if (this.sessionState !== "ready") throw new PureReactiveProtocolError(`Session is not ready; current state is ${this.sessionState}.`, "SESSION_NOT_READY");
  }

  private transition(state: SessionState): void {
    this.sessionState = state;
    for (const listener of this.stateListeners) {
      try { listener(state); } catch { /* state observers cannot corrupt protocol state */ }
    }
  }
}

const internal = (session: ReactiveSession): SessionIntegration => {
  const access = (session as unknown as { [SESSION_INTEGRATION]?: SessionIntegration })[SESSION_INTEGRATION];
  if (!access) throw new TypeError("The supplied session is not a Pure Reactive Protocol session.");
  return access;
};

export const registerStreamAcceptor = (session: ReactiveSession, acceptor: StreamAcceptor): (() => void) => internal(session).registerAcceptor(acceptor);
export const registerSignalAcceptor = (session: ReactiveSession, acceptor: SignalAcceptor): (() => void) => internal(session).registerSignalAcceptor(acceptor);
export const getNegotiatedCapabilities = (session: ReactiveSession): CapabilitySet => internal(session).capabilities();

export const connectWithRuntime = async (
  transport: ReactiveTransport,
  runtime: ProtocolRuntime,
  options: { readonly origin: SessionOrigin; readonly signal?: AbortSignal }
): Promise<ReactiveSession> => new ReactiveSessionImpl(runtime).attach(transport, {
  origin: options.origin,
  ...(options.signal === undefined ? {} : { signal: options.signal })
});

export const connect = async (
  transport: ReactiveTransport,
  options: { readonly signal?: AbortSignal } = {}
): Promise<ReactiveSession> => connectWithRuntime(
  transport,
  new ProtocolRuntime(),
  { origin: "initiator", ...(options.signal === undefined ? {} : { signal: options.signal }) }
);

export const accept = async (
  transport: ReactiveTransport,
  options: { readonly signal?: AbortSignal } = {}
): Promise<ReactiveSession> => connectWithRuntime(
  transport,
  new ProtocolRuntime(),
  { origin: "acceptor", ...(options.signal === undefined ? {} : { signal: options.signal }) }
);
