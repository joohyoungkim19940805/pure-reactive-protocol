import { AsyncQueue, deferred, type Deferred } from "../../core/async-queue";
import { attribute, bytes, strictText, type ProtocolAttribute } from "../../core/attributes";
import { CapabilitySet, type CapabilityDescriptor, type NegotiatedCapability } from "../../core/capabilities";
import {
  CapabilityMismatchError,
  ConnectionLostError,
  PureReactiveProtocolError,
  StreamClosedError
} from "../../core/errors";
import {
  SESSION_INTEGRATION,
  type ReactiveSession,
  type ReactiveStream,
  type SessionIntegration,
  type DatagramAcceptor,
  type SessionDatagram,
  type SessionSignal,
  type SessionState,
  type StreamAcceptor,
  type StreamMessage,
  type SignalAcceptor
} from "../../core/session";
import type { ReactiveTransport, TransportConnection, TransportLane } from "../../transport/types";
import { RELIABLE_ORDERED_LANE } from "../../transport/types";
import {
  RPC_CAPABILITY_ID,
  RPC_PATTERN_ATTRIBUTE,
  RPC_PROFILE_ATTRIBUTE,
  RPC_PROFILE_ID,
  RPC_TARGET_ATTRIBUTE,
  attachRpcProfile
} from "../../profile/rpc/index";
import { jsonCodec, type PayloadCodec } from "../../profile/rpc/codec";
import { decodeRSocketFrame, defaultRSocketSetup, encodeRSocketFrame, RSocketProtocolError } from "./codec";
import {
  RSOCKET_FLAG_COMPLETE,
  RSOCKET_FLAG_FOLLOWS,
  RSOCKET_FLAG_LEASE,
  RSOCKET_FLAG_NEXT,
  RSOCKET_FLAG_RESPOND,
  RSOCKET_MAX_FRAME_BYTES,
  RSOCKET_MAX_REQUEST_N,
  RSOCKET_MAX_STREAM_ID,
  RSocketErrorCode,
  RSocketFrameType,
  type RSocketFrame,
  type RSocketSetupFields
} from "./frame";
import {
  RSOCKET_COMPOSITE_METADATA_MIME,
  RSOCKET_ROUTING_MIME,
  decodeRoutes,
  encodeRoute,
  encodeRoutingCompositeMetadata,
  firstRouteFromCompositeMetadata
} from "./metadata";

export const RSOCKET_COMPATIBILITY_CAPABILITY_ID = "prp.compat.rsocket-v1";
const MAX_ERROR_BYTES = 1024;
const MAX_IN_FLIGHT_RSOCKET_FRAGMENTS = 65_536;
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

type RpcPattern = "unary" | "notify" | "server-stream" | "duplex";
type RSocketOrigin = "initiator" | "acceptor";

export interface RSocketCompatibilityOptions {
  readonly codec?: PayloadCodec;
  readonly keepAliveMs?: number;
  readonly lifetimeMs?: number;
  /** Maximum encoded RSocket frame emitted by this compatibility layer. */
  readonly maxFrameBytes?: number;
  /** Upper bound for one reassembled application payload (metadata + data). */
  readonly maxItemBytes?: number;
  /** Session-wide upper bound for bytes retained by incomplete fragmented sequences. */
  readonly maxInFlightReassemblyBytes?: number;
  readonly handshakeTimeoutMs?: number;
}

interface ResolvedOptions {
  readonly codec: PayloadCodec;
  readonly keepAliveMs: number;
  readonly lifetimeMs: number;
  readonly maxFrameBytes: number;
  readonly maxItemBytes: number;
  readonly maxInFlightReassemblyBytes: number;
  readonly handshakeTimeoutMs: number;
}

interface FragmentAssembly {
  readonly originalType: RSocketFrameType;
  readonly originalFlags: number;
  readonly initialRequestN?: number;
  readonly metadata: Uint8Array[];
  readonly data: Uint8Array[];
  metadataBytes: number;
  dataBytes: number;
  dataStarted: boolean;
  fragments: number;
}

class RSocketReassemblyResourceError extends PureReactiveProtocolError {
  constructor(readonly streamId: number, message: string) {
    super(message, "RSOCKET_REASSEMBLY_RESOURCE_EXHAUSTED");
    this.name = "RSocketReassemblyResourceError";
  }
}

interface RSocketStreamState {
  readonly id: number;
  wireId?: number;
  readonly attributes: readonly ProtocolAttribute[];
  readonly pattern: RpcPattern;
  readonly initiatedLocally: boolean;
  readonly incoming: AsyncQueue<StreamMessage>;
  readonly abortController: AbortController;
  readonly creditWaiters: Deferred<void>[];
  wireStarted: boolean;
  initialItemSent: boolean;
  pregrantedAppDemand: number;
  pendingInitialResponseDemand: number;
  inboundAllowance: number;
  outboundCredit: number;
  inboundClosed: boolean;
  outboundClosed: boolean;
  cancelled: boolean;
  writeTail: Promise<void>;
}

const resolveOptions = (options: RSocketCompatibilityOptions = {}): ResolvedOptions => {
  const codec = options.codec ?? jsonCodec;
  if (!codec.id) throw new TypeError("RSocket compatibility codec id must not be empty.");
  bytes(codec.id);
  const keepAliveMs = options.keepAliveMs ?? 20_000;
  const lifetimeMs = options.lifetimeMs ?? 90_000;
  const maxFrameBytes = options.maxFrameBytes ?? RSOCKET_MAX_FRAME_BYTES;
  const maxItemBytes = options.maxItemBytes ?? 64 * 1024 * 1024;
  const maxInFlightReassemblyBytes = options.maxInFlightReassemblyBytes ?? 128 * 1024 * 1024;
  const handshakeTimeoutMs = options.handshakeTimeoutMs ?? 30_000;
  for (const [name, value] of Object.entries({ keepAliveMs, lifetimeMs, maxFrameBytes, maxItemBytes, maxInFlightReassemblyBytes, handshakeTimeoutMs })) {
    if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError(`${name} must be a positive safe integer.`);
  }
  if (keepAliveMs > 0x7fffffff || lifetimeMs > 0x7fffffff) throw new RangeError("RSocket keepalive/lifetime must fit unsigned 31 bits.");
  if (lifetimeMs <= keepAliveMs) throw new RangeError("RSocket lifetimeMs must be greater than keepAliveMs.");
  if (maxFrameBytes < 64 || maxFrameBytes > RSOCKET_MAX_FRAME_BYTES) throw new RangeError(`maxFrameBytes must be between 64 and ${RSOCKET_MAX_FRAME_BYTES}.`);
  if (maxInFlightReassemblyBytes < maxFrameBytes) throw new RangeError("maxInFlightReassemblyBytes must be at least maxFrameBytes.");
  return Object.freeze({ codec, keepAliveMs, lifetimeMs, maxFrameBytes, maxItemBytes, maxInFlightReassemblyBytes, handshakeTimeoutMs });
};

const randomId = (): string => {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") return crypto.randomUUID();
  return `rsocket-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
};

const snapshotAttributes = (attributes: readonly ProtocolAttribute[]): readonly ProtocolAttribute[] =>
  attributes.map((item) => ({ id: item.id, value: item.value.slice(), ...(item.required === undefined ? {} : { required: item.required }) }));

const concat = (parts: readonly Uint8Array[], total: number): Uint8Array => {
  const output = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) { output.set(part, offset); offset += part.byteLength; }
  return output;
};

const boundedText = (value: unknown): Uint8Array => {
  const message = value instanceof Error ? value.message : String(value);
  const output = new Uint8Array(MAX_ERROR_BYTES);
  const { written } = encoder.encodeInto(message, output);
  return output.slice(0, written);
};

const frameError = (frame: RSocketFrame): PureReactiveProtocolError => {
  let message = `Remote RSocket ERROR 0x${(frame.errorCode ?? 0).toString(16)}.`;
  if (frame.data?.byteLength) {
    try { message = decoder.decode(frame.data); } catch { /* diagnostic only */ }
  }
  return new PureReactiveProtocolError(message, `RSOCKET_ERROR_${(frame.errorCode ?? 0).toString(16)}`);
};

const patternFromAttributes = (attributes: readonly ProtocolAttribute[]): { readonly target: string; readonly pattern: RpcPattern } => {
  const read = (id: string): string => {
    const values = attributes.filter((item) => item.id === id);
    if (values.length !== 1) throw new PureReactiveProtocolError(`RSocket compatibility requires exactly one ${id} attribute.`, "RSOCKET_RPC_OPEN_INVALID");
    try { return strictText(values[0]!.value); }
    catch (cause) { throw new PureReactiveProtocolError(`${id} is not valid UTF-8.`, "RSOCKET_RPC_OPEN_INVALID", { cause }); }
  };
  const profile = read(RPC_PROFILE_ATTRIBUTE);
  const target = read(RPC_TARGET_ATTRIBUTE);
  const pattern = read(RPC_PATTERN_ATTRIBUTE) as RpcPattern;
  if (profile !== RPC_PROFILE_ID || !target || !( ["unary", "notify", "server-stream", "duplex"] as const).includes(pattern)) {
    throw new PureReactiveProtocolError("RSocket compatibility currently maps the PRP RPC/1 profile only.", "RSOCKET_PROFILE_UNSUPPORTED");
  }
  return { target, pattern };
};

const attributesForRequest = (target: string, pattern: RpcPattern): readonly ProtocolAttribute[] => [
  attribute(RPC_PROFILE_ATTRIBUTE, RPC_PROFILE_ID, { required: true }),
  attribute(RPC_TARGET_ATTRIBUTE, target, { required: true }),
  attribute(RPC_PATTERN_ATTRIBUTE, pattern, { required: true })
];

const requestTypeFor = (pattern: RpcPattern): RSocketFrameType => {
  switch (pattern) {
    case "unary": return RSocketFrameType.REQUEST_RESPONSE;
    case "notify": return RSocketFrameType.REQUEST_FNF;
    case "server-stream": return RSocketFrameType.REQUEST_STREAM;
    case "duplex": return RSocketFrameType.REQUEST_CHANNEL;
  }
};

const patternForRequestType = (type: RSocketFrameType): RpcPattern | undefined => {
  switch (type) {
    case RSocketFrameType.REQUEST_RESPONSE: return "unary";
    case RSocketFrameType.REQUEST_FNF: return "notify";
    case RSocketFrameType.REQUEST_STREAM: return "server-stream";
    case RSocketFrameType.REQUEST_CHANNEL: return "duplex";
    default: return undefined;
  }
};

const payloadSize = (frame: RSocketFrame): number => (frame.metadata?.byteLength ?? 0) + (frame.data?.byteLength ?? 0);

/**
 * Splits request/PAYLOAD logical items according to RSocket's Follows rule. Metadata is always emitted before data.
 * Every PAYLOAD fragment carries NEXT as required by RSocket 1.0; Follows keeps fragments inside reassembly.
 * COMPLETE is preserved only on the final fragment when the original logical frame completed its sending direction.
 */
function frameEncodedBytes(frame: RSocketFrame): number {
  let extra = 0;
  if (frame.type === RSocketFrameType.REQUEST_STREAM || frame.type === RSocketFrameType.REQUEST_CHANNEL) extra += 4;
  const metadataBytes = frame.metadata === undefined ? 0 : 3 + frame.metadata.byteLength;
  return 6 + extra + metadataBytes + (frame.data?.byteLength ?? 0);
}

function* fragmentFrame(frame: RSocketFrame, maxFrameBytes: number): Generator<RSocketFrame> {
  if (frameEncodedBytes(frame) <= maxFrameBytes) { yield frame; return; }
  if (![RSocketFrameType.REQUEST_RESPONSE, RSocketFrameType.REQUEST_FNF, RSocketFrameType.REQUEST_STREAM, RSocketFrameType.REQUEST_CHANNEL, RSocketFrameType.PAYLOAD].includes(frame.type)) {
    throw new PureReactiveProtocolError(`RSocket ${RSocketFrameType[frame.type]} frame exceeds ${maxFrameBytes} bytes and is not fragmentable.`, "RSOCKET_FRAME_TOO_LARGE");
  }

  const metadata = frame.metadata ?? new Uint8Array(0);
  const data = frame.data ?? new Uint8Array(0);
  let metadataOffset = 0;
  let dataOffset = 0;
  let first = true;
  const originalSemanticFlags = (frame.flags ?? 0) & (RSOCKET_FLAG_NEXT | RSOCKET_FLAG_COMPLETE);

  while (metadataOffset < metadata.byteLength || dataOffset < data.byteLength || first) {
    const type = first ? frame.type : RSocketFrameType.PAYLOAD;
    const typeExtra = first && (frame.type === RSocketFrameType.REQUEST_STREAM || frame.type === RSocketFrameType.REQUEST_CHANNEL) ? 4 : 0;
    let capacity = maxFrameBytes - 6 - typeExtra;
    if (capacity <= 0) throw new RangeError("RSocket frame limit leaves no payload space.");

    let metadataChunk: Uint8Array | undefined;
    let dataChunk: Uint8Array | undefined;
    if (metadataOffset < metadata.byteLength) {
      if (capacity <= 3) throw new RangeError("RSocket frame limit is too small for metadata fragmentation.");
      const take = Math.min(metadata.byteLength - metadataOffset, capacity - 3);
      metadataChunk = metadata.subarray(metadataOffset, metadataOffset + take);
      metadataOffset += take;
      capacity -= 3 + take;
    }
    if (metadataOffset >= metadata.byteLength && capacity > 0 && dataOffset < data.byteLength) {
      const take = Math.min(data.byteLength - dataOffset, capacity);
      dataChunk = data.subarray(dataOffset, dataOffset + take);
      dataOffset += take;
    }

    const more = metadataOffset < metadata.byteLength || dataOffset < data.byteLength;
    let flags = more ? RSOCKET_FLAG_FOLLOWS : 0;
    if (type === RSocketFrameType.PAYLOAD) flags |= RSOCKET_FLAG_NEXT; if (!more) {
      if (frame.type === RSocketFrameType.PAYLOAD && (originalSemanticFlags & RSOCKET_FLAG_COMPLETE) !== 0) flags |= RSOCKET_FLAG_COMPLETE;
      else if (frame.type === RSocketFrameType.REQUEST_CHANNEL && (originalSemanticFlags & RSOCKET_FLAG_COMPLETE) !== 0) flags |= RSOCKET_FLAG_COMPLETE;
    }
    const fragment: RSocketFrame = {
      type,
      streamId: frame.streamId,
      flags,
      ...(first && frame.initialRequestN !== undefined ? { initialRequestN: frame.initialRequestN } : {}),
      ...(metadataChunk === undefined ? {} : { metadata: metadataChunk }),
      ...(dataChunk === undefined ? {} : { data: dataChunk })
    };
    // Only the current fragment is encoded. A multi-megabyte logical item is never materialized
    // as one encoded RSocket frame before fragmentation.
    if (encodeRSocketFrame(fragment).byteLength > maxFrameBytes) throw new RangeError("RSocket fragment exceeded configured maxFrameBytes.");
    yield fragment;
    first = false;
    if (!more) return;
  }
}

class RSocketReactiveStream implements ReactiveStream {
  constructor(private readonly session: RSocketCompatibilitySession, readonly streamState: RSocketStreamState) {}
  get id(): bigint { return BigInt(this.streamState.id); }
  get attributes(): readonly ProtocolAttribute[] { return this.streamState.attributes; }
  get signal(): AbortSignal { return this.streamState.abortController.signal; }
  get closed(): boolean { return this.streamState.cancelled || (this.streamState.inboundClosed && this.streamState.outboundClosed); }

  request(count = 1): Promise<void> { return this.session.request(this.streamState, count); }
  send(data = new Uint8Array(0), attributes: readonly ProtocolAttribute[] = []): Promise<void> {
    const payload = data.slice();
    const attrs = snapshotAttributes(attributes);
    const operation = this.streamState.writeTail.then(() => this.session.send(this.streamState, payload, attrs));
    this.streamState.writeTail = operation.catch(() => {});
    return operation;
  }
  complete(): Promise<void> { return this.session.complete(this.streamState); }
  cancel(reason: string | Error = "cancelled"): Promise<void> { return this.session.cancel(this.streamState, reason); }
  fail(error: unknown): Promise<void> { return this.session.fail(this.streamState, error); }

  [Symbol.asyncIterator](): AsyncIterator<StreamMessage> {
    let returned = false;
    return {
      next: async () => {
        if (returned) return { value: undefined as never, done: true };
        await this.request(1);
        return this.streamState.incoming.next();
      },
      return: async () => {
        returned = true;
        if (!this.closed) await this.cancel("RSocket compatibility iterator stopped before stream completion.");
        return { value: undefined as never, done: true };
      }
    };
  }
}

export class RSocketCompatibilitySession implements ReactiveSession {
  private readonly incomingStreams = new AsyncQueue<ReactiveStream>();
  private readonly incomingSignals = new AsyncQueue<SessionSignal>();
  private readonly incomingDatagrams = new AsyncQueue<SessionDatagram>();
  private readonly acceptors = new Set<StreamAcceptor>();
  private readonly signalAcceptors = new Set<SignalAcceptor>();
  private readonly datagramAcceptors = new Set<DatagramAcceptor>();
  private readonly listeners = new Set<(state: SessionState) => void>();
  private readonly streams = new Map<number, RSocketStreamState>();
  private readonly pendingLocalStreams = new Set<RSocketStreamState>();
  private readonly fragments = new Map<number, FragmentAssembly>();
  private readonly options: ResolvedOptions;
  private readonly logicalSessionId = randomId();
  private readonly capabilitiesValue: CapabilitySet;
  private connection: TransportConnection | undefined;
  private lane: TransportLane | undefined;
  private origin: RSocketOrigin | undefined;
  private sessionState: SessionState = "idle";
  private nextLogicalStreamId = 0;
  private nextWireStreamId = 0;
  private highestRemoteStreamId = 0;
  private writeTail: Promise<void> = Promise.resolve();
  private startTail: Promise<void> = Promise.resolve();
  private ready = deferred<void>();
  private setup: RSocketSetupFields | undefined;
  private profileDispose: (() => void) | undefined;
  private keepAliveTimer: ReturnType<typeof setTimeout> | undefined;
  private lastInboundAt = Date.now();
  private inFlightReassemblyBytes = 0;
  private inFlightFragmentCount = 0;
  private closedByPeer = false;

  readonly [SESSION_INTEGRATION]: SessionIntegration = {
    registerAcceptor: (acceptor) => { this.acceptors.add(acceptor); return () => this.acceptors.delete(acceptor); },
    registerSignalAcceptor: (acceptor) => { this.signalAcceptors.add(acceptor); return () => this.signalAcceptors.delete(acceptor); },
    registerDatagramAcceptor: (acceptor) => { this.datagramAcceptors.add(acceptor); return () => this.datagramAcceptors.delete(acceptor); },
    capabilities: () => this.capabilitiesValue
  };

  constructor(options: RSocketCompatibilityOptions = {}) {
    this.options = resolveOptions(options);
    const rpc: CapabilityDescriptor = Object.freeze({ id: RPC_CAPABILITY_ID, minVersion: 1, maxVersion: 1, parameters: bytes(this.options.codec.id) });
    const compat: CapabilityDescriptor = Object.freeze({ id: RSOCKET_COMPATIBILITY_CAPABILITY_ID, minVersion: 1, maxVersion: 1 });
    const values: NegotiatedCapability[] = [rpc, compat].map((descriptor) => ({ id: descriptor.id, version: 1, local: descriptor, remote: descriptor }));
    this.capabilitiesValue = new CapabilitySet(values);
    this.incomingDatagrams.end();
  }

  get state(): SessionState { return this.sessionState; }
  get sessionId(): string { return this.logicalSessionId; }
  get signals(): AsyncIterable<SessionSignal> { return this.incomingSignals; }
  get datagrams(): AsyncIterable<SessionDatagram> { return this.incomingDatagrams; }
  get maxDatagramBytes(): number { return 0; }
  supports(capabilityId: string): boolean { return this.capabilitiesValue.has(capabilityId); }
  onStateChange(listener: (state: SessionState) => void): () => void { this.listeners.add(listener); return () => this.listeners.delete(listener); }
  [Symbol.asyncIterator](): AsyncIterator<ReactiveStream> { return this.incomingStreams[Symbol.asyncIterator](); }

  async attach(transport: ReactiveTransport, origin: RSocketOrigin, signal?: AbortSignal): Promise<this> {
    if (this.sessionState !== "idle") throw new PureReactiveProtocolError("RSocket compatibility session is one-shot.", "RSOCKET_SESSION_ALREADY_USED");
    this.origin = origin;
    this.nextLogicalStreamId = origin === "initiator" ? 1 : 2;
    this.nextWireStreamId = origin === "initiator" ? 1 : 2;
    this.highestRemoteStreamId = 0;
    this.startTail = Promise.resolve();
    this.lastInboundAt = Date.now();
    this.transition("connecting");
    this.ready = deferred<void>();
    void this.ready.promise.catch(() => {});
    try {
      this.connection = await transport.connect(signal);
      this.lane = await this.connection.openLane({ ...RELIABLE_ORDERED_LANE, maxFrameBytes: this.options.maxFrameBytes });
      void this.connection.closed.catch(() => {});
      void this.readLoop(this.lane);
      if (origin === "initiator") {
        this.setup = {
          ...defaultRSocketSetup(),
          keepAliveMs: this.options.keepAliveMs,
          lifetimeMs: this.options.lifetimeMs,
          metadataMimeType: RSOCKET_COMPOSITE_METADATA_MIME,
          dataMimeType: this.options.codec.id
        };
        await this.write({ type: RSocketFrameType.SETUP, streamId: 0, setup: this.setup });
        await this.activateProfile();
        this.transition("ready");
        this.ready.resolve();
        this.startKeepAlive();
      }
      if (origin === "acceptor") {
        let timer: ReturnType<typeof setTimeout> | undefined;
        const timeout = new Promise<never>((_resolve, reject) => {
          timer = setTimeout(() => reject(new PureReactiveProtocolError("RSocket SETUP handshake timed out.", "RSOCKET_HANDSHAKE_TIMEOUT")), this.options.handshakeTimeoutMs);
          (timer as unknown as { unref?: () => void }).unref?.();
        });
        try { await Promise.race([this.ready.promise, timeout]); }
        finally { if (timer) clearTimeout(timer); }
      }
      return this;
    } catch (error) {
      this.detach(error);
      throw error;
    }
  }

  async open(attributes: readonly ProtocolAttribute[] = []): Promise<ReactiveStream> {
    this.assertReady();
    const { pattern } = patternFromAttributes(attributes);
    const id = this.nextLogicalStreamId;
    this.nextLogicalStreamId += 2;
    const state = this.createState(id, snapshotAttributes(attributes), pattern, true);
    this.pendingLocalStreams.add(state);
    return new RSocketReactiveStream(this, state);
  }

  async sendDatagram(_data: Uint8Array): Promise<void> {
    throw new PureReactiveProtocolError("Native PRP datagrams are not available through RSocket compatibility sessions.", "DATAGRAM_UNAVAILABLE");
  }

  async signal(attributes: readonly ProtocolAttribute[] = [], payload?: Uint8Array): Promise<void> {
    this.assertReady();
    if (attributes.length) throw new PureReactiveProtocolError("RSocket METADATA_PUSH has no generic mapping for PRP signal attributes.", "RSOCKET_SIGNAL_ATTRIBUTES_UNSUPPORTED");
    const metadata = payload?.slice() ?? new Uint8Array(0);
    await this.write({ type: RSocketFrameType.METADATA_PUSH, streamId: 0, metadata });
  }

  async close(reason = "closed"): Promise<void> {
    if (this.sessionState === "closed") return;
    if (this.sessionState === "ready" && !this.closedByPeer) {
      await this.write({ type: RSocketFrameType.ERROR, streamId: 0, errorCode: RSocketErrorCode.CONNECTION_CLOSE, data: boundedText(reason) }).catch(() => {});
    }
    await this.finishClosed(reason);
  }

  async request(state: RSocketStreamState, count: number): Promise<void> {
    if (!Number.isInteger(count) || count <= 0 || count > RSOCKET_MAX_REQUEST_N) throw new RangeError(`RSocket demand must be 1..${RSOCKET_MAX_REQUEST_N}.`);
    if (state.cancelled) throw new StreamClosedError();
    if (state.inboundClosed) return;
    let remaining = count;
    if (state.pregrantedAppDemand > 0) {
      const use = Math.min(remaining, state.pregrantedAppDemand);
      state.pregrantedAppDemand -= use;
      remaining -= use;
    }
    if (remaining === 0) return;
    if (!this.directionSupportsRequestN(state)) return;
    if (state.initiatedLocally && !state.wireStarted) {
      state.pendingInitialResponseDemand = Math.min(RSOCKET_MAX_REQUEST_N - 1, state.pendingInitialResponseDemand + remaining);
      return;
    }
    state.inboundAllowance = Math.min(RSOCKET_MAX_REQUEST_N, state.inboundAllowance + remaining);
    await this.write({ type: RSocketFrameType.REQUEST_N, streamId: this.wireStreamId(state), requestN: remaining });
  }

  async send(state: RSocketStreamState, data: Uint8Array, attributes: readonly ProtocolAttribute[]): Promise<void> {
    this.assertStreamOpen(state);
    if (attributes.length) throw new PureReactiveProtocolError("RSocket RPC/1 compatibility does not map DATA attributes.", "RSOCKET_DATA_ATTRIBUTES_UNSUPPORTED");
    if (data.byteLength > this.options.maxItemBytes) throw new PureReactiveProtocolError(`RSocket logical payload exceeds ${this.options.maxItemBytes} bytes.`, "ITEM_TOO_LARGE");

    if (state.initiatedLocally && !state.wireStarted) {
      await this.sendInitialRequest(state, data, false);
      return;
    }
    if (state.initiatedLocally && state.pattern !== "duplex") throw new StreamClosedError("RSocket request input is a single item for this interaction model.");
    await this.waitOutboundCredit(state);
    state.outboundCredit -= 1;
    if (!state.initiatedLocally && state.pattern === "unary") {
      await this.writeLogical({ type: RSocketFrameType.PAYLOAD, streamId: this.wireStreamId(state), flags: RSOCKET_FLAG_NEXT | RSOCKET_FLAG_COMPLETE, data });
      state.outboundClosed = true;
      this.maybeRetire(state);
      return;
    }
    await this.writeLogical({ type: RSocketFrameType.PAYLOAD, streamId: this.wireStreamId(state), flags: RSOCKET_FLAG_NEXT, data });
  }

  async complete(state: RSocketStreamState): Promise<void> {
    if (state.cancelled) return;
    if (state.outboundClosed) { this.maybeRetire(state); return; }
    if (state.initiatedLocally && !state.wireStarted) {
      if (state.pattern === "duplex") throw new PureReactiveProtocolError("RSocket REQUEST_CHANNEL requires an initial payload carrying routing metadata.", "RSOCKET_CHANNEL_INITIAL_ITEM_REQUIRED");
      throw new PureReactiveProtocolError("RSocket request must send its request payload before completion.", "RSOCKET_REQUEST_ITEM_REQUIRED");
    }
    if (state.initiatedLocally && state.pattern !== "duplex") {
      state.outboundClosed = true;
      this.maybeRetire(state);
      return;
    }
    if (!state.initiatedLocally && (state.pattern === "notify" || state.pattern === "unary")) {
      state.outboundClosed = true;
      this.maybeRetire(state);
      return;
    }
    await this.write({ type: RSocketFrameType.PAYLOAD, streamId: this.wireStreamId(state), flags: RSOCKET_FLAG_COMPLETE });
    state.outboundClosed = true;
    this.maybeRetire(state);
  }

  async cancel(state: RSocketStreamState, reason: string | Error): Promise<void> {
    if (state.cancelled) return;
    state.cancelled = true;
    if (state.initiatedLocally && state.wireStarted) await this.write({ type: RSocketFrameType.CANCEL, streamId: this.wireStreamId(state) }).catch(() => {});
    else if (!state.initiatedLocally && state.wireStarted) await this.write({ type: RSocketFrameType.ERROR, streamId: this.wireStreamId(state), errorCode: RSocketErrorCode.CANCELED, data: boundedText(reason) }).catch(() => {});
    this.terminate(state, reason instanceof Error ? reason : new StreamClosedError(reason));
  }

  async fail(state: RSocketStreamState, error: unknown): Promise<void> {
    if (state.cancelled) return;
    state.cancelled = true;
    if (state.wireStarted) await this.write({ type: RSocketFrameType.ERROR, streamId: this.wireStreamId(state), errorCode: RSocketErrorCode.APPLICATION_ERROR, data: boundedText(error) }).catch(() => {});
    this.terminate(state, error instanceof Error ? error : new Error(String(error)));
  }

  private sendInitialRequest(state: RSocketStreamState, data: Uint8Array, completeChannel: boolean): Promise<void> {
    const operation = this.startTail.then(() => this.sendInitialRequestNow(state, data, completeChannel));
    this.startTail = operation.then(() => {}, () => {});
    return operation;
  }

  private async sendInitialRequestNow(state: RSocketStreamState, data: Uint8Array, completeChannel: boolean): Promise<void> {
    if (state.wireStarted) throw new PureReactiveProtocolError("RSocket request already started on the wire.", "RSOCKET_STREAM_ALREADY_STARTED");
    if (this.nextWireStreamId > RSOCKET_MAX_STREAM_ID) {
      throw new PureReactiveProtocolError("RSocket stream id space exhausted; open a new connection.", "RSOCKET_STREAM_ID_EXHAUSTED");
    }
    const { target } = patternFromAttributes(state.attributes);
    const metadata = this.encodeRoute(target);
    if (metadata.byteLength + data.byteLength > this.options.maxItemBytes) {
      throw new PureReactiveProtocolError(`RSocket logical payload exceeds ${this.options.maxItemBytes} bytes.`, "ITEM_TOO_LARGE");
    }
    const type = requestTypeFor(state.pattern);
    const initialRequestN = state.pattern === "server-stream" || state.pattern === "duplex"
      ? Math.min(RSOCKET_MAX_REQUEST_N, 1 + state.pendingInitialResponseDemand)
      : undefined;
    const flags = state.pattern === "duplex" && completeChannel ? RSOCKET_FLAG_COMPLETE : 0;
    const wireId = this.nextWireStreamId;

    // Make the state visible before the first bytes can trigger an immediate peer response,
    // but commit the next wire id only after the whole initial logical request was written.
    state.wireId = wireId;
    state.wireStarted = true;
    state.initialItemSent = true;
    if (state.pattern === "unary") {
      state.outboundClosed = true;
      state.inboundAllowance = 1;
    } else if (state.pattern === "notify") {
      state.outboundClosed = true;
      state.inboundClosed = true;
    } else if (state.pattern === "server-stream") {
      state.outboundClosed = true;
      state.inboundAllowance = initialRequestN ?? 1;
    } else {
      state.inboundAllowance = initialRequestN ?? 1;
    }
    this.streams.set(wireId, state);

    try {
      await this.writeLogical({ type, streamId: wireId, flags, ...(initialRequestN === undefined ? {} : { initialRequestN }), metadata, data });
      this.nextWireStreamId += 2;
      this.pendingLocalStreams.delete(state);
      state.pendingInitialResponseDemand = 0;
      this.maybeRetire(state);
    } catch (error) {
      if (this.streams.get(wireId) === state) this.streams.delete(wireId);
      delete state.wireId;
      state.wireStarted = false;
      state.initialItemSent = false;
      state.inboundAllowance = 0;
      state.inboundClosed = false;
      state.outboundClosed = false;
      throw error;
    }
  }

  private wireStreamId(state: RSocketStreamState): number {
    if (state.wireId === undefined) throw new PureReactiveProtocolError("RSocket stream has not been committed to the wire yet.", "RSOCKET_STREAM_NOT_STARTED");
    return state.wireId;
  }

  private async waitOutboundCredit(state: RSocketStreamState): Promise<void> {
    if (!this.sendNeedsCredit(state)) return;
    while (state.outboundCredit <= 0) {
      if (state.cancelled || state.outboundClosed) throw new StreamClosedError();
      const waiter = deferred<void>();
      state.creditWaiters.push(waiter);
      await waiter.promise;
    }
  }

  private sendNeedsCredit(state: RSocketStreamState): boolean {
    if (state.initiatedLocally) return state.pattern === "duplex" && state.wireStarted;
    return state.pattern === "server-stream" || state.pattern === "duplex";
  }

  private directionSupportsRequestN(state: RSocketStreamState): boolean {
    if (state.initiatedLocally) return state.pattern === "server-stream" || state.pattern === "duplex";
    return state.pattern === "duplex";
  }

  private createState(id: number, attributes: readonly ProtocolAttribute[], pattern: RpcPattern, initiatedLocally: boolean, wireId?: number): RSocketStreamState {
    return {
      id,
      ...(wireId === undefined ? {} : { wireId }),
      attributes,
      pattern,
      initiatedLocally,
      incoming: new AsyncQueue<StreamMessage>(),
      abortController: new AbortController(),
      creditWaiters: [],
      wireStarted: false,
      initialItemSent: false,
      pregrantedAppDemand: initiatedLocally && pattern !== "notify" ? 1 : 0,
      pendingInitialResponseDemand: 0,
      inboundAllowance: 0,
      outboundCredit: 0,
      inboundClosed: false,
      outboundClosed: false,
      cancelled: false,
      writeTail: Promise.resolve()
    };
  }

  private async readLoop(lane: TransportLane): Promise<void> {
    try {
      for await (const raw of lane.incoming) {
        this.lastInboundAt = Date.now();
        const frame = decodeRSocketFrame(raw);
        await this.handleRawFrame(frame);
      }
      if (this.sessionState !== "closed") this.detach(new ConnectionLostError("RSocket transport ended without CONNECTION_CLOSE."));
    } catch (error) {
      if (this.sessionState !== "closed") this.detach(error);
    }
  }

  private async handleRawFrame(frame: RSocketFrame): Promise<void> {
    if (this.origin === "acceptor" && this.sessionState === "connecting") {
      if (frame.type !== RSocketFrameType.SETUP || frame.streamId !== 0 || !frame.setup) {
        await this.write({ type: RSocketFrameType.ERROR, streamId: 0, errorCode: RSocketErrorCode.INVALID_SETUP, data: boundedText("Expected SETUP as first RSocket frame.") }).catch(() => {});
        throw new RSocketProtocolError("Expected SETUP as first RSocket frame.");
      }
      await this.acceptSetup(frame);
      return;
    }
    if (frame.type === RSocketFrameType.SETUP) return; // spec says ignore additional SETUP after establishment
    if (this.sessionState !== "ready") throw new RSocketProtocolError("RSocket frame received before setup completed.");

    if (frame.type === RSocketFrameType.KEEPALIVE) {
      if ((frame.flags ?? 0) & RSOCKET_FLAG_RESPOND) await this.write({ type: RSocketFrameType.KEEPALIVE, streamId: 0, lastReceivedPosition: frame.lastReceivedPosition ?? 0n, flags: 0, ...(frame.data === undefined ? {} : { data: frame.data }) });
      return;
    }
    if (frame.type === RSocketFrameType.ERROR && frame.streamId === 0) {
      if (frame.errorCode === RSocketErrorCode.CONNECTION_CLOSE) {
        this.closedByPeer = true;
        await this.finishClosed("Remote RSocket connection close.");
        return;
      }
      throw frameError(frame);
    }
    if (frame.type === RSocketFrameType.METADATA_PUSH) {
      const signal: SessionSignal = { data: frame.metadata?.slice() ?? new Uint8Array(0), attributes: [] };
      for (const acceptor of this.signalAcceptors) {
        if (!acceptor.accepts(signal)) continue;
        void Promise.resolve().then(() => acceptor.handle(signal)).catch((error) => this.detach(error));
        return;
      }
      this.incomingSignals.push(signal);
      return;
    }

    const directPattern = patternForRequestType(frame.type);
    if (directPattern !== undefined) this.acceptRemoteRequestId(frame.streamId);
    const relevantStreamFrame = directPattern !== undefined ||
      frame.type === RSocketFrameType.REQUEST_N ||
      frame.type === RSocketFrameType.CANCEL ||
      frame.type === RSocketFrameType.PAYLOAD ||
      frame.type === RSocketFrameType.ERROR;
    // Unknown frames with IGNORE set are decoded as opaque frames and stop here.
    if (!relevantStreamFrame) return;

    let reconstructed: RSocketFrame | undefined;
    try {
      reconstructed = this.reassemble(frame);
    } catch (error) {
      if (error instanceof RSocketReassemblyResourceError) {
        const state = this.streams.get(error.streamId);
        if (state) this.terminate(state, error);
        await this.write({
          type: RSocketFrameType.ERROR,
          streamId: error.streamId,
          errorCode: RSocketErrorCode.REJECTED,
          data: boundedText(error)
        }).catch(() => {});
        return;
      }
      throw error;
    }
    if (!reconstructed) return;
    const pattern = patternForRequestType(reconstructed.type);
    if ((pattern !== undefined || reconstructed.type === RSocketFrameType.PAYLOAD) && payloadSize(reconstructed) > this.options.maxItemBytes) {
      throw new RSocketProtocolError(`RSocket logical payload exceeds ${this.options.maxItemBytes} bytes.`);
    }
    if (pattern) { await this.handleRequest(reconstructed, pattern); return; }
    const state = this.streams.get(reconstructed.streamId);
    if (!state) return; // RSocket's handling-unexpected rules are deliberately lenient for unknown streams.
    switch (reconstructed.type) {
      case RSocketFrameType.REQUEST_N:
        if (reconstructed.requestN) {
          state.outboundCredit = Math.min(RSOCKET_MAX_REQUEST_N, state.outboundCredit + reconstructed.requestN);
          for (const waiter of state.creditWaiters.splice(0)) waiter.resolve();
        }
        return;
      case RSocketFrameType.CANCEL:
        this.terminate(state, new StreamClosedError("Remote RSocket requester cancelled the stream."));
        return;
      case RSocketFrameType.ERROR:
        this.terminate(state, frameError(reconstructed));
        return;
      case RSocketFrameType.PAYLOAD:
        await this.handlePayload(state, reconstructed);
        return;
      default:
        return;
    }
  }

  private async acceptSetup(frame: RSocketFrame): Promise<void> {
    const setup = frame.setup!;
    if (setup.major !== 1 || setup.minor !== 0) {
      await this.write({ type: RSocketFrameType.ERROR, streamId: 0, errorCode: RSocketErrorCode.UNSUPPORTED_SETUP, data: boundedText(`Unsupported RSocket version ${setup.major}.${setup.minor}.`) }).catch(() => {});
      throw new RSocketProtocolError(`Unsupported RSocket version ${setup.major}.${setup.minor}.`);
    }
    if (setup.resumeToken) {
      await this.write({ type: RSocketFrameType.ERROR, streamId: 0, errorCode: RSocketErrorCode.UNSUPPORTED_SETUP, data: boundedText("RSocket resume is not advertised by this compatibility profile.") }).catch(() => {});
      throw new RSocketProtocolError("RSocket resume is unsupported by this compatibility profile.");
    }
    if (((frame.flags ?? 0) & RSOCKET_FLAG_LEASE) !== 0) {
      await this.write({ type: RSocketFrameType.ERROR, streamId: 0, errorCode: RSocketErrorCode.UNSUPPORTED_SETUP, data: boundedText("RSocket LEASE is not advertised by this compatibility profile.") }).catch(() => {});
      throw new RSocketProtocolError("RSocket LEASE is unsupported by this compatibility profile.");
    }
    if (setup.metadataMimeType !== RSOCKET_COMPOSITE_METADATA_MIME && setup.metadataMimeType !== RSOCKET_ROUTING_MIME) {
      await this.write({ type: RSocketFrameType.ERROR, streamId: 0, errorCode: RSocketErrorCode.UNSUPPORTED_SETUP, data: boundedText(`Unsupported RSocket metadata MIME ${setup.metadataMimeType}.`) }).catch(() => {});
      throw new RSocketProtocolError(`Unsupported RSocket metadata MIME ${setup.metadataMimeType}.`);
    }
    if (setup.dataMimeType !== this.options.codec.id) {
      await this.write({ type: RSocketFrameType.ERROR, streamId: 0, errorCode: RSocketErrorCode.UNSUPPORTED_SETUP, data: boundedText(`RSocket data MIME mismatch: expected ${this.options.codec.id}, received ${setup.dataMimeType}.`) }).catch(() => {});
      throw new CapabilityMismatchError(`RSocket data MIME mismatch: expected ${this.options.codec.id}, received ${setup.dataMimeType}.`);
    }
    this.setup = setup;
    await this.activateProfile();
    this.transition("ready");
    this.ready.resolve();
    this.startKeepAlive();
  }

  private async activateProfile(): Promise<void> {
    if (this.profileDispose) return;
    this.profileDispose = attachRpcProfile(this, this.options.codec);
  }

  private acceptRemoteRequestId(streamId: number): void {
    const expectedRemoteStreamId = this.highestRemoteStreamId === 0
      ? (this.origin === "initiator" ? 2 : 1)
      : this.highestRemoteStreamId + 2;
    if (streamId !== expectedRemoteStreamId) {
      throw new RSocketProtocolError(`Remote requester must allocate stream ids sequentially by +2; expected ${expectedRemoteStreamId}, received ${streamId}.`);
    }
    this.highestRemoteStreamId = streamId;
  }

  private async handleRequest(frame: RSocketFrame, pattern: RpcPattern): Promise<void> {
    const target = this.decodeRoute(frame.metadata);
    if (!target) {
      await this.write({ type: RSocketFrameType.ERROR, streamId: frame.streamId, errorCode: RSocketErrorCode.INVALID, data: boundedText("RSocket request is missing routing metadata.") });
      return;
    }
    const state = this.createState(frame.streamId, attributesForRequest(target, pattern), pattern, false, frame.streamId);
    state.wireStarted = true;
    state.initialItemSent = true;
    state.pregrantedAppDemand = 1;
    if (pattern === "unary") {
      state.outboundCredit = 1;
      state.inboundClosed = true;
    } else if (pattern === "notify") {
      state.outboundClosed = true;
      state.inboundClosed = true;
    } else if (pattern === "server-stream") {
      state.outboundCredit = frame.initialRequestN ?? 0;
      state.inboundClosed = true;
    } else {
      state.outboundCredit = frame.initialRequestN ?? 0;
      if ((frame.flags ?? 0) & RSOCKET_FLAG_COMPLETE) state.inboundClosed = true;
    }
    state.incoming.push({ data: frame.data?.slice() ?? new Uint8Array(0), attributes: [] });
    if (state.inboundClosed) state.incoming.end();
    this.streams.set(frame.streamId, state);
    const stream = new RSocketReactiveStream(this, state);
    for (const acceptor of this.acceptors) {
      let accepted = false;
      try { accepted = acceptor.accepts(stream); } catch (error) { await stream.fail(error); return; }
      if (!accepted) continue;
      void Promise.resolve().then(() => acceptor.handle(stream)).catch(async (error) => { if (!stream.closed) await stream.fail(error).catch(() => {}); });
      return;
    }
    this.incomingStreams.push(stream);
  }

  private async handlePayload(state: RSocketStreamState, frame: RSocketFrame): Promise<void> {
    const flags = frame.flags ?? 0;
    if ((flags & RSOCKET_FLAG_NEXT) !== 0) {
      if (state.inboundAllowance <= 0 && !(state.initiatedLocally && state.pattern === "unary")) {
        throw new RSocketProtocolError(`Peer sent PAYLOAD without demand on stream ${state.id}.`);
      }
      if (state.inboundAllowance > 0) state.inboundAllowance -= 1;
      state.incoming.push({ data: frame.data?.slice() ?? new Uint8Array(0), attributes: [] });
    }
    if ((flags & RSOCKET_FLAG_COMPLETE) !== 0 || (state.initiatedLocally && state.pattern === "unary" && (flags & RSOCKET_FLAG_NEXT) !== 0)) {
      state.inboundClosed = true;
      state.incoming.end();
      this.maybeRetire(state);
    }
  }

  private reassemble(frame: RSocketFrame): RSocketFrame | undefined {
    const existing = this.fragments.get(frame.streamId);
    if (existing && (frame.type === RSocketFrameType.CANCEL || frame.type === RSocketFrameType.ERROR)) {
      this.dropFragmentAssembly(frame.streamId);
      return frame;
    }

    const rawFlags = frame.flags ?? 0;
    const follows = (rawFlags & RSOCKET_FLAG_FOLLOWS) !== 0 &&
      !(frame.type === RSocketFrameType.PAYLOAD && (rawFlags & RSOCKET_FLAG_COMPLETE) !== 0);
    const fragmentable = patternForRequestType(frame.type) !== undefined || frame.type === RSocketFrameType.PAYLOAD;
    if (!existing && !follows) return frame;
    if (!fragmentable && !existing) return frame;

    let assembly = existing;
    if (!assembly) {
      assembly = {
        originalType: frame.type,
        originalFlags: rawFlags & ~RSOCKET_FLAG_FOLLOWS,
        ...(frame.initialRequestN === undefined ? {} : { initialRequestN: frame.initialRequestN }),
        metadata: [], data: [], metadataBytes: 0, dataBytes: 0, dataStarted: false, fragments: 0
      };
      this.fragments.set(frame.streamId, assembly);
    } else if (frame.type !== RSocketFrameType.PAYLOAD) {
      throw new RSocketProtocolError("RSocket fragmented sequence continuation must use PAYLOAD frames, CANCEL, or ERROR.");
    }

    this.appendFragment(assembly, frame);
    if (follows) return undefined;
    this.dropFragmentAssembly(frame.streamId);
    const metadata = assembly.metadataBytes ? concat(assembly.metadata, assembly.metadataBytes) : undefined;
    const data = concat(assembly.data, assembly.dataBytes);
    const finalFlags = (assembly.originalFlags | (rawFlags & (RSOCKET_FLAG_NEXT | RSOCKET_FLAG_COMPLETE))) & ~RSOCKET_FLAG_FOLLOWS;
    return {
      type: assembly.originalType,
      streamId: frame.streamId,
      flags: finalFlags,
      ...(assembly.initialRequestN === undefined ? {} : { initialRequestN: assembly.initialRequestN }),
      ...(metadata === undefined ? {} : { metadata }),
      data
    };
  }

  private appendFragment(assembly: FragmentAssembly, frame: RSocketFrame): void {
    const metadataBytes = frame.metadata?.byteLength ?? 0;
    const dataBytes = frame.data?.byteLength ?? 0;
    const addedBytes = metadataBytes + dataBytes;
    if (assembly.metadataBytes + assembly.dataBytes + addedBytes > this.options.maxItemBytes) {
      this.dropFragmentAssembly(frame.streamId);
      throw new RSocketProtocolError(`RSocket reassembled item exceeds ${this.options.maxItemBytes} bytes.`);
    }
    if (this.inFlightReassemblyBytes + addedBytes > this.options.maxInFlightReassemblyBytes ||
        this.inFlightFragmentCount + 1 > MAX_IN_FLIGHT_RSOCKET_FRAGMENTS) {
      this.dropFragmentAssembly(frame.streamId);
      throw new RSocketReassemblyResourceError(frame.streamId, "RSocket fragmented-sequence reassembly budget is exhausted.");
    }
    if (metadataBytes > 0) {
      if (assembly.dataStarted) throw new RSocketProtocolError("RSocket fragmented metadata appeared after data bytes.");
      assembly.metadata.push(frame.metadata!.slice());
      assembly.metadataBytes += metadataBytes;
    }
    if (frame.data !== undefined) {
      assembly.dataStarted = true;
      assembly.data.push(frame.data.slice());
      assembly.dataBytes += dataBytes;
    }
    assembly.fragments += 1;
    this.inFlightReassemblyBytes += addedBytes;
    this.inFlightFragmentCount += 1;
  }

  private dropFragmentAssembly(streamId: number): FragmentAssembly | undefined {
    const assembly = this.fragments.get(streamId);
    if (!assembly) return undefined;
    this.fragments.delete(streamId);
    this.inFlightReassemblyBytes = Math.max(0, this.inFlightReassemblyBytes - assembly.metadataBytes - assembly.dataBytes);
    this.inFlightFragmentCount = Math.max(0, this.inFlightFragmentCount - assembly.fragments);
    return assembly;
  }

  private clearFragmentAssemblies(): void {
    this.fragments.clear();
    this.inFlightReassemblyBytes = 0;
    this.inFlightFragmentCount = 0;
  }

  private encodeRoute(target: string): Uint8Array {
    const mime = this.setup?.metadataMimeType ?? RSOCKET_COMPOSITE_METADATA_MIME;
    if (mime === RSOCKET_ROUTING_MIME) return encodeRoute(target);
    return encodeRoutingCompositeMetadata(target);
  }

  private decodeRoute(metadata?: Uint8Array): string | undefined {
    const mime = this.setup?.metadataMimeType ?? RSOCKET_COMPOSITE_METADATA_MIME;
    if (!metadata) return undefined;
    if (mime === RSOCKET_ROUTING_MIME) return decodeRoutes(metadata)[0];
    return firstRouteFromCompositeMetadata(metadata);
  }

  private writeLogical(frame: RSocketFrame): Promise<void> {
    const fragmentable = patternForRequestType(frame.type) !== undefined || frame.type === RSocketFrameType.PAYLOAD;
    if (fragmentable && payloadSize(frame) > this.options.maxItemBytes) {
      return Promise.reject(new PureReactiveProtocolError(`RSocket logical payload exceeds ${this.options.maxItemBytes} bytes.`, "ITEM_TOO_LARGE"));
    }
    const operation = this.writeTail.then(async () => {
      for (const fragment of fragmentFrame(frame, this.options.maxFrameBytes)) await this.writeNow(fragment);
    });
    this.writeTail = operation.catch(() => {});
    return operation;
  }

  private write(frame: RSocketFrame): Promise<void> {
    const operation = this.writeTail.then(() => this.writeNow(frame));
    this.writeTail = operation.catch(() => {});
    return operation;
  }

  private async writeNow(frame: RSocketFrame): Promise<void> {
    if (!this.lane) throw new ConnectionLostError("RSocket transport lane is unavailable.");
    const encoded = encodeRSocketFrame(frame);
    if (encoded.byteLength > this.options.maxFrameBytes) throw new PureReactiveProtocolError(`RSocket frame exceeds configured ${this.options.maxFrameBytes} bytes.`, "RSOCKET_FRAME_TOO_LARGE");
    try { await this.lane.write(encoded); }
    catch (cause) {
      const error = cause instanceof Error ? new ConnectionLostError(cause.message, { cause }) : new ConnectionLostError();
      this.detach(error);
      throw error;
    }
  }

  private startKeepAlive(): void {
    this.stopKeepAlive();
    const peerKeepAliveMs = this.setup?.keepAliveMs ?? this.options.keepAliveMs;
    const interval = this.origin === "initiator" ? this.options.keepAliveMs : Math.max(10, Math.min(peerKeepAliveMs, this.options.keepAliveMs));
    const lifetime = this.origin === "acceptor" ? (this.setup?.lifetimeMs ?? this.options.lifetimeMs) : this.options.lifetimeMs;
    const tick = async (): Promise<void> => {
      if (this.sessionState !== "ready") return;
      const silent = Date.now() - this.lastInboundAt;
      if (silent >= lifetime) {
        this.detach(new PureReactiveProtocolError(`RSocket peer was silent for ${silent}ms (lifetime ${lifetime}ms).`, "RSOCKET_KEEPALIVE_TIMEOUT"));
        return;
      }
      // RSocket 1.0 requires the client/requester side to emit KEEPALIVE periodically,
      // regardless of unrelated application traffic received from the peer.
      if (this.origin === "initiator") {
        await this.write({ type: RSocketFrameType.KEEPALIVE, streamId: 0, flags: RSOCKET_FLAG_RESPOND, lastReceivedPosition: 0n }).catch(() => {});
      }
      if (this.sessionState === "ready") this.scheduleKeepAlive(tick, interval);
    };
    this.scheduleKeepAlive(tick, interval);
  }

  private scheduleKeepAlive(tick: () => Promise<void>, interval: number): void {
    this.keepAliveTimer = setTimeout(() => { void tick(); }, interval);
    (this.keepAliveTimer as unknown as { unref?: () => void }).unref?.();
  }
  private stopKeepAlive(): void { if (this.keepAliveTimer) clearTimeout(this.keepAliveTimer); this.keepAliveTimer = undefined; }

  private maybeRetire(state: RSocketStreamState): void {
    if (!state.cancelled && !(state.inboundClosed && state.outboundClosed)) return;
    if (state.wireId !== undefined) this.streams.delete(state.wireId);
  }

  private terminate(state: RSocketStreamState, error: unknown): void {
    this.pendingLocalStreams.delete(state);
    state.cancelled = true;
    state.inboundClosed = true;
    state.outboundClosed = true;
    state.incoming.end(error, true);
    if (!state.abortController.signal.aborted) state.abortController.abort(error);
    for (const waiter of state.creditWaiters.splice(0)) waiter.reject(error);
    if (state.wireId !== undefined) {
      this.dropFragmentAssembly(state.wireId);
      this.streams.delete(state.wireId);
    }
  }

  private assertReady(): void {
    if (this.sessionState !== "ready") throw new PureReactiveProtocolError(`RSocket compatibility session is ${this.sessionState}.`, "SESSION_NOT_READY");
  }
  private assertStreamOpen(state: RSocketStreamState): void {
    this.assertReady();
    if (state.cancelled || state.outboundClosed) throw new StreamClosedError();
  }

  private transition(state: SessionState): void {
    this.sessionState = state;
    for (const listener of this.listeners) { try { listener(state); } catch { /* observers cannot break the protocol */ } }
  }

  private async finishClosed(reason: string): Promise<void> {
    if (this.sessionState === "closed") return;
    this.stopKeepAlive();
    const error = new StreamClosedError(reason);
    for (const state of [...this.streams.values()]) this.terminate(state, error);
    for (const state of [...this.pendingLocalStreams]) this.terminate(state, error);
    this.incomingStreams.end(undefined, true);
    this.incomingSignals.end(undefined, true);
    this.clearFragmentAssemblies();
    this.profileDispose?.();
    this.profileDispose = undefined;
    const lane = this.lane; const connection = this.connection;
    this.lane = undefined; this.connection = undefined;
    this.transition("closed");
    await Promise.resolve(lane?.close(reason)).catch(() => {});
    await Promise.resolve(connection?.close(undefined, reason)).catch(() => {});
  }

  private detach(error?: unknown): void {
    if (this.sessionState === "closed" || this.sessionState === "detached") return;
    this.stopKeepAlive();
    const failure = error instanceof Error ? error : new ConnectionLostError();
    if (this.sessionState === "connecting") this.ready.reject(failure);
    for (const state of [...this.streams.values()]) this.terminate(state, failure);
    for (const state of [...this.pendingLocalStreams]) this.terminate(state, failure);
    this.incomingStreams.end(failure, true);
    this.incomingSignals.end(failure, true);
    this.clearFragmentAssemblies();
    this.profileDispose?.();
    this.profileDispose = undefined;
    const lane = this.lane; const connection = this.connection;
    this.lane = undefined; this.connection = undefined;
    this.transition("detached");
    void Promise.resolve(lane?.close("detached")).catch(() => {});
    void Promise.resolve(connection?.close(undefined, "detached")).catch(() => {});
  }
}

export const connectRSocket = async (
  transport: ReactiveTransport,
  options: RSocketCompatibilityOptions & { readonly signal?: AbortSignal } = {}
): Promise<RSocketCompatibilitySession> => {
  const { signal, ...compatibility } = options;
  return new RSocketCompatibilitySession(compatibility).attach(transport, "initiator", signal);
};

export const acceptRSocket = async (
  transport: ReactiveTransport,
  options: RSocketCompatibilityOptions & { readonly signal?: AbortSignal } = {}
): Promise<RSocketCompatibilitySession> => {
  const { signal, ...compatibility } = options;
  return new RSocketCompatibilitySession(compatibility).attach(transport, "acceptor", signal);
};
