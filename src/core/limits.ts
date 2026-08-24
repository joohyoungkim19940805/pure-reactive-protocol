import { FRAME_HEADER_BYTES } from "./frame";

export interface ProtocolLimits {
  readonly maxFrameBytes: number;
  readonly maxAttributeBytes: number;
  readonly maxAttributes: number;
  readonly maxAttributeIdBytes: number;
  readonly maxAttributeValueBytes: number;
  /** Maximum logical DATA payload bytes accepted for one item after reassembly. */
  readonly maxInboundItemBytes: number;
  /** Maximum total bytes reserved by incomplete fragmented items in this runtime. */
  readonly maxInFlightReassemblyBytes: number;
  /** Maximum number of streams the remote peer may have open toward this runtime. */
  readonly maxInboundStreams: number;
  readonly maxPendingIncomingStreams: number;
  readonly maxPendingSignals: number;
  readonly maxInFlightSignalBytes: number;
  readonly maxRetainedStreamAttributeBytes: number;
  readonly maxRetiredStreams: number;
}

export const DEFAULT_PROTOCOL_LIMITS: Readonly<ProtocolLimits> = Object.freeze({
  maxFrameBytes: 16 * 1024 * 1024,
  maxAttributeBytes: 1024 * 1024,
  maxAttributes: 256,
  maxAttributeIdBytes: 1024,
  maxAttributeValueBytes: 1024 * 1024,
  maxInboundItemBytes: 64 * 1024 * 1024,
  maxInFlightReassemblyBytes: 128 * 1024 * 1024,
  maxInboundStreams: 4096,
  maxPendingIncomingStreams: 1024,
  maxPendingSignals: 1024,
  maxInFlightSignalBytes: 16 * 1024 * 1024,
  maxRetainedStreamAttributeBytes: 64 * 1024 * 1024,
  maxRetiredStreams: 4096
});

/** Fixed PRP/1 envelope used before peer limits are known. */
export const BOOTSTRAP_PROTOCOL_LIMITS: Readonly<ProtocolLimits> = Object.freeze({
  ...DEFAULT_PROTOCOL_LIMITS,
  maxFrameBytes: 64 * 1024,
  maxAttributeBytes: 60 * 1024,
  maxAttributeValueBytes: 16 * 1024
});

const positiveInteger = (name: keyof ProtocolLimits, value: number): number => {
  if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError(`${name} must be a positive safe integer.`);
  return value;
};

export const resolveProtocolLimits = (
  overrides: Partial<ProtocolLimits> = {}
): Readonly<ProtocolLimits> => {
  const resolved: ProtocolLimits = {
    maxFrameBytes: positiveInteger("maxFrameBytes", overrides.maxFrameBytes ?? DEFAULT_PROTOCOL_LIMITS.maxFrameBytes),
    maxAttributeBytes: positiveInteger("maxAttributeBytes", overrides.maxAttributeBytes ?? DEFAULT_PROTOCOL_LIMITS.maxAttributeBytes),
    maxAttributes: positiveInteger("maxAttributes", overrides.maxAttributes ?? DEFAULT_PROTOCOL_LIMITS.maxAttributes),
    maxAttributeIdBytes: positiveInteger("maxAttributeIdBytes", overrides.maxAttributeIdBytes ?? DEFAULT_PROTOCOL_LIMITS.maxAttributeIdBytes),
    maxAttributeValueBytes: positiveInteger("maxAttributeValueBytes", overrides.maxAttributeValueBytes ?? DEFAULT_PROTOCOL_LIMITS.maxAttributeValueBytes),
    maxInboundItemBytes: positiveInteger("maxInboundItemBytes", overrides.maxInboundItemBytes ?? DEFAULT_PROTOCOL_LIMITS.maxInboundItemBytes),
    maxInFlightReassemblyBytes: positiveInteger("maxInFlightReassemblyBytes", overrides.maxInFlightReassemblyBytes ?? DEFAULT_PROTOCOL_LIMITS.maxInFlightReassemblyBytes),
    maxInboundStreams: positiveInteger("maxInboundStreams", overrides.maxInboundStreams ?? DEFAULT_PROTOCOL_LIMITS.maxInboundStreams),
    maxPendingIncomingStreams: positiveInteger("maxPendingIncomingStreams", overrides.maxPendingIncomingStreams ?? DEFAULT_PROTOCOL_LIMITS.maxPendingIncomingStreams),
    maxPendingSignals: positiveInteger("maxPendingSignals", overrides.maxPendingSignals ?? DEFAULT_PROTOCOL_LIMITS.maxPendingSignals),
    maxInFlightSignalBytes: positiveInteger("maxInFlightSignalBytes", overrides.maxInFlightSignalBytes ?? DEFAULT_PROTOCOL_LIMITS.maxInFlightSignalBytes),
    maxRetainedStreamAttributeBytes: positiveInteger("maxRetainedStreamAttributeBytes", overrides.maxRetainedStreamAttributeBytes ?? DEFAULT_PROTOCOL_LIMITS.maxRetainedStreamAttributeBytes),
    maxRetiredStreams: positiveInteger("maxRetiredStreams", overrides.maxRetiredStreams ?? DEFAULT_PROTOCOL_LIMITS.maxRetiredStreams)
  };

  if (resolved.maxFrameBytes < FRAME_HEADER_BYTES) throw new RangeError(`maxFrameBytes must be at least the ${FRAME_HEADER_BYTES}-byte PRP/1 core header.`);
  if (resolved.maxFrameBytes > 0xffffffff) throw new RangeError("maxFrameBytes must fit unsigned 32 bits.");
  if (resolved.maxAttributeBytes >= resolved.maxFrameBytes) throw new RangeError("maxAttributeBytes must be smaller than maxFrameBytes.");
  if (resolved.maxAttributes > 0xffffffff) throw new RangeError("maxAttributes must fit unsigned 32 bits.");
  if (resolved.maxAttributeIdBytes > 0xffff) throw new RangeError("maxAttributeIdBytes must fit unsigned 16 bits.");
  if (resolved.maxAttributeValueBytes > resolved.maxAttributeBytes) throw new RangeError("maxAttributeValueBytes must not exceed maxAttributeBytes.");
  if (resolved.maxInboundItemBytes > 0xffffffff) throw new RangeError("maxInboundItemBytes must fit unsigned 32 bits in PRP/1.");
  if (resolved.maxInFlightReassemblyBytes < resolved.maxInboundItemBytes) throw new RangeError("maxInFlightReassemblyBytes must be at least maxInboundItemBytes.");
  if (resolved.maxInboundStreams > 0xffffffff) throw new RangeError("maxInboundStreams must fit unsigned 32 bits.");
  if (resolved.maxPendingIncomingStreams > resolved.maxInboundStreams) throw new RangeError("maxPendingIncomingStreams must not exceed maxInboundStreams.");
  return Object.freeze(resolved);
};

export interface PeerProtocolLimits {
  readonly maxFrameBytes: number;
  readonly maxAttributeBytes: number;
  readonly maxAttributes: number;
  readonly maxAttributeIdBytes: number;
  readonly maxAttributeValueBytes: number;
  readonly maxInboundStreams: number;
  readonly maxInboundItemBytes: number;
}

export const PEER_LIMITS_BYTES = 26;

export const encodePeerProtocolLimits = (limits: Readonly<ProtocolLimits>): Uint8Array => {
  const output = new Uint8Array(PEER_LIMITS_BYTES);
  const view = new DataView(output.buffer);
  view.setUint32(0, limits.maxFrameBytes);
  view.setUint32(4, limits.maxAttributeBytes);
  view.setUint32(8, limits.maxAttributes);
  view.setUint16(12, limits.maxAttributeIdBytes);
  view.setUint32(14, limits.maxAttributeValueBytes);
  view.setUint32(18, limits.maxInboundStreams);
  view.setUint32(22, limits.maxInboundItemBytes);
  return output;
};

export const decodePeerProtocolLimits = (value: Uint8Array | undefined): PeerProtocolLimits => {
  if (!value || value.length !== PEER_LIMITS_BYTES) throw new RangeError(`PRP core limits capability requires exactly ${PEER_LIMITS_BYTES} parameter bytes.`);
  const view = new DataView(value.buffer, value.byteOffset, value.byteLength);
  const limits: PeerProtocolLimits = {
    maxFrameBytes: view.getUint32(0),
    maxAttributeBytes: view.getUint32(4),
    maxAttributes: view.getUint32(8),
    maxAttributeIdBytes: view.getUint16(12),
    maxAttributeValueBytes: view.getUint32(14),
    maxInboundStreams: view.getUint32(18),
    maxInboundItemBytes: view.getUint32(22)
  };
  for (const [name, limit] of Object.entries(limits)) {
    if (!Number.isSafeInteger(limit) || limit <= 0) throw new RangeError(`Peer ${name} must be positive.`);
  }
  if (limits.maxFrameBytes < FRAME_HEADER_BYTES) throw new RangeError(`Peer maxFrameBytes must be at least the ${FRAME_HEADER_BYTES}-byte PRP/1 core header.`);
  if (limits.maxAttributeBytes >= limits.maxFrameBytes) throw new RangeError("Peer maxAttributeBytes must be smaller than maxFrameBytes.");
  if (limits.maxAttributeValueBytes > limits.maxAttributeBytes) throw new RangeError("Peer maxAttributeValueBytes must not exceed maxAttributeBytes.");
  return Object.freeze(limits);
};
