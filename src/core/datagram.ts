import { ProtocolViolationError } from "./errors";

export const CORE_DATAGRAM_CAPABILITY_ID = "prp.core.datagram";
export const PRP_DATAGRAM_MAGIC = 0x50524431; // "PRD1"
export const PRP_DATAGRAM_HEADER_BYTES = 4;

export interface DatagramOptions {
  /** Maximum application payload bytes accepted from one native PRP datagram. */
  readonly maxInboundBytes?: number;
  /** Maximum number of received datagrams buffered for the application. */
  readonly maxPending?: number;
}

export interface ResolvedDatagramOptions {
  readonly maxInboundBytes: number;
  readonly maxPending: number;
}

export const DEFAULT_DATAGRAM_OPTIONS: Readonly<ResolvedDatagramOptions> = Object.freeze({
  maxInboundBytes: 64 * 1024,
  maxPending: 256
});

const positiveInteger = (name: string, value: number): number => {
  if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError(`${name} must be a positive safe integer.`);
  return value;
};

export const resolveDatagramOptions = (options: DatagramOptions = {}): Readonly<ResolvedDatagramOptions> => Object.freeze({
  maxInboundBytes: positiveInteger("datagrams.maxInboundBytes", options.maxInboundBytes ?? DEFAULT_DATAGRAM_OPTIONS.maxInboundBytes),
  maxPending: positiveInteger("datagrams.maxPending", options.maxPending ?? DEFAULT_DATAGRAM_OPTIONS.maxPending)
});

/** Capability parameter: unsigned 32-bit maximum application payload accepted by the receiver. */
export const encodeDatagramCapability = (maxInboundBytes: number): Uint8Array => {
  positiveInteger("datagrams.maxInboundBytes", maxInboundBytes);
  if (maxInboundBytes > 0xffffffff) throw new RangeError("datagrams.maxInboundBytes must fit unsigned 32 bits.");
  const output = new Uint8Array(4);
  new DataView(output.buffer).setUint32(0, maxInboundBytes);
  return output;
};

export const decodeDatagramCapability = (value: Uint8Array | undefined): number => {
  if (!value || value.byteLength !== 4) throw new RangeError("prp.core.datagram requires exactly 4 parameter bytes.");
  const maxInboundBytes = new DataView(value.buffer, value.byteOffset, value.byteLength).getUint32(0);
  if (maxInboundBytes <= 0) throw new RangeError("prp.core.datagram maxInboundBytes must be positive.");
  return maxInboundBytes;
};

/** Compact PRP native datagram envelope. Datagram message boundaries are provided by the carrier. */
export const encodeDatagram = (payload: Uint8Array, maxFrameBytes = Number.MAX_SAFE_INTEGER): Uint8Array => {
  if (!(payload instanceof Uint8Array)) throw new TypeError("Datagram payload must be a Uint8Array.");
  const frameBytes = PRP_DATAGRAM_HEADER_BYTES + payload.byteLength;
  if (frameBytes > maxFrameBytes) throw new RangeError(`PRP datagram exceeds the carrier limit of ${maxFrameBytes} bytes.`);
  const output = new Uint8Array(frameBytes);
  new DataView(output.buffer).setUint32(0, PRP_DATAGRAM_MAGIC);
  output.set(payload, PRP_DATAGRAM_HEADER_BYTES);
  return output;
};

export const decodeDatagram = (frame: Uint8Array, maxPayloadBytes: number): Uint8Array => {
  if (frame.byteLength < PRP_DATAGRAM_HEADER_BYTES) throw new ProtocolViolationError("Truncated PRP native datagram.");
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
  if (view.getUint32(0) !== PRP_DATAGRAM_MAGIC) throw new ProtocolViolationError("Invalid PRP native datagram magic.");
  const payloadBytes = frame.byteLength - PRP_DATAGRAM_HEADER_BYTES;
  if (payloadBytes > maxPayloadBytes) throw new ProtocolViolationError(`PRP native datagram exceeds ${maxPayloadBytes} application bytes.`);
  return frame.slice(PRP_DATAGRAM_HEADER_BYTES);
};
