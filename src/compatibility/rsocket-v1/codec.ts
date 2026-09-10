import { PureReactiveProtocolError } from "../../core/errors";
import {
  RSOCKET_FLAG_COMPLETE,
  RSOCKET_FLAG_FOLLOWS,
  RSOCKET_FLAG_IGNORE,
  RSOCKET_FLAG_LEASE,
  RSOCKET_FLAG_METADATA,
  RSOCKET_FLAG_NEXT,
  RSOCKET_FLAG_RESPOND,
  RSOCKET_FLAG_RESUME,
  RSOCKET_HEADER_BYTES,
  RSOCKET_MAX_FRAME_BYTES,
  RSOCKET_MAX_REQUEST_N,
  RSOCKET_MAX_STREAM_ID,
  RSOCKET_VERSION_MAJOR,
  RSOCKET_VERSION_MINOR,
  RSocketFrameType,
  type RSocketFrame,
  type RSocketSetupFields
} from "./frame";

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

export class RSocketProtocolError extends PureReactiveProtocolError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, "RSOCKET_PROTOCOL_VIOLATION", options);
    this.name = "RSocketProtocolError";
  }
}

const u24 = (value: number): Uint8Array => {
  if (!Number.isInteger(value) || value < 0 || value > 0xffffff) throw new RangeError("Value must fit unsigned 24 bits.");
  return Uint8Array.of((value >>> 16) & 0xff, (value >>> 8) & 0xff, value & 0xff);
};

const readU24 = (input: Uint8Array, offset: number): number => {
  if (offset + 3 > input.byteLength) throw new RSocketProtocolError("Truncated unsigned 24-bit field.");
  return (input[offset]! << 16) | (input[offset + 1]! << 8) | input[offset + 2]!;
};

const validateStreamId = (streamId: number): void => {
  if (!Number.isInteger(streamId) || streamId < 0 || streamId > RSOCKET_MAX_STREAM_ID) throw new RangeError("RSocket stream id must fit unsigned 31 bits.");
};

const metadataAndDataSize = (metadata?: Uint8Array, data?: Uint8Array): number =>
  (metadata ? 3 + metadata.byteLength : 0) + (data?.byteLength ?? 0);

const encodeMetadataData = (metadata?: Uint8Array, data?: Uint8Array): Uint8Array => {
  const output = new Uint8Array(metadataAndDataSize(metadata, data));
  let offset = 0;
  if (metadata) {
    output.set(u24(metadata.byteLength), offset); offset += 3;
    output.set(metadata, offset); offset += metadata.byteLength;
  }
  if (data) output.set(data, offset);
  return output;
};

const decodeMetadataData = (input: Uint8Array, offset: number, metadataPresent: boolean): { metadata?: Uint8Array; data: Uint8Array } => {
  if (!metadataPresent) return { data: input.slice(offset) };
  const metadataLength = readU24(input, offset); offset += 3;
  if (offset + metadataLength > input.byteLength) throw new RSocketProtocolError("RSocket metadata length exceeds frame length.");
  const metadata = input.slice(offset, offset + metadataLength); offset += metadataLength;
  return { metadata, data: input.slice(offset) };
};

const encodeMime = (mime: string): Uint8Array => {
  const value = encoder.encode(mime);
  if (value.byteLength === 0 || value.byteLength > 255) throw new RangeError("RSocket MIME type must encode to 1..255 bytes.");
  const output = new Uint8Array(1 + value.byteLength);
  output[0] = value.byteLength;
  output.set(value, 1);
  return output;
};

const decodeMime = (input: Uint8Array, offset: number): readonly [string, number] => {
  if (offset >= input.byteLength) throw new RSocketProtocolError("Truncated RSocket MIME length.");
  const length = input[offset++]!;
  if (length === 0 || offset + length > input.byteLength) throw new RSocketProtocolError("Malformed RSocket MIME type.");
  try { return [decoder.decode(input.subarray(offset, offset + length)), offset + length] as const; }
  catch (cause) { throw new RSocketProtocolError("RSocket MIME type is not valid UTF-8.", { cause }); }
};

const encodeSetupBody = (setup: RSocketSetupFields, metadata?: Uint8Array, data?: Uint8Array, flags = 0): Uint8Array => {
  const resume = (flags & RSOCKET_FLAG_RESUME) !== 0;
  const metadataMime = encodeMime(setup.metadataMimeType);
  const dataMime = encodeMime(setup.dataMimeType);
  const token = setup.resumeToken ?? new Uint8Array(0);
  if (resume && token.byteLength > 0xffff) throw new RangeError("RSocket resume token exceeds 16-bit length.");
  const payload = encodeMetadataData(metadata, data);
  const fixed = 12 + (resume ? 2 + token.byteLength : 0);
  const output = new Uint8Array(fixed + metadataMime.byteLength + dataMime.byteLength + payload.byteLength);
  const view = new DataView(output.buffer);
  view.setUint16(0, setup.major);
  view.setUint16(2, setup.minor);
  if (!Number.isInteger(setup.keepAliveMs) || setup.keepAliveMs <= 0 || setup.keepAliveMs > 0x7fffffff) throw new RangeError("RSocket keepAliveMs must be 1..2^31-1.");
  if (!Number.isInteger(setup.lifetimeMs) || setup.lifetimeMs <= 0 || setup.lifetimeMs > 0x7fffffff) throw new RangeError("RSocket lifetimeMs must be 1..2^31-1.");
  view.setUint32(4, setup.keepAliveMs);
  view.setUint32(8, setup.lifetimeMs);
  let offset = 12;
  if (resume) { view.setUint16(offset, token.byteLength); offset += 2; output.set(token, offset); offset += token.byteLength; }
  output.set(metadataMime, offset); offset += metadataMime.byteLength;
  output.set(dataMime, offset); offset += dataMime.byteLength;
  output.set(payload, offset);
  return output;
};

export const encodeRSocketFrame = (frame: RSocketFrame): Uint8Array => {
  validateStreamId(frame.streamId);
  let flags = frame.flags ?? 0;
  if (frame.metadata) flags |= RSOCKET_FLAG_METADATA;
  let body: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  switch (frame.type) {
    case RSocketFrameType.SETUP: {
      if (frame.streamId !== 0 || !frame.setup) throw new RangeError("SETUP requires stream 0 and setup fields.");
      body = encodeSetupBody(frame.setup, frame.metadata, frame.data, flags);
      break;
    }
    case RSocketFrameType.KEEPALIVE: {
      if (frame.streamId !== 0) throw new RangeError("KEEPALIVE requires stream 0.");
      const position = frame.lastReceivedPosition ?? 0n;
      if (position < 0n || position > 0x7fffffffffffffffn) throw new RangeError("RSocket keepalive position must fit unsigned 63 bits.");
      body = new Uint8Array(8 + (frame.data?.byteLength ?? 0));
      new DataView(body.buffer).setBigUint64(0, position);
      if (frame.data) body.set(frame.data, 8);
      break;
    }
    case RSocketFrameType.REQUEST_RESPONSE:
    case RSocketFrameType.REQUEST_FNF:
      if (frame.streamId === 0) throw new RangeError("RSocket request requires nonzero stream id.");
      body = encodeMetadataData(frame.metadata, frame.data);
      break;
    case RSocketFrameType.REQUEST_STREAM:
    case RSocketFrameType.REQUEST_CHANNEL: {
      if (frame.streamId === 0) throw new RangeError("RSocket stream/channel request requires nonzero stream id.");
      const n = frame.initialRequestN ?? 0;
      if (!Number.isInteger(n) || n <= 0 || n > RSOCKET_MAX_REQUEST_N) throw new RangeError("Initial Request N must be 1..2^31-1.");
      const payload = encodeMetadataData(frame.metadata, frame.data);
      body = new Uint8Array(4 + payload.byteLength);
      new DataView(body.buffer).setUint32(0, n);
      body.set(payload, 4);
      break;
    }
    case RSocketFrameType.REQUEST_N: {
      if (frame.streamId === 0) throw new RangeError("REQUEST_N requires nonzero stream id.");
      const n = frame.requestN ?? 0;
      if (!Number.isInteger(n) || n <= 0 || n > RSOCKET_MAX_REQUEST_N) throw new RangeError("Request N must be 1..2^31-1.");
      body = new Uint8Array(4); new DataView(body.buffer).setUint32(0, n);
      flags &= ~RSOCKET_FLAG_METADATA;
      break;
    }
    case RSocketFrameType.CANCEL:
      if (frame.streamId === 0) throw new RangeError("CANCEL requires nonzero stream id.");
      flags = 0;
      break;
    case RSocketFrameType.PAYLOAD:
      if (frame.streamId === 0) throw new RangeError("PAYLOAD requires nonzero stream id."); if ((flags & (RSOCKET_FLAG_NEXT | RSOCKET_FLAG_COMPLETE)) === 0) throw new RangeError("RSocket PAYLOAD must set NEXT, COMPLETE, or both.");
      body = encodeMetadataData(frame.metadata, frame.data);
      break;
    case RSocketFrameType.ERROR: {
      const code = frame.errorCode ?? 0;
      if (!Number.isInteger(code) || code < 0 || code > 0xffffffff) throw new RangeError("RSocket error code must fit unsigned 32 bits.");
      body = new Uint8Array(4 + (frame.data?.byteLength ?? 0));
      new DataView(body.buffer).setUint32(0, code);
      if (frame.data) body.set(frame.data, 4);
      flags = 0;
      break;
    }
    case RSocketFrameType.LEASE: {
      if (frame.streamId !== 0) throw new RangeError("LEASE requires stream 0.");
      const ttl = frame.leaseTtlMs ?? 0;
      const requests = frame.leaseRequests ?? 0;
      if (!Number.isInteger(ttl) || ttl <= 0 || ttl > 0x7fffffff || !Number.isInteger(requests) || requests <= 0 || requests > 0x7fffffff) throw new RangeError("RSocket LEASE values must be 1..2^31-1.");
      body = new Uint8Array(8 + (frame.metadata?.byteLength ?? 0));
      const view = new DataView(body.buffer);
      view.setUint32(0, ttl);
      view.setUint32(4, requests);
      if (frame.metadata) body.set(frame.metadata, 8);
      break;
    }
    case RSocketFrameType.METADATA_PUSH:
      if (frame.streamId !== 0 || !frame.metadata) throw new RangeError("METADATA_PUSH requires stream 0 and metadata.");
      flags |= RSOCKET_FLAG_METADATA;
      body = frame.metadata.slice();
      break;
    default:
      throw new RangeError(`Encoding RSocket frame type ${frame.type} is not implemented.`);
  }
  const output = new Uint8Array(RSOCKET_HEADER_BYTES + body.byteLength);
  if (output.byteLength > RSOCKET_MAX_FRAME_BYTES) throw new RangeError(`RSocket frame exceeds ${RSOCKET_MAX_FRAME_BYTES} bytes.`);
  const view = new DataView(output.buffer);
  view.setUint32(0, frame.streamId & RSOCKET_MAX_STREAM_ID);
  view.setUint16(4, (frame.type << 10) | (flags & 0x03ff));
  output.set(body, RSOCKET_HEADER_BYTES);
  return output;
};

export const decodeRSocketFrame = (input: Uint8Array): RSocketFrame => {
  if (input.byteLength < RSOCKET_HEADER_BYTES || input.byteLength > RSOCKET_MAX_FRAME_BYTES) throw new RSocketProtocolError("Invalid RSocket frame length.");
  const view = new DataView(input.buffer, input.byteOffset, input.byteLength);
  const rawStreamId = view.getUint32(0);
  if ((rawStreamId & 0x80000000) !== 0) throw new RSocketProtocolError("RSocket stream id reserved bit must be zero.");
  const streamId = rawStreamId & RSOCKET_MAX_STREAM_ID;
  const typeAndFlags = view.getUint16(4);
  const type = (typeAndFlags >>> 10) as RSocketFrameType;
  const flags = typeAndFlags & 0x03ff;
  let offset = RSOCKET_HEADER_BYTES;
  const metadataPresent = (flags & RSOCKET_FLAG_METADATA) !== 0;

  switch (type) {
    case RSocketFrameType.SETUP: {
      if (streamId !== 0 || input.byteLength < offset + 12) throw new RSocketProtocolError("Malformed SETUP frame.");
      const major = view.getUint16(offset); const minor = view.getUint16(offset + 2);
      const rawKeepAliveMs = view.getUint32(offset + 4);
      const rawLifetimeMs = view.getUint32(offset + 8);
      if ((rawKeepAliveMs & 0x80000000) !== 0 || rawKeepAliveMs === 0) throw new RSocketProtocolError("SETUP keepalive must be a positive unsigned 31-bit value.");
      if ((rawLifetimeMs & 0x80000000) !== 0 || rawLifetimeMs === 0) throw new RSocketProtocolError("SETUP lifetime must be a positive unsigned 31-bit value.");
      const keepAliveMs = rawKeepAliveMs;
      const lifetimeMs = rawLifetimeMs;
      offset += 12;
      let resumeToken: Uint8Array | undefined;
      if ((flags & RSOCKET_FLAG_RESUME) !== 0) {
        if (offset + 2 > input.byteLength) throw new RSocketProtocolError("Truncated SETUP resume token length.");
        const length = view.getUint16(offset); offset += 2;
        if (offset + length > input.byteLength) throw new RSocketProtocolError("Truncated SETUP resume token.");
        resumeToken = input.slice(offset, offset + length); offset += length;
      }
      const metadataMime = decodeMime(input, offset); offset = metadataMime[1];
      const dataMime = decodeMime(input, offset); offset = dataMime[1];
      const payload = decodeMetadataData(input, offset, metadataPresent);
      return {
        type, streamId, flags,
        setup: { major, minor, keepAliveMs, lifetimeMs, metadataMimeType: metadataMime[0], dataMimeType: dataMime[0], ...(resumeToken ? { resumeToken } : {}) },
        ...(payload.metadata ? { metadata: payload.metadata } : {}),
        ...(payload.data.byteLength ? { data: payload.data } : {})
      };
    }
    case RSocketFrameType.KEEPALIVE: {
      if (streamId !== 0 || input.byteLength < offset + 8) throw new RSocketProtocolError("Malformed KEEPALIVE frame.");
      const lastReceivedPosition = view.getBigUint64(offset);
      if (lastReceivedPosition > 0x7fffffffffffffffn) throw new RSocketProtocolError("RSocket KEEPALIVE position reserved bit must be zero.");
      return { type, streamId, flags, lastReceivedPosition, ...(input.byteLength > offset + 8 ? { data: input.slice(offset + 8) } : {}) };
    }
    case RSocketFrameType.REQUEST_RESPONSE:
    case RSocketFrameType.REQUEST_FNF: {
      if (streamId === 0) throw new RSocketProtocolError("Request frame uses stream 0.");
      const payload = decodeMetadataData(input, offset, metadataPresent);
      return { type, streamId, flags, ...(payload.metadata ? { metadata: payload.metadata } : {}), ...(payload.data.byteLength || type !== RSocketFrameType.REQUEST_FNF ? { data: payload.data } : {}) };
    }
    case RSocketFrameType.REQUEST_STREAM:
    case RSocketFrameType.REQUEST_CHANNEL: {
      if (streamId === 0 || offset + 4 > input.byteLength) throw new RSocketProtocolError("Malformed stream/channel request.");
      const rawInitialRequestN = view.getUint32(offset); offset += 4;
      if ((rawInitialRequestN & 0x80000000) !== 0 || rawInitialRequestN === 0) throw new RSocketProtocolError("Initial Request N must be a positive unsigned 31-bit value.");
      const initialRequestN = rawInitialRequestN;
      const payload = decodeMetadataData(input, offset, metadataPresent);
      return { type, streamId, flags, initialRequestN, ...(payload.metadata ? { metadata: payload.metadata } : {}), ...(payload.data.byteLength ? { data: payload.data } : {}) };
    }
    case RSocketFrameType.REQUEST_N: {
      if (streamId === 0 || input.byteLength !== offset + 4) throw new RSocketProtocolError("Malformed REQUEST_N frame.");
      const rawRequestN = view.getUint32(offset);
      if ((rawRequestN & 0x80000000) !== 0 || rawRequestN === 0) throw new RSocketProtocolError("REQUEST_N must be a positive unsigned 31-bit value.");
      return { type, streamId, flags, requestN: rawRequestN };
    }
    case RSocketFrameType.CANCEL:
      if (streamId === 0 || input.byteLength !== offset) throw new RSocketProtocolError("Malformed CANCEL frame.");
      return { type, streamId, flags };
    case RSocketFrameType.PAYLOAD: {
      if (streamId === 0) throw new RSocketProtocolError("PAYLOAD uses stream 0."); if ((flags & (RSOCKET_FLAG_NEXT | RSOCKET_FLAG_COMPLETE)) === 0) throw new RSocketProtocolError("RSocket PAYLOAD must set NEXT, COMPLETE, or both.");
      // PAYLOAD always carries NEXT, COMPLETE, or both, including continuation frames while FOLLOWS is set.
      // Reassembly still exposes the completed logical item only once to the application layer.
      const payload = decodeMetadataData(input, offset, metadataPresent);
      return { type, streamId, flags, ...(payload.metadata ? { metadata: payload.metadata } : {}), ...(payload.data.byteLength || (flags & RSOCKET_FLAG_NEXT) !== 0 ? { data: payload.data } : {}) };
    }
    case RSocketFrameType.ERROR:
      if (offset + 4 > input.byteLength) throw new RSocketProtocolError("Malformed ERROR frame.");
      return { type, streamId, flags, errorCode: view.getUint32(offset), ...(input.byteLength > offset + 4 ? { data: input.slice(offset + 4) } : {}) };
    case RSocketFrameType.LEASE: {
      if (streamId !== 0 || offset + 8 > input.byteLength) throw new RSocketProtocolError("Malformed LEASE frame.");
      const ttl = view.getUint32(offset);
      const requests = view.getUint32(offset + 4);
      if ((ttl & 0x80000000) !== 0 || ttl === 0 || (requests & 0x80000000) !== 0 || requests === 0) {
        throw new RSocketProtocolError("LEASE values must be positive unsigned 31-bit values.");
      }
      return { type, streamId, flags, leaseTtlMs: ttl, leaseRequests: requests, ...(input.byteLength > offset + 8 ? { metadata: input.slice(offset + 8) } : {}) };
    }
    case RSocketFrameType.METADATA_PUSH:
      if (streamId !== 0 || !metadataPresent) throw new RSocketProtocolError("Malformed METADATA_PUSH frame.");
      return { type, streamId, flags, metadata: input.slice(offset) };
    default:
      if ((flags & RSOCKET_FLAG_IGNORE) !== 0) {
        return { type, streamId, flags, ...(input.byteLength > offset ? { data: input.slice(offset) } : {}) };
      }
      throw new RSocketProtocolError(`Unsupported RSocket frame type: ${type}.`);
  }
};

export const defaultRSocketSetup = (): RSocketSetupFields => ({
  major: RSOCKET_VERSION_MAJOR,
  minor: RSOCKET_VERSION_MINOR,
  keepAliveMs: 20_000,
  lifetimeMs: 90_000,
  metadataMimeType: "message/x.rsocket.composite-metadata.v0",
  dataMimeType: "application/json"
});

export { RSOCKET_FLAG_COMPLETE, RSOCKET_FLAG_FOLLOWS, RSOCKET_FLAG_IGNORE, RSOCKET_FLAG_LEASE, RSOCKET_FLAG_METADATA, RSOCKET_FLAG_NEXT, RSOCKET_FLAG_RESPOND, RSOCKET_FLAG_RESUME };
