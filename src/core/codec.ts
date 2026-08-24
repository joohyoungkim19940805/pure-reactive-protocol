import { bytes, strictText, type ProtocolAttribute } from "./attributes";
import {
  DATA_FRAGMENTED_FLAG,
  FRAME_HEADER_BYTES,
  FrameKind,
  PROTOCOL_MAGIC,
  PROTOCOL_MAJOR,
  PROTOCOL_MINOR,
  type ProtocolFrame
} from "./frame";
import { ProtocolViolationError } from "./errors";
import { DEFAULT_PROTOCOL_LIMITS, type ProtocolLimits } from "./limits";

const ATTRIBUTE_REQUIRED = 1;
const MAX_U64 = 0xffffffffffffffffn;

const encodeAttributes = (
  attributes: readonly ProtocolAttribute[],
  limits: Readonly<ProtocolLimits>
): Uint8Array => {
  if (attributes.length > limits.maxAttributes) throw new RangeError(`Frame has more than ${limits.maxAttributes} attributes.`);
  const encoded = attributes.map((item) => ({ item, id: bytes(item.id) }));
  let length = 0;
  for (const { item, id } of encoded) {
    if (id.length === 0) throw new RangeError("Attribute id must not be empty.");
    if (id.length > limits.maxAttributeIdBytes) throw new RangeError(`Attribute id exceeds ${limits.maxAttributeIdBytes} bytes: ${item.id}`);
    if (item.value.length > limits.maxAttributeValueBytes) throw new RangeError(`Attribute value exceeds ${limits.maxAttributeValueBytes} bytes: ${item.id}`);
    length += 7 + id.length + item.value.length;
    if (length > limits.maxAttributeBytes) throw new RangeError(`Attribute area exceeds ${limits.maxAttributeBytes} bytes.`);
  }
  const output = new Uint8Array(length);
  const view = new DataView(output.buffer);
  let offset = 0;
  for (const { item, id } of encoded) {
    view.setUint8(offset, item.required ? ATTRIBUTE_REQUIRED : 0);
    view.setUint16(offset + 1, id.length);
    view.setUint32(offset + 3, item.value.length);
    offset += 7;
    output.set(id, offset);
    offset += id.length;
    output.set(item.value, offset);
    offset += item.value.length;
  }
  return output;
};

/** Returns the exact encoded TLV byte length and applies the normal attribute limits. */
export const measureAttributeBytes = (
  attributes: readonly ProtocolAttribute[],
  limits: Readonly<ProtocolLimits> = DEFAULT_PROTOCOL_LIMITS
): number => encodeAttributes(attributes, limits).byteLength;

const decodeAttributes = (
  value: Uint8Array,
  limits: Readonly<ProtocolLimits>
): readonly ProtocolAttribute[] => {
  if (value.length > limits.maxAttributeBytes) throw new ProtocolViolationError(`Attribute area exceeds ${limits.maxAttributeBytes} bytes.`);
  const attributes: ProtocolAttribute[] = [];
  const view = new DataView(value.buffer, value.byteOffset, value.byteLength);
  let offset = 0;
  while (offset < value.length) {
    if (attributes.length >= limits.maxAttributes) throw new ProtocolViolationError(`Frame has more than ${limits.maxAttributes} attributes.`);
    if (value.length - offset < 7) throw new ProtocolViolationError("Truncated attribute header.");
    const flags = view.getUint8(offset);
    if ((flags & ~ATTRIBUTE_REQUIRED) !== 0) throw new ProtocolViolationError(`Undefined attribute flag bits: 0x${flags.toString(16)}.`);
    const idLength = view.getUint16(offset + 1);
    const valueLength = view.getUint32(offset + 3);
    if (idLength === 0 || idLength > limits.maxAttributeIdBytes) throw new ProtocolViolationError("Invalid attribute id length.");
    if (valueLength > limits.maxAttributeValueBytes) throw new ProtocolViolationError(`Attribute value exceeds ${limits.maxAttributeValueBytes} bytes.`);
    offset += 7;
    if (offset + idLength + valueLength > value.length) throw new ProtocolViolationError("Truncated attribute value.");
    let id: string;
    try {
      id = strictText(value.subarray(offset, offset + idLength));
    } catch (cause) {
      throw new ProtocolViolationError("Attribute id is not valid UTF-8.", { cause });
    }
    if (!id) throw new ProtocolViolationError("Attribute id must not be empty.");
    offset += idLength;
    const attributeValue = value.slice(offset, offset + valueLength);
    offset += valueLength;
    attributes.push({ id, value: attributeValue, ...(flags & ATTRIBUTE_REQUIRED ? { required: true } : {}) });
  }
  return attributes;
};

const isKnownKind = (value: number): value is FrameKind =>
  value === FrameKind.HELLO || value === FrameKind.WELCOME || value === FrameKind.CLOSE || value === FrameKind.PING || value === FrameKind.PONG || value === FrameKind.OPEN ||
  value === FrameKind.DATA || value === FrameKind.DEMAND || value === FrameKind.COMPLETE ||
  value === FrameKind.CANCEL || value === FrameKind.ERROR || value === FrameKind.SIGNAL || value === FrameKind.FRAGMENT;

export const validateFrame = (frame: ProtocolFrame): void => {
  const hasAttributes = (frame.attributes?.length ?? 0) > 0;
  const payloadBytes = frame.payload?.length ?? 0;
  const hasPayload = payloadBytes > 0;
  const flags = frame.flags ?? 0;
  const credit = frame.credit ?? 0;
  const fragmentLength = frame.fragmentLength ?? 0;

  if (!Number.isInteger(flags) || flags < 0 || flags > 0xff) throw new ProtocolViolationError("Frame flags must fit unsigned 8 bits.");
  if (!Number.isInteger(credit) || credit < 0 || credit > 0xffffffff) throw new ProtocolViolationError("Frame credit must fit unsigned 32 bits.");
  if (!Number.isInteger(fragmentLength) || fragmentLength < 0 || fragmentLength > 0xffffffff) throw new ProtocolViolationError("Fragment length must fit unsigned 32 bits.");
  if (credit !== 0 && fragmentLength !== 0) throw new ProtocolViolationError("A frame cannot carry both demand credit and fragment length.");

  switch (frame.kind) {
    case FrameKind.HELLO:
    case FrameKind.WELCOME:
      if (frame.streamId !== 0n || flags !== 0 || credit !== 0 || fragmentLength !== 0 || hasPayload) throw new ProtocolViolationError("Invalid HELLO/WELCOME frame shape.");
      return;
    case FrameKind.CLOSE:
      if (frame.streamId !== 0n || flags !== 0 || credit !== 0 || fragmentLength !== 0 || hasAttributes) throw new ProtocolViolationError("Invalid CLOSE frame shape.");
      return;
    case FrameKind.PING:
    case FrameKind.PONG:
      if (frame.streamId !== 0n || flags !== 0 || credit !== 0 || fragmentLength !== 0 || hasAttributes || payloadBytes !== 8) {
        throw new ProtocolViolationError(`Invalid ${FrameKind[frame.kind]} frame shape.`);
      }
      return;
    case FrameKind.OPEN:
      if (frame.streamId === 0n || flags !== 0 || credit !== 0 || fragmentLength !== 0 || hasPayload) throw new ProtocolViolationError("Invalid OPEN frame shape.");
      return;
    case FrameKind.DATA: {
      if (frame.streamId === 0n || credit !== 0 || (flags & ~DATA_FRAGMENTED_FLAG) !== 0) throw new ProtocolViolationError("Invalid DATA frame shape.");
      const fragmented = (flags & DATA_FRAGMENTED_FLAG) !== 0;
      if (!fragmented && fragmentLength !== 0) throw new ProtocolViolationError("Non-fragmented DATA must not declare a fragment length.");
      if (fragmented && (fragmentLength <= 0 || payloadBytes >= fragmentLength)) throw new ProtocolViolationError("Fragmented DATA must declare a total payload length larger than its first fragment.");
      return;
    }
    case FrameKind.FRAGMENT:
      if (frame.streamId === 0n || flags !== 0 || credit !== 0 || fragmentLength !== 0 || hasAttributes || !hasPayload) throw new ProtocolViolationError("Invalid FRAGMENT frame shape.");
      return;
    case FrameKind.DEMAND:
      if (frame.streamId === 0n || flags !== 0 || credit <= 0 || fragmentLength !== 0 || hasAttributes || hasPayload) throw new ProtocolViolationError("Invalid DEMAND frame shape.");
      return;
    case FrameKind.COMPLETE:
      if (frame.streamId === 0n || flags !== 0 || credit !== 0 || fragmentLength !== 0 || hasAttributes || hasPayload) throw new ProtocolViolationError("Invalid COMPLETE frame shape.");
      return;
    case FrameKind.CANCEL:
      if (frame.streamId === 0n || flags !== 0 || credit !== 0 || fragmentLength !== 0 || hasAttributes) throw new ProtocolViolationError("Invalid CANCEL frame shape.");
      return;
    case FrameKind.ERROR:
      if (flags !== 0 || credit !== 0 || fragmentLength !== 0) throw new ProtocolViolationError("Invalid ERROR frame shape.");
      return;
    case FrameKind.SIGNAL:
      if (frame.streamId !== 0n || flags !== 0 || credit !== 0 || fragmentLength !== 0) throw new ProtocolViolationError("Invalid SIGNAL frame shape.");
      return;
  }
};

export const encodeFrame = (
  frame: ProtocolFrame,
  limits: Readonly<ProtocolLimits> = DEFAULT_PROTOCOL_LIMITS
): Uint8Array => {
  if (!isKnownKind(frame.kind)) throw new RangeError(`Unknown core frame kind: ${String(frame.kind)}.`);
  if (frame.streamId < 0n || frame.streamId > MAX_U64) throw new RangeError("streamId must fit unsigned 64 bits.");
  if (frame.sequence <= 0n || frame.sequence > MAX_U64) throw new RangeError("sequence must be between 1 and 2^64-1.");
  validateFrame(frame);
  const flags = frame.flags ?? 0;
  const headerValue = frame.kind === FrameKind.DATA && (flags & DATA_FRAGMENTED_FLAG) !== 0
    ? frame.fragmentLength ?? 0
    : frame.credit ?? 0;
  const attributes = encodeAttributes(frame.attributes ?? [], limits);
  const payload = frame.payload ?? new Uint8Array(0);
  const frameBytes = FRAME_HEADER_BYTES + attributes.length + payload.length;
  if (frameBytes > limits.maxFrameBytes) throw new RangeError(`Frame exceeds ${limits.maxFrameBytes} bytes.`);
  const output = new Uint8Array(frameBytes);
  const view = new DataView(output.buffer);
  view.setUint32(0, PROTOCOL_MAGIC);
  view.setUint8(4, PROTOCOL_MAJOR);
  view.setUint8(5, PROTOCOL_MINOR);
  view.setUint8(6, frame.kind);
  view.setUint8(7, flags);
  view.setUint16(8, FRAME_HEADER_BYTES);
  view.setBigUint64(10, frame.streamId);
  view.setBigUint64(18, frame.sequence);
  view.setUint32(26, headerValue);
  view.setUint32(30, attributes.length);
  view.setUint16(34, 0);
  output.set(attributes, FRAME_HEADER_BYTES);
  output.set(payload, FRAME_HEADER_BYTES + attributes.length);
  return output;
};

export const decodeFrame = (
  input: Uint8Array,
  limits: Readonly<ProtocolLimits> = DEFAULT_PROTOCOL_LIMITS
): ProtocolFrame => {
  if (input.length > limits.maxFrameBytes) throw new ProtocolViolationError(`Frame exceeds ${limits.maxFrameBytes} bytes.`);
  if (input.length < FRAME_HEADER_BYTES) throw new ProtocolViolationError("Frame is shorter than the PRP/1 core header.");
  const view = new DataView(input.buffer, input.byteOffset, input.byteLength);
  if (view.getUint32(0) !== PROTOCOL_MAGIC) throw new ProtocolViolationError("Invalid PRP/1 magic.");
  if (view.getUint8(4) !== PROTOCOL_MAJOR) throw new ProtocolViolationError(`Unsupported PRP major version ${view.getUint8(4)}.`);
  if (view.getUint8(5) !== PROTOCOL_MINOR) throw new ProtocolViolationError(`Unsupported PRP minor version ${view.getUint8(5)}.`);
  const kindValue = view.getUint8(6);
  if (!isKnownKind(kindValue)) throw new ProtocolViolationError(`Unknown core frame kind 0x${kindValue.toString(16)}.`);
  const flags = view.getUint8(7);
  if (view.getUint16(34) !== 0) throw new ProtocolViolationError("PRP/1 reserved header field must be zero.");
  const headerBytes = view.getUint16(8);
  if (headerBytes !== FRAME_HEADER_BYTES) throw new ProtocolViolationError(`PRP/1 core header length must be exactly ${FRAME_HEADER_BYTES} bytes.`);
  const sequence = view.getBigUint64(18);
  if (sequence === 0n) throw new ProtocolViolationError("PRP/1 peer sequence starts at 1.");
  const attributeBytes = view.getUint32(30);
  if (attributeBytes > limits.maxAttributeBytes || headerBytes + attributeBytes > input.length) throw new ProtocolViolationError("Invalid attribute area length.");
  const attributes = decodeAttributes(input.subarray(headerBytes, headerBytes + attributeBytes), limits);
  const payload = input.slice(headerBytes + attributeBytes);
  const headerValue = view.getUint32(26);
  const fragmentedData = kindValue === FrameKind.DATA && (flags & DATA_FRAGMENTED_FLAG) !== 0;
  const frame: ProtocolFrame = {
    kind: kindValue,
    streamId: view.getBigUint64(10),
    sequence,
    ...(flags === 0 ? {} : { flags }),
    ...(kindValue === FrameKind.DEMAND && headerValue !== 0 ? { credit: headerValue } : {}),
    ...(fragmentedData ? { fragmentLength: headerValue } : {}),
    ...(attributes.length === 0 ? {} : { attributes }),
    ...(payload.length === 0 ? {} : { payload })
  };
  if (headerValue !== 0 && kindValue !== FrameKind.DEMAND && !fragmentedData) throw new ProtocolViolationError(`${FrameKind[kindValue]} must not use the PRP/1 header value field.`);
  validateFrame(frame);
  return frame;
};
