import { bytes, strictText, type ProtocolAttribute } from "./attributes";
import { validateFrame } from "./codec";
import { ProtocolViolationError } from "./errors";
import { DATA_FRAGMENTED_FLAG, FrameKind, type ProtocolFrame } from "./frame";
import { DEFAULT_PROTOCOL_LIMITS, type ProtocolLimits } from "./limits";

/** Optional session capability. HELLO/WELCOME always use the original PRP/1 codec. */
export const CORE_COMPACT_FRAMES_CAPABILITY_ID = "prp.core.compact-frames";
export const PRP_COMPACT_FRAME_MAGIC = 0xc1;

const HAS_ATTRIBUTES = 0x20;
const HAS_HEADER_VALUE = 0x40;
const FRAGMENTED = 0x80;
const MAX_U64 = 0xffffffffffffffffn;
// Index zero is the literal UTF-8 escape. These IDs are fixed by capability version 1.
const ATTRIBUTE_IDS = ["", "prp.profile", "prp.rpc.target", "prp.rpc.pattern", "prp.error.code", "prp.session.id"] as const;
const ATTRIBUTE_CODES = new Map<string, number>(ATTRIBUTE_IDS.slice(1).map((id, index) => [id, index + 1]));

const varintBytes = (value: bigint): number => {
  let count = 1;
  while (value >= 128n) { count += 1; value >>= 7n; }
  return count;
};

const writeVarint = (output: Uint8Array, at: number, value: bigint): number => {
  while (value >= 128n) {
    output[at++] = Number(value & 127n) | 128;
    value >>= 7n;
  }
  output[at++] = Number(value);
  return at;
};

interface PreparedAttribute {
  readonly code: number;
  readonly item: ProtocolAttribute;
  readonly literalId?: Uint8Array;
}

const prepareAttributes = (attributes: readonly ProtocolAttribute[], limits: Readonly<ProtocolLimits>) => {
  if (attributes.length > limits.maxAttributes) throw new RangeError(`Frame has more than ${limits.maxAttributes} attributes.`);
  let logicalBytes = 0;
  let encodedBytes = attributes.length === 0 ? 0 : varintBytes(BigInt(attributes.length));
  const prepared: PreparedAttribute[] = [];
  for (const item of attributes) {
    const code = ATTRIBUTE_CODES.get(item.id) ?? 0;
    const literalId = code === 0 ? bytes(item.id) : undefined;
    const idBytes = literalId?.byteLength ?? item.id.length;
    if (idBytes === 0 || idBytes > limits.maxAttributeIdBytes) throw new RangeError("Invalid attribute id length.");
    if (item.value.byteLength > limits.maxAttributeValueBytes) throw new RangeError(`Attribute value exceeds ${limits.maxAttributeValueBytes} bytes: ${item.id}`);
    // Apply the original TLV budget even when the dictionary reduces physical bytes.
    logicalBytes += 7 + idBytes + item.value.byteLength;
    if (logicalBytes > limits.maxAttributeBytes) throw new RangeError(`Attribute area exceeds ${limits.maxAttributeBytes} bytes.`);
    encodedBytes += varintBytes(BigInt(code * 2 + (item.required ? 1 : 0)))
      + (literalId === undefined ? 0 : varintBytes(BigInt(idBytes)) + idBytes)
      + varintBytes(BigInt(item.value.byteLength)) + item.value.byteLength;
    prepared.push({ code, item, ...(literalId === undefined ? {} : { literalId }) });
  }
  return { prepared, encodedBytes };
};

/** Includes the count prefix; still validates the uncompressed logical TLV limits. */
export const measureCompactAttributeBytes = (
  attributes: readonly ProtocolAttribute[],
  limits: Readonly<ProtocolLimits> = DEFAULT_PROTOCOL_LIMITS
): number => prepareAttributes(attributes, limits).encodedBytes;

export const encodeCompactFrame = (
  frame: ProtocolFrame,
  limits: Readonly<ProtocolLimits> = DEFAULT_PROTOCOL_LIMITS
): Uint8Array => {
  if (!Number.isInteger(frame.kind) || typeof FrameKind[frame.kind] !== "string"
      || frame.kind === FrameKind.HELLO || frame.kind === FrameKind.WELCOME) {
    throw new ProtocolViolationError("Compact frames require a known, non-handshake frame kind.");
  }
  if (typeof frame.streamId !== "bigint" || frame.streamId < 0n || frame.streamId > MAX_U64) throw new RangeError("streamId must fit unsigned 64 bits.");
  if (typeof frame.sequence !== "bigint" || frame.sequence <= 0n || frame.sequence > MAX_U64) throw new RangeError("sequence must be between 1 and 2^64-1.");
  validateFrame(frame);
  const fragmented = ((frame.flags ?? 0) & DATA_FRAGMENTED_FLAG) !== 0;
  const headerValue = fragmented ? frame.fragmentLength ?? 0 : frame.credit ?? 0;
  const attributes = prepareAttributes(frame.attributes ?? [], limits);
  const payload = frame.payload ?? new Uint8Array(0);
  const length = 2 + varintBytes(frame.streamId) + varintBytes(frame.sequence)
    + (headerValue === 0 ? 0 : varintBytes(BigInt(headerValue))) + attributes.encodedBytes + payload.byteLength;
  if (length > limits.maxFrameBytes) throw new RangeError(`Frame exceeds ${limits.maxFrameBytes} bytes.`);
  const output = new Uint8Array(length);
  output[0] = PRP_COMPACT_FRAME_MAGIC;
  output[1] = frame.kind | (attributes.prepared.length === 0 ? 0 : HAS_ATTRIBUTES)
    | (headerValue === 0 ? 0 : HAS_HEADER_VALUE) | (fragmented ? FRAGMENTED : 0);
  let at = writeVarint(output, 2, frame.streamId);
  at = writeVarint(output, at, frame.sequence);
  if (headerValue !== 0) at = writeVarint(output, at, BigInt(headerValue));
  if (attributes.prepared.length !== 0) {
    at = writeVarint(output, at, BigInt(attributes.prepared.length));
    for (const { code, item, literalId } of attributes.prepared) {
      at = writeVarint(output, at, BigInt(code * 2 + (item.required ? 1 : 0)));
      if (literalId !== undefined) {
        at = writeVarint(output, at, BigInt(literalId.byteLength));
        output.set(literalId, at); at += literalId.byteLength;
      }
      at = writeVarint(output, at, BigInt(item.value.byteLength));
      output.set(item.value, at); at += item.value.byteLength;
    }
  }
  output.set(payload, at);
  return output;
};

export const decodeCompactFrame = (
  input: Uint8Array,
  limits: Readonly<ProtocolLimits> = DEFAULT_PROTOCOL_LIMITS
): ProtocolFrame => {
  if (input.byteLength > limits.maxFrameBytes) throw new ProtocolViolationError(`Frame exceeds ${limits.maxFrameBytes} bytes.`);
  if (input.byteLength < 4 || input[0] !== PRP_COMPACT_FRAME_MAGIC) throw new ProtocolViolationError("Invalid compact PRP frame header.");
  let at = 2;
  const readVarint = (bits: 32 | 64): bigint => {
    const maximumBytes = bits === 32 ? 5 : 10;
    let value = 0n;
    for (let index = 0; index < maximumBytes; index += 1) {
      if (at >= input.byteLength) throw new ProtocolViolationError("Truncated compact unsigned varint.");
      const byte = input[at++]!;
      const digit = byte & 127;
      if (index === maximumBytes - 1 && digit > (bits === 32 ? 15 : 1)) throw new ProtocolViolationError("Compact unsigned varint overflow.");
      value |= BigInt(digit) << BigInt(index * 7);
      if ((byte & 128) === 0) {
        if (index > 0 && digit === 0) throw new ProtocolViolationError("Non-canonical compact unsigned varint.");
        return value;
      }
    }
    throw new ProtocolViolationError("Compact unsigned varint overflow.");
  };
  const control = input[1]!;
  const kind = control & 31;
  if (typeof FrameKind[kind] !== "string" || kind === FrameKind.HELLO || kind === FrameKind.WELCOME) {
    throw new ProtocolViolationError("Compact frames require a known, non-handshake frame kind.");
  }
  const streamId = readVarint(64);
  const sequence = readVarint(64);
  if (sequence === 0n) throw new ProtocolViolationError("PRP peer sequence starts at 1.");
  const headerValue = (control & HAS_HEADER_VALUE) === 0 ? 0 : Number(readVarint(32));
  if ((control & HAS_HEADER_VALUE) !== 0 && headerValue === 0) throw new ProtocolViolationError("Non-canonical zero compact header value.");
  const attributes: ProtocolAttribute[] = [];
  if ((control & HAS_ATTRIBUTES) !== 0) {
    const count = Number(readVarint(32));
    if (count === 0 || count > limits.maxAttributes) throw new ProtocolViolationError("Invalid compact attribute count.");
    let logicalBytes = 0;
    for (let index = 0; index < count; index += 1) {
      const tag = Number(readVarint(32));
      const code = Math.floor(tag / 2);
      let id: string;
      let idBytes: number;
      if (code === 0) {
        idBytes = Number(readVarint(32));
        if (idBytes === 0 || idBytes > limits.maxAttributeIdBytes || at + idBytes > input.byteLength) throw new ProtocolViolationError("Invalid compact attribute id length.");
        try { id = strictText(input.subarray(at, at + idBytes)); }
        catch (cause) { throw new ProtocolViolationError("Attribute id is not valid UTF-8.", { cause }); }
        at += idBytes;
      } else {
        const known = ATTRIBUTE_IDS[code];
        if (known === undefined) throw new ProtocolViolationError("Unknown compact attribute dictionary code.");
        id = known; idBytes = known.length;
        if (idBytes > limits.maxAttributeIdBytes) throw new ProtocolViolationError("Invalid compact attribute id length.");
      }
      const length = Number(readVarint(32));
      if (length > limits.maxAttributeValueBytes || at + length > input.byteLength) throw new ProtocolViolationError("Invalid compact attribute value length.");
      logicalBytes += 7 + idBytes + length;
      if (logicalBytes > limits.maxAttributeBytes) throw new ProtocolViolationError(`Attribute area exceeds ${limits.maxAttributeBytes} bytes.`);
      attributes.push({ id, value: input.slice(at, at + length), ...(tag % 2 === 1 ? { required: true } : {}) });
      at += length;
    }
  }
  const fragmented = (control & FRAGMENTED) !== 0;
  if (headerValue !== 0 && kind !== FrameKind.DEMAND && !(kind === FrameKind.DATA && fragmented)) {
    throw new ProtocolViolationError("Compact header value is not allowed for this frame kind.");
  }
  const frame: ProtocolFrame = {
    kind,
    streamId,
    sequence,
    ...(fragmented ? { flags: DATA_FRAGMENTED_FLAG, fragmentLength: headerValue } : {}),
    ...(kind === FrameKind.DEMAND && headerValue !== 0 ? { credit: headerValue } : {}),
    ...(attributes.length === 0 ? {} : { attributes }),
    ...(at === input.byteLength ? {} : { payload: input.slice(at) })
  };
  validateFrame(frame);
  return frame;
};
