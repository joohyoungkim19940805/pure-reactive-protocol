import { ProtocolViolationError } from "../core/errors";
import { DEFAULT_PROTOCOL_LIMITS } from "../core/limits";

export const encodeLengthPrefixedFrame = (frame: Uint8Array): Uint8Array => {
  if (frame.length === 0) throw new RangeError("Length-prefixed frame must not be empty.");
  if (frame.length > 0xffffffff) throw new RangeError("Length-prefixed frame must fit unsigned 32 bits.");
  const output = new Uint8Array(4 + frame.length);
  new DataView(output.buffer).setUint32(0, frame.length);
  output.set(frame, 4);
  return output;
};

export class LengthPrefixedFrameDecoder {
  private readonly prefix = new Uint8Array(4);
  private prefixBytes = 0;
  private frame: Uint8Array | undefined;
  private frameBytes = 0;

  constructor(private readonly maxFrameBytes = DEFAULT_PROTOCOL_LIMITS.maxFrameBytes) {
    if (!Number.isSafeInteger(maxFrameBytes) || maxFrameBytes <= 0 || maxFrameBytes > 0xffffffff) throw new RangeError("maxFrameBytes must be an integer between 1 and 2^32-1.");
  }

  get bufferedBytes(): number { return this.prefixBytes + this.frameBytes; }

  push(chunk: Uint8Array): Uint8Array[] {
    const frames: Uint8Array[] = [];
    let offset = 0;
    while (offset < chunk.length) {
      if (!this.frame) {
        while (this.prefixBytes < 4 && offset < chunk.length) this.prefix[this.prefixBytes++] = chunk[offset++]!;
        if (this.prefixBytes < 4) break;
        const length = new DataView(this.prefix.buffer).getUint32(0);
        this.prefixBytes = 0;
        if (length === 0) throw new ProtocolViolationError("Length-prefixed frame must not be empty.");
        if (length > this.maxFrameBytes) throw new ProtocolViolationError(`Length-prefixed frame exceeds ${this.maxFrameBytes} bytes.`);
        this.frame = new Uint8Array(length);
        this.frameBytes = 0;
      }
      const remaining = this.frame.length - this.frameBytes;
      const take = Math.min(remaining, chunk.length - offset);
      this.frame.set(chunk.subarray(offset, offset + take), this.frameBytes);
      this.frameBytes += take;
      offset += take;
      if (this.frameBytes === this.frame.length) {
        frames.push(this.frame);
        this.frame = undefined;
        this.frameBytes = 0;
      }
    }
    return frames;
  }
}
