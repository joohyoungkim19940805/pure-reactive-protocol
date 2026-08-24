import { RSOCKET_MAX_FRAME_BYTES } from "./frame";
import { RSocketProtocolError } from "./codec";

export const encodeRSocketLengthPrefixedFrame = (frame: Uint8Array): Uint8Array => {
  if (frame.byteLength === 0 || frame.byteLength > RSOCKET_MAX_FRAME_BYTES) throw new RangeError("Invalid RSocket frame length.");
  const output = new Uint8Array(3 + frame.byteLength);
  output[0] = (frame.byteLength >>> 16) & 0xff;
  output[1] = (frame.byteLength >>> 8) & 0xff;
  output[2] = frame.byteLength & 0xff;
  output.set(frame, 3);
  return output;
};

export class RSocketLengthPrefixedDecoder {
  private readonly prefix = new Uint8Array(3);
  private prefixBytes = 0;
  private frame: Uint8Array | undefined;
  private frameBytes = 0;

  constructor(private readonly maxFrameBytes = RSOCKET_MAX_FRAME_BYTES) {
    if (!Number.isInteger(maxFrameBytes) || maxFrameBytes <= 0 || maxFrameBytes > RSOCKET_MAX_FRAME_BYTES) {
      throw new RangeError(`maxFrameBytes must be between 1 and ${RSOCKET_MAX_FRAME_BYTES}.`);
    }
  }

  push(chunk: Uint8Array): Uint8Array[] {
    const frames: Uint8Array[] = [];
    let offset = 0;
    while (offset < chunk.byteLength) {
      if (!this.frame) {
        while (this.prefixBytes < 3 && offset < chunk.byteLength) this.prefix[this.prefixBytes++] = chunk[offset++]!;
        if (this.prefixBytes < 3) break;
        const length = (this.prefix[0]! << 16) | (this.prefix[1]! << 8) | this.prefix[2]!;
        this.prefixBytes = 0;
        if (length === 0 || length > this.maxFrameBytes) throw new RSocketProtocolError(`RSocket 24-bit frame length exceeds ${this.maxFrameBytes} bytes.`);
        this.frame = new Uint8Array(length);
        this.frameBytes = 0;
      }
      const copy = Math.min(this.frame.byteLength - this.frameBytes, chunk.byteLength - offset);
      this.frame.set(chunk.subarray(offset, offset + copy), this.frameBytes);
      this.frameBytes += copy; offset += copy;
      if (this.frameBytes === this.frame.byteLength) {
        frames.push(this.frame);
        this.frame = undefined;
        this.frameBytes = 0;
      }
    }
    return frames;
  }
}
