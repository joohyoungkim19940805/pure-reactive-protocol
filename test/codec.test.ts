import { describe, expect, it } from "vitest";
import { attribute, bytes } from "../src/core/attributes";
import { decodeFrame, encodeFrame } from "../src/core/codec";
import { ProtocolViolationError } from "../src/core/errors";
import { FrameKind } from "../src/core/frame";
import { LengthPrefixedFrameDecoder, encodeLengthPrefixedFrame } from "../src/transport/framing";

describe("PRP/1 codec", () => {
  it("round-trips binary frames and optional attributes", () => {
    const encoded = encodeFrame({
      kind: FrameKind.DATA,
      streamId: 1n,
      sequence: 7n,
      attributes: [attribute("future.example/a", "present")],
      payload: Uint8Array.of(1, 2, 3)
    });
    expect(decodeFrame(encoded)).toMatchObject({ kind: FrameKind.DATA, streamId: 1n, sequence: 7n });
  });

  it("supports explicit session CLOSE and rejects malformed core headers", () => {
    const close = decodeFrame(encodeFrame({ kind: FrameKind.CLOSE, streamId: 0n, sequence: 1n, payload: new TextEncoder().encode("done") }));
    expect(close.kind).toBe(FrameKind.CLOSE);

    const reserved = encodeFrame({ kind: FrameKind.SIGNAL, streamId: 0n, sequence: 1n }).slice();
    reserved[7] = 1;
    expect(() => decodeFrame(reserved)).toThrow(ProtocolViolationError);

    const kind = encodeFrame({ kind: FrameKind.SIGNAL, streamId: 0n, sequence: 1n }).slice();
    kind[6] = 0xff;
    expect(() => decodeFrame(kind)).toThrow(ProtocolViolationError);

    const header = encodeFrame({ kind: FrameKind.SIGNAL, streamId: 0n, sequence: 1n }).slice();
    new DataView(header.buffer, header.byteOffset, header.byteLength).setUint16(8, 37);
    expect(() => decodeFrame(header)).toThrow(ProtocolViolationError);
  });

  it("rejects unknown attribute flag bits", () => {
    const encoded = encodeFrame({
      kind: FrameKind.SIGNAL,
      streamId: 0n,
      sequence: 1n,
      attributes: [attribute("x", "y")]
    }).slice();
    encoded[36] = 0x80;
    expect(() => decodeFrame(encoded)).toThrow(ProtocolViolationError);
  });

  it("rejects ill-formed JavaScript strings instead of silently replacing surrogates", () => {
    expect(() => bytes("\ud800")).toThrow(TypeError);
    expect(() => attribute("bad\ud800", "value")).toThrow(TypeError);
  });

  it("reconstructs a length-prefixed frame one byte at a time", () => {
    const source = encodeFrame({ kind: FrameKind.SIGNAL, streamId: 0n, sequence: 1n, payload: Uint8Array.of(9, 8, 7) });
    const framed = encodeLengthPrefixedFrame(source);
    const decoder = new LengthPrefixedFrameDecoder();
    const frames: Uint8Array[] = [];
    for (const value of framed) frames.push(...decoder.push(Uint8Array.of(value)));
    expect(frames).toHaveLength(1);
    expect(frames[0]).toEqual(source);
    expect(decoder.bufferedBytes).toBe(0);
  });
});
