import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { attribute } from "../src/core/attributes";
import { decodeFrame, encodeFrame } from "../src/core/codec";
import { CORE_COMPACT_FRAMES_CAPABILITY_ID, decodeCompactFrame, encodeCompactFrame, measureCompactAttributeBytes } from "../src/core/compact";
import { decodeDatagram, encodeDatagram } from "../src/core/datagram";
import { ProtocolViolationError } from "../src/core/errors";
import { FrameKind, type ProtocolFrame } from "../src/core/frame";
import { DEFAULT_PROTOCOL_LIMITS } from "../src/core/limits";
import { createRuntime } from "../src/core/runtime";
import { connectWithRuntime } from "../src/core/session";
import { createMemoryTransportPair } from "../src/transport/memory";
import type { ReactiveTransport, TransportLane } from "../src/transport/types";

interface Vector {
  name: string;
  kind: keyof typeof FrameKind;
  streamId: string;
  sequence: string;
  credit?: number;
  flags?: number;
  fragmentLength?: number;
  attributes?: Array<{ id: string; valueHex: string; required?: boolean }>;
  payloadHex?: string;
  wireHex: string;
}
const fixtures = JSON.parse(readFileSync(new URL("../compatibility/fixtures/compact-frames-v1.json", import.meta.url), "utf8")) as {
  vectors: Vector[]; nativeDatagram: { payloadHex: string; wireHex: string };
};
const hex = (value: Uint8Array) => Buffer.from(value).toString("hex");
const unhex = (value: string) => Uint8Array.from(Buffer.from(value, "hex"));
const frameFor = (vector: Vector): ProtocolFrame => ({
  kind: FrameKind[vector.kind], streamId: BigInt(vector.streamId), sequence: BigInt(vector.sequence),
  ...(vector.credit === undefined ? {} : { credit: vector.credit }),
  ...(vector.flags === undefined ? {} : { flags: vector.flags }),
  ...(vector.fragmentLength === undefined ? {} : { fragmentLength: vector.fragmentLength }),
  ...(vector.attributes === undefined ? {} : { attributes: vector.attributes.map(value => attribute(value.id, unhex(value.valueHex), value.required === undefined ? {} : { required: value.required })) }),
  ...(vector.payloadHex === undefined ? {} : { payload: unhex(vector.payloadHex) })
});

const recording = (transport: ReactiveTransport, frames: Uint8Array[], datagrams: Uint8Array[], lanes: TransportLane[]): ReactiveTransport => ({
  id: transport.id,
  async connect(signal) {
    const connection = await transport.connect(signal);
    return { ...connection, async openLane(requirements) {
      const lane = await connection.openLane(requirements);
      const saved = requirements?.reliability === "best-effort" ? datagrams : frames;
      lanes.push(lane);
      return { ...lane, async write(frame) { saved.push(frame.slice()); await lane.write(frame); } };
    } };
  }
});

describe("negotiated compact PRP frames", () => {
  for (const vector of fixtures.vectors) it(`matches the shared Java/TypeScript ${vector.name} fixture`, () => {
    const frame = frameFor(vector);
    expect(hex(encodeCompactFrame(frame))).toBe(vector.wireHex);
    expect(decodeCompactFrame(unhex(vector.wireHex))).toEqual(decodeFrame(encodeFrame(frame)));
  });

  it("keeps the unsigned 64-bit range without losing stream or sequence precision", () => {
    for (const value of [1n, 127n, 128n, 16383n, 16384n, (1n << 53n) + 1n, 0xffffffffffffffffn]) {
      const frame = { kind: FrameKind.DATA, streamId: value, sequence: value };
      expect(decodeCompactFrame(encodeCompactFrame(frame))).toEqual(frame);
    }
    expect(() => encodeCompactFrame({ kind: FrameKind.DATA, streamId: 1n << 64n, sequence: 1n })).toThrow();
    expect(() => encodeCompactFrame({ kind: FrameKind.DATA, streamId: 1n, sequence: 0n })).toThrow();
  });

  it("round-trips every post-handshake frame shape including fragmentation and errors", () => {
    const examples: ProtocolFrame[] = [
      { kind: FrameKind.PING, streamId: 0n, sequence: 2n, payload: new Uint8Array(8) },
      { kind: FrameKind.PONG, streamId: 0n, sequence: 3n, payload: new Uint8Array(8) },
      { kind: FrameKind.FRAGMENT, streamId: 1n, sequence: 4n, payload: Uint8Array.of(1) },
      { kind: FrameKind.COMPLETE, streamId: 1n, sequence: 5n },
      { kind: FrameKind.CANCEL, streamId: 1n, sequence: 6n, payload: Uint8Array.of(1) },
      { kind: FrameKind.ERROR, streamId: 1n, sequence: 7n, attributes: [attribute("prp.error.code", "error", { required: true })] }
    ];
    for (const frame of examples) expect(decodeCompactFrame(encodeCompactFrame(frame))).toEqual(decodeFrame(encodeFrame(frame)));
    for (const kind of [FrameKind.HELLO, FrameKind.WELCOME]) expect(() => encodeCompactFrame({ kind, streamId: 0n, sequence: 1n })).toThrow(ProtocolViolationError);
  });

  it("rejects truncated, overlong, overflowing and non-canonical control integers", () => {
    for (const wire of [
      "c11101", "c111800001", "c111018000", "c1110100", // truncated/non-minimal/zero sequence
      "c111ffffffffffffffffff0201", "c111018080808080808080808000", // u64 overflow
      "c15201018080808010", "c15201018000", "c152010100", // u32 overflow/non-minimal/zero header
      "c130010100", "c1300101010c00", // zero count/unknown dictionary code
      "c1300101010002fffe00", "c130010101000000", // invalid UTF-8/empty literal id
      "c1190101", "c1010001", "c1020001", "c1000001", // unknown kind/handshake
      "c151010201", "c191010201", // header for DATA without flag/fragment flag without total
      "c13001010102056869" // truncated value
    ]) expect(() => decodeCompactFrame(unhex(wire)), wire).toThrow(ProtocolViolationError);
  });

  it("enforces logical TLV budgets even for dictionary entries and checks physical frame bytes", () => {
    const frame = { kind: FrameKind.OPEN, streamId: 1n, sequence: 2n, attributes: [attribute("prp.rpc.target", "a")] };
    const encoded = encodeCompactFrame(frame);
    const limits = { ...DEFAULT_PROTOCOL_LIMITS, maxAttributeBytes: 20 };
    expect(() => encodeCompactFrame(frame, limits)).toThrow();
    expect(() => decodeCompactFrame(encoded, limits)).toThrow();
    expect(() => decodeCompactFrame(encoded, { ...DEFAULT_PROTOCOL_LIMITS, maxAttributeIdBytes: 3 })).toThrow();
    expect(() => decodeCompactFrame(encoded, { ...DEFAULT_PROTOCOL_LIMITS, maxFrameBytes: encoded.byteLength - 1 })).toThrow();
    expect(() => encodeCompactFrame(frame, { ...DEFAULT_PROTOCOL_LIMITS, maxFrameBytes: encoded.byteLength - 1 })).toThrow();
    expect(measureCompactAttributeBytes([])).toBe(0);
  });

  it("uses the shared one-byte native envelope only when explicitly selected", () => {
    const payload = unhex(fixtures.nativeDatagram.payloadHex);
    expect(hex(encodeDatagram(payload, 4, true))).toBe(fixtures.nativeDatagram.wireHex);
    expect(decodeDatagram(unhex(fixtures.nativeDatagram.wireHex), 3, true)).toEqual(payload);
    expect(() => decodeDatagram(encodeDatagram(payload), 3, true)).toThrow();
    expect(() => decodeDatagram(encodeDatagram(payload, 4, true), 3)).toThrow();
    expect(() => encodeDatagram(payload, 3, true)).toThrow();
    expect(() => decodeDatagram(Uint8Array.of(0xd1, 1, 2, 3), 2, true)).toThrow();
  });

  for (const peerCompact of [true, false]) it(`negotiates both directions and ${peerCompact ? "selects compact" : "falls back to legacy"} without changing demand or cancellation`, async () => {
    const [left, right] = createMemoryTransportPair();
    const leftFrames: Uint8Array[] = [], rightFrames: Uint8Array[] = [], leftDatagrams: Uint8Array[] = [], lanes: TransportLane[] = [];
    const [client, server] = await Promise.all([
      connectWithRuntime(recording(left, leftFrames, leftDatagrams, lanes), createRuntime({ compactFrames: true }), { origin: "initiator" }),
      connectWithRuntime(recording(right, rightFrames, [], []), createRuntime({ compactFrames: peerCompact }), { origin: "acceptor" })
    ]);
    try {
      expect(client.supports(CORE_COMPACT_FRAMES_CAPABILITY_ID)).toBe(peerCompact);
      expect(server.supports(CORE_COMPACT_FRAMES_CAPABILITY_ID)).toBe(peerCompact);
      expect(leftFrames[0]![0]).toBe(0x50); expect(rightFrames[0]![0]).toBe(0x50);
      const local = await client.open([attribute("prp.rpc.target", "test")]);
      const remote = (await server[Symbol.asyncIterator]().next()).value!;
      const response = remote[Symbol.asyncIterator]().next();
      await local.send(Uint8Array.of(3, 2, 1));
      expect((await response).value?.data).toEqual(Uint8Array.of(3, 2, 1));
      const nextDatagram = server.datagrams[Symbol.asyncIterator]().next();
      await client.sendDatagram(Uint8Array.of(9));
      expect((await nextDatagram).value?.data).toEqual(Uint8Array.of(9));
      expect(leftDatagrams[0]![0]).toBe(peerCompact ? 0xd1 : 0x50);
      await local.cancel("done");
      for (let attempt = 0; attempt < 20 && !remote.signal.aborted; attempt += 1) await new Promise(resolve => setTimeout(resolve, 1));
      expect(remote.signal.aborted).toBe(true);
      expect(leftFrames.slice(1).every(frame => frame[0] === (peerCompact ? 0xc1 : 0x50))).toBe(true);
      expect(rightFrames.slice(1).every(frame => frame[0] === (peerCompact ? 0xc1 : 0x50))).toBe(true);
    } finally { await Promise.all([client.close(), server.close()]); }
  });

  it("reassembles a fragmented item under compact headers without spending extra item credit", async () => {
    const [left, right] = createMemoryTransportPair();
    const runtime = () => createRuntime({ compactFrames: true, limits: {
      maxFrameBytes: 96, maxAttributeBytes: 48, maxAttributeIdBytes: 32, maxAttributeValueBytes: 32,
      maxInboundItemBytes: 4096, maxInFlightReassemblyBytes: 8192
    } });
    const [client, server] = await Promise.all([
      connectWithRuntime(left, runtime(), { origin: "initiator" }), connectWithRuntime(right, runtime(), { origin: "acceptor" })
    ]);
    try {
      const local = await client.open();
      const remote = (await server[Symbol.asyncIterator]().next()).value!;
      const next = remote[Symbol.asyncIterator]().next();
      const payload = Uint8Array.from({ length: 2048 }, (_, i) => i % 251);
      await local.send(payload, [attribute("kind", "large")]);
      expect((await next).value).toEqual({ data: payload, attributes: [attribute("kind", "large")] });
      await local.complete();
      expect(await remote[Symbol.asyncIterator]().next()).toMatchObject({ done: true });
    } finally { await Promise.all([client.close(), server.close()]); }
  });

  for (const compact of [true, false]) it(`rejects ${compact ? "legacy traffic after compact negotiation" : "compact traffic without negotiation"}`, async () => {
    const [left, right] = createMemoryTransportPair();
    const lanes: TransportLane[] = [];
    const [client, server] = await Promise.all([
      connectWithRuntime(recording(left, [], [], lanes), createRuntime({ compactFrames: true }), { origin: "initiator" }),
      connectWithRuntime(right, createRuntime({ compactFrames: compact }), { origin: "acceptor" })
    ]);
    try {
      const frame = { kind: FrameKind.OPEN, streamId: 1n, sequence: 2n };
      await lanes[0]!.write(compact ? encodeFrame(frame) : encodeCompactFrame(frame));
      for (let i = 0; i < 50 && server.state === "ready"; i++) await new Promise(resolve => setTimeout(resolve, 1));
      expect(server.state).toBe("detached");
    } finally { await Promise.all([client.close(), server.close()]); }
  });

  it("rejects compact traffic before HELLO even when the local runtime offers compact mode", async () => {
    const [left, right] = createMemoryTransportPair();
    const premature: ReactiveTransport = {
      id: left.id,
      async connect(signal) {
        const connection = await left.connect(signal);
        return { ...connection, async openLane(requirements) {
          const lane = await connection.openLane(requirements);
          return { ...lane, async write(frame) {
            await lane.write(frame[0] === 0x50 && frame[6] === FrameKind.HELLO
              ? encodeCompactFrame({ kind: FrameKind.DATA, streamId: 1n, sequence: 1n }) : frame);
          } };
        } };
      }
    };
    const results = await Promise.allSettled([
      connectWithRuntime(premature, createRuntime({ compactFrames: true, handshakeTimeoutMs: 100 }), { origin: "initiator" }),
      connectWithRuntime(right, createRuntime({ compactFrames: true, handshakeTimeoutMs: 100 }), { origin: "acceptor" })
    ]);
    expect(results.every(result => result.status === "rejected")).toBe(true);
    expect(results.some(result => result.status === "rejected" && result.reason instanceof ProtocolViolationError)).toBe(true);
  });

  it("preserves strict peer sequence validation for compact frames", async () => {
    const [left, right] = createMemoryTransportPair();
    const lanes: TransportLane[] = [];
    const [client, server] = await Promise.all([
      connectWithRuntime(recording(left, [], [], lanes), createRuntime({ compactFrames: true }), { origin: "initiator" }),
      connectWithRuntime(right, createRuntime({ compactFrames: true }), { origin: "acceptor" })
    ]);
    try {
      await client.open();
      await server[Symbol.asyncIterator]().next();
      await lanes[0]!.write(encodeCompactFrame({ kind: FrameKind.CANCEL, streamId: 1n, sequence: 2n }));
      for (let i = 0; i < 50 && server.state === "ready"; i++) await new Promise(resolve => setTimeout(resolve, 1));
      expect(server.state).toBe("detached");
    } finally { await Promise.all([client.close(), server.close()]); }
  });

  it("drops an incompatible native envelope without disturbing reliable compact streams", async () => {
    const [left, right] = createMemoryTransportPair();
    const lanes: TransportLane[] = [];
    const [client, server] = await Promise.all([
      connectWithRuntime(recording(left, [], [], lanes), createRuntime({ compactFrames: true }), { origin: "initiator" }),
      connectWithRuntime(right, createRuntime({ compactFrames: true }), { origin: "acceptor" })
    ]);
    try {
      const next = server.datagrams[Symbol.asyncIterator]().next();
      await lanes[1]!.write(encodeDatagram(Uint8Array.of(1)));
      await client.sendDatagram(Uint8Array.of(2));
      expect((await next).value?.data).toEqual(Uint8Array.of(2));
      expect(server.state).toBe("ready");
      await client.open();
      expect((await server[Symbol.asyncIterator]().next()).value?.id).toBe(1n);
    } finally { await Promise.all([client.close(), server.close()]); }
  });

  it("records exact layer costs instead of treating transport framing as game payload", () => {
    const data = { kind: FrameKind.DATA, streamId: 1n, sequence: 2n, payload: new Uint8Array(16) };
    const rpc = frameFor(fixtures.vectors.find(value => value.name === "rpc-open")!);
    const record = {
      reliableData16: { legacy: encodeFrame(data).byteLength, compact: encodeCompactFrame(data).byteLength },
      reliableRpcOpen: { legacy: encodeFrame(rpc).byteLength, compact: encodeCompactFrame(rpc).byteLength },
      nativePayload16: { legacy: encodeDatagram(new Uint8Array(16)).byteLength, compact: encodeDatagram(new Uint8Array(16), 1024, true).byteLength },
      unchangedCarrierLengthPrefixBytes: 4, unchangedRoutingEnvelopeBytes: 6
    };
    expect(record.reliableData16).toEqual({ legacy: 52, compact: 20 });
    console.info("PRP_COMPACT_LAYER_BYTES=" + JSON.stringify(record));
  });

  it("keeps exact layer accounting for every application payload size from 1 to 1100 bytes", () => {
    for (let size = 1; size <= 1100; size++) {
      const payload = new Uint8Array(size);
      const frame = { kind: FrameKind.DATA, streamId: 1n, sequence: 2n, payload };
      expect(encodeFrame(frame).byteLength).toBe(size + 36);
      expect(encodeCompactFrame(frame).byteLength).toBe(size + 4);
      const routed = new Uint8Array(size + 6);
      new DataView(routed.buffer).setUint32(0, 0x50525231);
      new DataView(routed.buffer).setUint16(4, 1);
      expect(encodeDatagram(routed).byteLength).toBe(size + 10);
      expect(encodeDatagram(routed, 2048, true).byteLength).toBe(size + 7);
    }
  });
});
