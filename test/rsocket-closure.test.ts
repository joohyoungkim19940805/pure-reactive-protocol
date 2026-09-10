import { describe, expect, it } from "vitest";
import { AsyncQueue } from "../src/core/async-queue";
import type { ReactiveTransport, TransportConnection } from "../src/transport/types";
import { RpcPeer, binaryCodec } from "../src/profile/rpc";
import { connectRSocket, acceptRSocket } from "../src/compatibility/rsocket-v1/session";
import { decodeRSocketFrame, defaultRSocketSetup, encodeRSocketFrame, RSocketProtocolError } from "../src/compatibility/rsocket-v1/codec";
import {
  RSOCKET_FLAG_COMPLETE,
  RSOCKET_FLAG_FOLLOWS,
  RSOCKET_FLAG_IGNORE,
  RSOCKET_FLAG_NEXT,
  RSOCKET_FLAG_RESPOND,
  RSocketFrameType
} from "../src/compatibility/rsocket-v1/frame";
import { RSOCKET_COMPOSITE_METADATA_MIME } from "../src/compatibility/rsocket-v1/metadata";
import { createMemoryTransportPair } from "../src/transport/memory";

class ManualRSocketTransport implements ReactiveTransport {
  readonly id = "rsocket-closure-manual";
  readonly incoming = new AsyncQueue<Uint8Array>();
  readonly writes: Uint8Array[] = [];

  async connect(): Promise<TransportConnection> {
    return {
      description: { id: this.id, traits: ["reliable", "ordered", "message-boundaries"] },
      closed: new Promise(() => {}),
      openLane: async () => ({
        id: `${this.id}:0`,
        incoming: this.incoming,
        write: async (frame) => { this.writes.push(frame.slice()); },
        close: () => { this.incoming.end(); }
      }),
      close: () => { this.incoming.end(); }
    };
  }
}

const setupFrame = (keepAliveMs = 20, lifetimeMs = 200): Uint8Array => encodeRSocketFrame({
  type: RSocketFrameType.SETUP,
  streamId: 0,
  setup: {
    ...defaultRSocketSetup(),
    keepAliveMs,
    lifetimeMs,
    metadataMimeType: RSOCKET_COMPOSITE_METADATA_MIME,
    dataMimeType: "application/json"
  }
});

const acceptManual = async (transport: ManualRSocketTransport, options: Parameters<typeof acceptRSocket>[1] = {}) => {
  const promise = acceptRSocket(transport, { keepAliveMs: 20, lifetimeMs: 200, ...options });
  transport.incoming.push(setupFrame(20, 200));
  return promise;
};

const waitFor = async (predicate: () => boolean, attempts = 100): Promise<void> => {
  for (let i = 0; i < attempts && !predicate(); i += 1) await new Promise((resolve) => setTimeout(resolve, 1));
  if (!predicate()) throw new Error("condition was not reached");
};

describe("RSocket alpha closure conformance", () => {
  it("sets NEXT on every fragmented PAYLOAD continuation", async () => {
    const transport = new ManualRSocketTransport();
    const session = await connectRSocket(transport, {
      codec: binaryCodec,
      keepAliveMs: 1000,
      lifetimeMs: 5000,
      maxFrameBytes: 128,
      maxItemBytes: 2048
    });
    const rpc = new RpcPeer(session);
    const pending = rpc.requestResponse<Uint8Array, Uint8Array>("fragment-wire", new Uint8Array(512));
    const completion = expect(pending).resolves.toEqual(Uint8Array.of(7));

    await waitFor(() => transport.writes.map(decodeRSocketFrame).some((frame) => frame.type === RSocketFrameType.PAYLOAD));
    const written = transport.writes.map(decodeRSocketFrame);
    const request = written.find((frame) => frame.type === RSocketFrameType.REQUEST_RESPONSE)!;
    const continuations = written.filter((frame) => frame.streamId === request.streamId && frame.type === RSocketFrameType.PAYLOAD);
    expect(continuations.length).toBeGreaterThan(0);
    for (const fragment of continuations) expect((fragment.flags ?? 0) & RSOCKET_FLAG_NEXT).toBe(RSOCKET_FLAG_NEXT);

    transport.incoming.push(encodeRSocketFrame({
      type: RSocketFrameType.PAYLOAD,
      streamId: request.streamId,
      flags: RSOCKET_FLAG_NEXT | RSOCKET_FLAG_COMPLETE,
      data: Uint8Array.of(7)
    }));
    await completion;
    await session.close();
  });

  it("rejects PAYLOAD frames that set neither NEXT nor COMPLETE", () => {
    expect(() => encodeRSocketFrame({
      type: RSocketFrameType.PAYLOAD,
      streamId: 1,
      flags: RSOCKET_FLAG_FOLLOWS,
      data: Uint8Array.of(1)
    })).toThrow(/NEXT, COMPLETE/);

    const invalid = new Uint8Array(7);
    const view = new DataView(invalid.buffer);
    view.setUint32(0, 1);
    view.setUint16(4, (RSocketFrameType.PAYLOAD << 10) | RSOCKET_FLAG_FOLLOWS);
    invalid[6] = 1;
    expect(() => decodeRSocketFrame(invalid)).toThrow(RSocketProtocolError);
  });
  it("fragments logical payloads larger than the 24-bit RSocket frame ceiling without pre-encoding them", async () => {
    const [left, right] = createMemoryTransportPair();
    const options = { codec: binaryCodec, keepAliveMs: 1000, lifetimeMs: 5000, maxFrameBytes: 1024 * 1024, maxItemBytes: 20 * 1024 * 1024 } as const;
    const [serverSession, clientSession] = await Promise.all([acceptRSocket(right, options), connectRSocket(left, options)]);
    const server = new RpcPeer(serverSession);
    const client = new RpcPeer(clientSession);
    server.register<Uint8Array, Uint8Array>("large", { requestResponse: (value) => value });
    const value = new Uint8Array(0x01000000 + 1024);
    value[0] = 1;
    value[value.length - 1] = 2;
    const response = await client.requestResponse<Uint8Array, Uint8Array>("large", value);
    expect(response.byteLength).toBe(value.byteLength);
    expect(response[0]).toBe(1);
    expect(response[response.length - 1]).toBe(2);
    await Promise.all([clientSession.close(), serverSession.close()]);
  }, 30_000);

  it("allows CANCEL to terminate an unfinished fragmented request without detaching the connection", async () => {
    const transport = new ManualRSocketTransport();
    const session = await acceptManual(transport);
    transport.incoming.push(encodeRSocketFrame({
      type: RSocketFrameType.REQUEST_RESPONSE,
      streamId: 1,
      flags: RSOCKET_FLAG_FOLLOWS,
      data: new Uint8Array(32)
    }));
    transport.incoming.push(encodeRSocketFrame({ type: RSocketFrameType.CANCEL, streamId: 1 }));
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(session.state).toBe("ready");
    await session.close();
  });

  it("treats PAYLOAD F+C as complete and ignores Follows", async () => {
    const transport = new ManualRSocketTransport();
    const session = await connectRSocket(transport, { keepAliveMs: 1000, lifetimeMs: 5000 });
    const rpc = new RpcPeer(session);
    const pending = rpc.requestResponse<number, number>("echo", 1);
    await waitFor(() => transport.writes.some((raw) => decodeRSocketFrame(raw).type === RSocketFrameType.REQUEST_RESPONSE));
    const request = transport.writes.map(decodeRSocketFrame).find((frame) => frame.type === RSocketFrameType.REQUEST_RESPONSE)!;
    transport.incoming.push(encodeRSocketFrame({
      type: RSocketFrameType.PAYLOAD,
      streamId: request.streamId,
      flags: RSOCKET_FLAG_FOLLOWS | RSOCKET_FLAG_NEXT | RSOCKET_FLAG_COMPLETE,
      data: new TextEncoder().encode("2")
    }));
    await expect(pending).resolves.toBe(2);
    expect(session.state).toBe("ready");
    await session.close();
  });

  it("ignores unknown frame types only when the RSocket IGNORE flag is set", () => {
    const ignored = new Uint8Array(6);
    const view = new DataView(ignored.buffer);
    view.setUint32(0, 0);
    view.setUint16(4, (0x30 << 10) | RSOCKET_FLAG_IGNORE);
    expect(decodeRSocketFrame(ignored)).toMatchObject({ type: 0x30, flags: RSOCKET_FLAG_IGNORE });

    const rejected = ignored.slice();
    new DataView(rejected.buffer).setUint16(4, 0x30 << 10);
    expect(() => decodeRSocketFrame(rejected)).toThrow(RSocketProtocolError);
  });

  it("sends client KEEPALIVE periodically even while unrelated inbound traffic is arriving", async () => {
    const transport = new ManualRSocketTransport();
    const session = await connectRSocket(transport, { keepAliveMs: 15, lifetimeMs: 150 });
    const traffic = setInterval(() => {
      transport.incoming.push(encodeRSocketFrame({ type: RSocketFrameType.METADATA_PUSH, streamId: 0, metadata: Uint8Array.of(1) }));
    }, 4);
    await new Promise((resolve) => setTimeout(resolve, 50));
    clearInterval(traffic);
    const keepalives = transport.writes.map(decodeRSocketFrame).filter((frame) => frame.type === RSocketFrameType.KEEPALIVE && ((frame.flags ?? 0) & RSOCKET_FLAG_RESPOND) !== 0);
    expect(keepalives.length).toBeGreaterThanOrEqual(2);
    expect(session.state).toBe("ready");
    await session.close();
  });

  it("rejects skipped remote requester stream ids", async () => {
    const transport = new ManualRSocketTransport();
    const session = await acceptManual(transport);
    transport.incoming.push(encodeRSocketFrame({ type: RSocketFrameType.REQUEST_RESPONSE, streamId: 1, data: Uint8Array.of(1) }));
    await new Promise((resolve) => setTimeout(resolve, 2));
    transport.incoming.push(encodeRSocketFrame({ type: RSocketFrameType.REQUEST_RESPONSE, streamId: 5, data: Uint8Array.of(1) }));
    await waitFor(() => session.state === "detached");
    expect(session.state).toBe("detached");
  });

  it("does not consume a wire stream id when the first local request fails before writing", async () => {
    const transport = new ManualRSocketTransport();
    const session = await connectRSocket(transport, { codec: binaryCodec, keepAliveMs: 1000, lifetimeMs: 5000, maxItemBytes: 100 });
    const rpc = new RpcPeer(session);
    await expect(rpc.requestResponse<Uint8Array, Uint8Array>("echo", new Uint8Array(101))).rejects.toMatchObject({ code: "ITEM_TOO_LARGE" });

    const pending = rpc.requestResponse<Uint8Array, Uint8Array>("echo", Uint8Array.of(1));
    await waitFor(() => transport.writes.map(decodeRSocketFrame).some((frame) => frame.type === RSocketFrameType.REQUEST_RESPONSE));
    const request = transport.writes.map(decodeRSocketFrame).find((frame) => frame.type === RSocketFrameType.REQUEST_RESPONSE)!;
    expect(request.streamId).toBe(1);
    transport.incoming.push(encodeRSocketFrame({ type: RSocketFrameType.PAYLOAD, streamId: 1, flags: RSOCKET_FLAG_NEXT | RSOCKET_FLAG_COMPLETE, data: Uint8Array.of(2) }));
    await expect(pending).resolves.toEqual(Uint8Array.of(2));
    await session.close();
  });

  it("bounds aggregate fragmented-sequence memory and rejects only the overflowing stream", async () => {
    const transport = new ManualRSocketTransport();
    const session = await acceptManual(transport, { maxFrameBytes: 128, maxItemBytes: 1000, maxInFlightReassemblyBytes: 150 });
    transport.incoming.push(encodeRSocketFrame({ type: RSocketFrameType.REQUEST_RESPONSE, streamId: 1, flags: RSOCKET_FLAG_FOLLOWS, data: new Uint8Array(100) }));
    transport.incoming.push(encodeRSocketFrame({ type: RSocketFrameType.REQUEST_RESPONSE, streamId: 3, flags: RSOCKET_FLAG_FOLLOWS, data: new Uint8Array(100) }));
    await waitFor(() => transport.writes.map(decodeRSocketFrame).some((frame) => frame.type === RSocketFrameType.ERROR && frame.streamId === 3));
    expect(session.state).toBe("ready");
    await session.close();
  });

  it("rejects high reserved bits in unsigned 31-bit RSocket fields", () => {
    const requestN = encodeRSocketFrame({ type: RSocketFrameType.REQUEST_N, streamId: 1, requestN: 1 });
    new DataView(requestN.buffer, requestN.byteOffset, requestN.byteLength).setUint32(6, 0x80000001);
    expect(() => decodeRSocketFrame(requestN)).toThrow(RSocketProtocolError);

    const setup = setupFrame();
    new DataView(setup.buffer, setup.byteOffset, setup.byteLength).setUint32(10, 0x80000001);
    expect(() => decodeRSocketFrame(setup)).toThrow(RSocketProtocolError);
  });
});
