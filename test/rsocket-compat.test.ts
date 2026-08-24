import { describe, expect, it } from "vitest";
import { RpcPeer } from "../src/profile/rpc";
import { connectRSocket, acceptRSocket } from "../src/compatibility/rsocket-v1/session";
import { RSocketByteStreamTransport } from "../src/compatibility/rsocket-v1/transport";
import { decodeRSocketFrame, encodeRSocketFrame } from "../src/compatibility/rsocket-v1/codec";
import { RSOCKET_FLAG_COMPLETE, RSOCKET_FLAG_NEXT, RSocketFrameType } from "../src/compatibility/rsocket-v1/frame";
import { createMemoryTransportPair } from "../src/transport/memory";

const collect = async <T>(source: AsyncIterable<T>): Promise<T[]> => {
  const values: T[] = [];
  for await (const value of source) values.push(value);
  return values;
};

describe("RSocket 1.0 compatibility", () => {
  it("round-trips standard frame headers and payload flags", () => {
    const encoded = encodeRSocketFrame({
      type: RSocketFrameType.PAYLOAD,
      streamId: 1,
      flags: RSOCKET_FLAG_NEXT | RSOCKET_FLAG_COMPLETE,
      data: Uint8Array.of(1, 2, 3)
    });
    expect(decodeRSocketFrame(encoded)).toMatchObject({ type: RSocketFrameType.PAYLOAD, streamId: 1 });
  });

  it("maps the same RpcPeer API onto all four RSocket interaction models in both directions", async () => {
    const [left, right] = createMemoryTransportPair();
    const [serverSession, clientSession] = await Promise.all([
      acceptRSocket(right, { keepAliveMs: 20, lifetimeMs: 300, maxFrameBytes: 128 }),
      connectRSocket(left, { keepAliveMs: 20, lifetimeMs: 300, maxFrameBytes: 128 })
    ]);
    const server = new RpcPeer(serverSession);
    const client = new RpcPeer(clientSession);
    server.register<any, any>("echo", {
      requestResponse: (value: number) => value + 1,
      fireAndForget: async () => {},
      requestStream: async function* (count: number) { for (let i = 0; i < count; i += 1) yield "x".repeat(200) + i; },
      requestChannel: async function* (input: AsyncIterable<number>) { for await (const value of input) yield value * 2; }
    });
    client.register<any, any>("reverse", { requestResponse: (value: number) => value * 3 });

    await expect(client.requestResponse("echo", 1)).resolves.toBe(2);
    await expect(server.requestResponse("reverse", 4)).resolves.toBe(12);
    await expect(collect(client.requestStream("echo", 3))).resolves.toHaveLength(3);
    await client.fireAndForget("echo", null);
    await expect(collect(client.requestChannel("echo", (async function* () { yield 1; yield 2; yield 3; })())))
      .resolves.toEqual([2, 4, 6]);
    await Promise.all([clientSession.close(), serverSession.close()]);
  });

  it("uses the RSocket 24-bit framing transport for ordered byte streams", async () => {
    const ab = new TransformStream<Uint8Array, Uint8Array>();
    const ba = new TransformStream<Uint8Array, Uint8Array>();
    const left = RSocketByteStreamTransport.from({ readable: ba.readable, writable: ab.writable }, { id: "left" });
    const right = RSocketByteStreamTransport.from({ readable: ab.readable, writable: ba.writable }, { id: "right" });
    const [serverSession, clientSession] = await Promise.all([
      acceptRSocket(right, { keepAliveMs: 50, lifetimeMs: 500, maxFrameBytes: 96 }),
      connectRSocket(left, { keepAliveMs: 50, lifetimeMs: 500, maxFrameBytes: 96 })
    ]);
    const server = new RpcPeer(serverSession);
    const client = new RpcPeer(clientSession);
    server.register("large", { requestResponse: (value) => value });
    const value = { text: "z".repeat(2_000) };
    await expect(client.requestResponse("large", value)).resolves.toEqual(value);
    await Promise.all([clientSession.close(), serverSession.close()]);
  });
});
