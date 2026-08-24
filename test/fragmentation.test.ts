import { describe, expect, it } from "vitest";
import { attribute, attributeText } from "../src/core/attributes";
import { capabilitiesToAttributes } from "../src/core/capabilities";
import { decodeFrame, encodeFrame } from "../src/core/codec";
import { DATA_FRAGMENTED_FLAG, FrameKind } from "../src/core/frame";
import type { ProtocolLimits } from "../src/core/limits";
import { createRuntime } from "../src/core/runtime";
import { connectWithRuntime } from "../src/core/session";
import { AsyncQueue } from "../src/core/async-queue";
import { RpcPeer, rpcProfile } from "../src/profile/rpc/index";
import { createMemoryTransportPair } from "../src/transport/memory";
import type { ReactiveTransport, TransportConnection } from "../src/transport/types";

const smallLimits = {
  maxFrameBytes: 96,
  maxAttributeBytes: 48,
  maxAttributeIdBytes: 32,
  maxAttributeValueBytes: 32,
  maxInboundItemBytes: 4096,
  maxInFlightReassemblyBytes: 8192
} as const;

class ManualTransport implements ReactiveTransport {
  readonly id = "fragment-manual";
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

const createSlowTransportPair = (delayMs = 2): readonly [ReactiveTransport, ReactiveTransport] => {
  class Endpoint {
    readonly incoming = new AsyncQueue<Uint8Array>();
    peer?: Endpoint;
    claimed = false;
    closed = false;

    async write(frame: Uint8Array): Promise<void> {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      if (this.closed || !this.peer || this.peer.closed) throw new Error("slow transport is closed");
      this.peer.incoming.push(frame.slice());
    }

    close(): void {
      if (this.closed) return;
      this.closed = true;
      this.incoming.end();
    }
  }

  const left = new Endpoint();
  const right = new Endpoint();
  left.peer = right;
  right.peer = left;
  const transport = (endpoint: Endpoint, id: string): ReactiveTransport => ({
    id,
    async connect(): Promise<TransportConnection> {
      if (endpoint.claimed) throw new Error("slow endpoint already claimed");
      endpoint.claimed = true;
      let laneClaimed = false;
      return {
        description: { id, traits: ["reliable", "ordered", "message-boundaries"] },
        closed: new Promise(() => {}),
        async openLane() {
          if (laneClaimed) throw new Error("slow endpoint exposes one lane");
          laneClaimed = true;
          return {
            id: `${id}:0`,
            incoming: endpoint.incoming,
            write: (frame: Uint8Array) => endpoint.write(frame),
            close: () => endpoint.close()
          };
        },
        close: () => endpoint.close()
      };
    }
  });
  return [transport(left, "slow:left"), transport(right, "slow:right")];
};

const manualAcceptor = async (transport: ManualTransport, limits: Partial<ProtocolLimits> = smallLimits) => {
  const runtime = createRuntime({ limits });
  const sessionPromise = connectWithRuntime(transport, runtime, { origin: "acceptor" });
  transport.incoming.push(encodeFrame({
    kind: FrameKind.HELLO,
    streamId: 0n,
    sequence: 1n,
    attributes: [
      attribute("prp.session.id", "fragment-test", { required: true }),
      ...capabilitiesToAttributes(runtime.capabilities, runtime.policy)
    ]
  }));
  return { runtime, session: await sessionPromise };
};

describe("PRP/1 logical item fragmentation", () => {
  it("encodes fragmented DATA and FRAGMENT as distinct wire frames", () => {
    const data = encodeFrame({
      kind: FrameKind.DATA,
      streamId: 1n,
      sequence: 1n,
      flags: DATA_FRAGMENTED_FLAG,
      fragmentLength: 100,
      attributes: [attribute("kind", "large")],
      payload: new Uint8Array(20).fill(1)
    });
    expect(decodeFrame(data)).toMatchObject({
      kind: FrameKind.DATA,
      flags: DATA_FRAGMENTED_FLAG,
      fragmentLength: 100
    });

    const continuation = decodeFrame(encodeFrame({
      kind: FrameKind.FRAGMENT,
      streamId: 1n,
      sequence: 2n,
      payload: new Uint8Array(80).fill(2)
    }));
    expect(continuation.kind).toBe(FrameKind.FRAGMENT);
    expect(continuation.payload).toHaveLength(80);
  });

  it("reassembles a large payload transparently and preserves DATA attributes", async () => {
    const [left, right] = createMemoryTransportPair();
    const [server, client] = await Promise.all([
      connectWithRuntime(right, createRuntime({ limits: smallLimits }), { origin: "acceptor" }),
      connectWithRuntime(left, createRuntime({ limits: smallLimits }), { origin: "initiator" })
    ]);
    expect(client.supports("prp.core.fragmentation")).toBe(true);

    const local = await client.open();
    const remote = (await server[Symbol.asyncIterator]().next()).value!;
    const next = remote[Symbol.asyncIterator]().next();
    const payload = Uint8Array.from({ length: 1000 }, (_, index) => index % 251);
    await local.send(payload, [attribute("kind", "large")]);
    const message = await next;

    expect(message.done).toBe(false);
    expect(message.value!.data).toEqual(payload);
    expect(attributeText(message.value!.attributes, "kind")).toBe("large");
    await Promise.all([local.cancel(), remote.cancel(), client.close(), server.close()]);
  });

  it("keeps fragmentation transparent to the RPC profile", async () => {
    const rpcLimits = {
      maxFrameBytes: 160,
      maxAttributeBytes: 120,
      maxAttributeIdBytes: 32,
      maxAttributeValueBytes: 64,
      maxInboundItemBytes: 4096,
      maxInFlightReassemblyBytes: 8192
    } as const;
    const [left, right] = createMemoryTransportPair();
    const [serverSession, clientSession] = await Promise.all([
      connectWithRuntime(right, createRuntime({ limits: rpcLimits, extensions: [rpcProfile()] }), { origin: "acceptor" }),
      connectWithRuntime(left, createRuntime({ limits: rpcLimits, extensions: [rpcProfile()] }), { origin: "initiator" })
    ]);
    const server = new RpcPeer(serverSession).register<string, string>("echo", {
      requestResponse: async (input) => input
    });
    const client = new RpcPeer(clientSession);
    const payload = "x".repeat(2000);
    expect(await client.requestResponse<string, string>("echo", payload)).toBe(payload);
    client.dispose();
    server.dispose();
    await Promise.all([clientSession.close(), serverSession.close()]);
  });

  it("consumes demand per logical item rather than per fragment", async () => {
    const [left, right] = createMemoryTransportPair();
    const [server, client] = await Promise.all([
      connectWithRuntime(right, createRuntime({ limits: smallLimits }), { origin: "acceptor" }),
      connectWithRuntime(left, createRuntime({ limits: smallLimits }), { origin: "initiator" })
    ]);
    const local = await client.open();
    const remote = (await server[Symbol.asyncIterator]().next()).value!;
    const iterator = remote[Symbol.asyncIterator]();
    const firstRead = iterator.next();
    const firstPayload = new Uint8Array(700).fill(1);
    const secondPayload = new Uint8Array(700).fill(2);
    const firstSend = local.send(firstPayload);
    const secondSend = local.send(secondPayload);
    let secondSettled = false;
    void secondSend.then(() => { secondSettled = true; });

    await firstSend;
    const first = await firstRead;
    expect(first.value!.data).toEqual(firstPayload);
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(secondSettled).toBe(false);

    const secondRead = iterator.next();
    await secondSend;
    const second = await secondRead;
    expect(second.value!.data).toEqual(secondPayload);
    await Promise.all([local.cancel(), remote.cancel(), client.close(), server.close()]);
  });

  it("stops unsent fragments when the local stream is cancelled mid-item", async () => {
    const [left, right] = createSlowTransportPair();
    const [server, client] = await Promise.all([
      connectWithRuntime(right, createRuntime({ limits: smallLimits }), { origin: "acceptor" }),
      connectWithRuntime(left, createRuntime({ limits: smallLimits }), { origin: "initiator" })
    ]);
    const local = await client.open();
    const remote = (await server[Symbol.asyncIterator]().next()).value!;
    void remote[Symbol.asyncIterator]().next().catch(() => {});

    const sending = local.send(new Uint8Array(3000).fill(3));
    await new Promise((resolve) => setTimeout(resolve, 8));
    const cancelling = local.cancel("stop fragmented send");
    await expect(sending).rejects.toMatchObject({ code: "STREAM_CLOSED" });
    await cancelling;
    for (let attempt = 0; attempt < 100 && !remote.signal.aborted; attempt += 1) await new Promise((resolve) => setTimeout(resolve, 2));
    expect(remote.signal.aborted).toBe(true);
    expect(client.state).toBe("ready");
    expect(server.state).toBe("ready");
    await Promise.all([client.close(), server.close()]);
  });

  it("rejects an item larger than the peer advertised limit before sending DATA", async () => {
    const [left, right] = createMemoryTransportPair();
    const serverRuntime = createRuntime({ limits: {
      ...smallLimits,
      maxInboundItemBytes: 200,
      maxInFlightReassemblyBytes: 400
    } });
    const [server, client] = await Promise.all([
      connectWithRuntime(right, serverRuntime, { origin: "acceptor" }),
      connectWithRuntime(left, createRuntime({ limits: smallLimits }), { origin: "initiator" })
    ]);
    const local = await client.open();
    const remote = (await server[Symbol.asyncIterator]().next()).value!;
    const pending = remote[Symbol.asyncIterator]().next();

    await expect(local.send(new Uint8Array(201))).rejects.toMatchObject({ code: "ITEM_TOO_LARGE" });
    expect(client.state).toBe("ready");
    await local.send(new Uint8Array(199).fill(7));
    expect((await pending).value!.data).toHaveLength(199);
    await Promise.all([local.cancel(), remote.cancel(), client.close(), server.close()]);
  });

  it("treats COMPLETE before final fragment as a protocol violation", async () => {
    const transport = new ManualTransport();
    const { session } = await manualAcceptor(transport);
    transport.incoming.push(encodeFrame({ kind: FrameKind.OPEN, streamId: 1n, sequence: 2n }));
    const remote = (await session[Symbol.asyncIterator]().next()).value!;
    const pending = remote[Symbol.asyncIterator]().next();
    const pendingRejection = expect(pending).rejects.toMatchObject({ code: "PROTOCOL_VIOLATION" });

    transport.incoming.push(encodeFrame({
      kind: FrameKind.DATA,
      streamId: 1n,
      sequence: 3n,
      flags: DATA_FRAGMENTED_FLAG,
      fragmentLength: 10,
      payload: Uint8Array.of(1, 2, 3)
    }));
    transport.incoming.push(encodeFrame({ kind: FrameKind.COMPLETE, streamId: 1n, sequence: 4n }));

    for (let attempt = 0; attempt < 50 && session.state !== "detached"; attempt += 1) await new Promise((resolve) => setTimeout(resolve, 1));
    expect(session.state).toBe("detached");
    await pendingRejection;
  });

  it("discards in-flight fragments after local cancellation without detaching the session", async () => {
    const transport = new ManualTransport();
    const { session } = await manualAcceptor(transport);
    transport.incoming.push(encodeFrame({ kind: FrameKind.OPEN, streamId: 1n, sequence: 2n }));
    const remote = (await session[Symbol.asyncIterator]().next()).value!;
    void remote[Symbol.asyncIterator]().next().catch(() => {});
    transport.incoming.push(encodeFrame({
      kind: FrameKind.DATA,
      streamId: 1n,
      sequence: 3n,
      flags: DATA_FRAGMENTED_FLAG,
      fragmentLength: 10,
      payload: Uint8Array.of(1, 2, 3)
    }));
    await remote.cancel("stop");
    transport.incoming.push(encodeFrame({
      kind: FrameKind.FRAGMENT,
      streamId: 1n,
      sequence: 4n,
      payload: Uint8Array.of(4, 5, 6, 7, 8, 9, 10)
    }));
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(session.state).toBe("ready");
    await session.close();
  });

  it("fails only the affected stream when the dynamic reassembly budget is exhausted", async () => {
    const transport = new ManualTransport();
    const limits = {
      ...smallLimits,
      maxInboundItemBytes: 100,
      maxInFlightReassemblyBytes: 100,
      maxInboundStreams: 2,
      maxPendingIncomingStreams: 2
    };
    const { session } = await manualAcceptor(transport, limits);
    transport.incoming.push(encodeFrame({ kind: FrameKind.OPEN, streamId: 1n, sequence: 2n }));
    const first = (await session[Symbol.asyncIterator]().next()).value!;
    transport.incoming.push(encodeFrame({ kind: FrameKind.OPEN, streamId: 3n, sequence: 3n }));
    const second = (await session[Symbol.asyncIterator]().next()).value!;
    void first[Symbol.asyncIterator]().next().catch(() => {});
    void second[Symbol.asyncIterator]().next().catch(() => {});

    transport.incoming.push(encodeFrame({
      kind: FrameKind.DATA,
      streamId: 1n,
      sequence: 4n,
      flags: DATA_FRAGMENTED_FLAG,
      fragmentLength: 80,
      payload: Uint8Array.of(1)
    }));
    transport.incoming.push(encodeFrame({
      kind: FrameKind.DATA,
      streamId: 3n,
      sequence: 5n,
      flags: DATA_FRAGMENTED_FLAG,
      fragmentLength: 80,
      payload: Uint8Array.of(2)
    }));

    for (let attempt = 0; attempt < 50 && !second.signal.aborted; attempt += 1) await new Promise((resolve) => setTimeout(resolve, 1));
    expect(second.signal.aborted).toBe(true);
    expect(session.state).toBe("ready");
    await first.cancel();
    await session.close();
  });
});

it("enforces maxInboundItemBytes for unfragmented DATA too", async () => {
  const transport = new ManualTransport();
  const limits = {
    maxFrameBytes: 512,
    maxAttributeBytes: 128,
    maxAttributeIdBytes: 64,
    maxAttributeValueBytes: 64,
    maxInboundItemBytes: 100,
    maxInFlightReassemblyBytes: 200
  };
  const { session } = await manualAcceptor(transport, limits);
  transport.incoming.push(encodeFrame({ kind: FrameKind.OPEN, streamId: 1n, sequence: 2n }));
  const remote = (await session[Symbol.asyncIterator]().next()).value!;
  const pending = remote[Symbol.asyncIterator]().next();
  const pendingRejection = expect(pending).rejects.toMatchObject({ code: "PROTOCOL_VIOLATION" });
  await new Promise((resolve) => setTimeout(resolve, 0));
  transport.incoming.push(encodeFrame({
    kind: FrameKind.DATA,
    streamId: 1n,
    sequence: 3n,
    payload: new Uint8Array(200)
  }));
  for (let attempt = 0; attempt < 50 && session.state !== "detached"; attempt += 1) await new Promise((resolve) => setTimeout(resolve, 1));
  expect(session.state).toBe("detached");
  await pendingRejection;
});
