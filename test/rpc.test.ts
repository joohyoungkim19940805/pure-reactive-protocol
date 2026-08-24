import { describe, expect, it } from "vitest";
import { createRuntime } from "../src/core/runtime";
import { connectWithRuntime } from "../src/core/session";
import { binaryCodec, jsonCodec, rpcProfile, RpcCapabilityError, RpcPeer } from "../src/profile/rpc";
import { createMemoryTransportPair } from "../src/transport/memory";

const rpcPair = async () => {
  const [left, right] = createMemoryTransportPair();
  const serverRuntime = createRuntime({ extensions: [rpcProfile()] });
  const clientRuntime = createRuntime({ extensions: [rpcProfile()] });
  const [serverSession, clientSession] = await Promise.all([
    connectWithRuntime(right, serverRuntime, { origin: "acceptor" }),
    connectWithRuntime(left, clientRuntime, { origin: "initiator" })
  ]);
  return { clientSession, serverSession, client: new RpcPeer(clientSession), server: new RpcPeer(serverSession) };
};

const collect = async <T>(source: AsyncIterable<T>): Promise<T[]> => {
  const values: T[] = [];
  for await (const value of source) values.push(value);
  return values;
};

describe("RPC profile", () => {
  it("requires the RPC capability and rejects codec-semantic mismatches", async () => {
    const [left, right] = createMemoryTransportPair();
    const plain = createRuntime();
    const rpc = createRuntime({ extensions: [rpcProfile()] });
    const [serverSession, clientSession] = await Promise.all([
      connectWithRuntime(right, plain, { origin: "acceptor" }),
      connectWithRuntime(left, rpc, { origin: "initiator" })
    ]);
    expect(() => new RpcPeer(clientSession)).toThrow(RpcCapabilityError);
    await Promise.all([clientSession.close(), serverSession.close()]);

    const [left2, right2] = createMemoryTransportPair();
    const jsonRuntime = createRuntime({ extensions: [rpcProfile()] });
    const binaryRuntime = createRuntime({ extensions: [rpcProfile({ codec: binaryCodec })] });
    const result = await Promise.allSettled([
      connectWithRuntime(right2, binaryRuntime, { origin: "acceptor" }),
      connectWithRuntime(left2, jsonRuntime, { origin: "initiator" })
    ]);
    expect(result.some((entry) => entry.status === "rejected")).toBe(true);
  });

  it("supports unary, notification, server-stream, and duplex patterns", async () => {
    const { client, server, clientSession, serverSession } = await rpcPair();
    let notified: unknown;
    server.register("echo", { requestResponse: async (input) => ({ input }) });
    server.register("notify", { fireAndForget: async (input) => { notified = input; } });
    server.register("numbers", { requestStream: async function* () { yield 1; yield 2; yield 3; } });
    server.register("double", { requestChannel: async function* (input) { for await (const value of input as AsyncIterable<number>) yield value * 2; } });

    await expect(client.requestResponse("echo", "hello")).resolves.toEqual({ input: "hello" });
    await client.fireAndForget("notify", { id: 7 });
    expect(notified).toEqual({ id: 7 });
    await expect(collect(client.requestStream("numbers", null))).resolves.toEqual([1, 2, 3]);
    await expect(collect(client.requestChannel<number, number>("double", (async function* () { yield 1; yield 2; yield 3; })())))
      .resolves.toEqual([2, 4, 6]);
    await Promise.all([clientSession.close(), serverSession.close()]);
  });

  it("cancels server streaming work when the consumer stops early", async () => {
    const { client, server, clientSession, serverSession } = await rpcPair();
    let finalized = false;
    server.register("infinite", {
      requestStream: async function* () {
        try { while (true) yield 0; }
        finally { finalized = true; }
      }
    });
    for await (const value of client.requestStream<number, number>("infinite", 0)) {
      expect(value).toBe(0);
      break;
    }
    for (let attempt = 0; attempt < 50 && !finalized; attempt += 1) await new Promise((resolve) => setTimeout(resolve, 1));
    expect(finalized).toBe(true);
    await Promise.all([clientSession.close(), serverSession.close()]);
  });

  it("supports standard AbortSignal cancellation for unary RPC", async () => {
    const { client, server, clientSession, serverSession } = await rpcPair();
    server.register("never", { requestResponse: async () => new Promise<never>(() => {}) });
    const controller = new AbortController();
    const response = client.requestResponse("never", null, { signal: controller.signal });
    controller.abort(new Error("cancelled by test"));
    await expect(response).rejects.toThrow("cancelled by test");
    await Promise.all([clientSession.close(), serverSession.close()]);
  });

  it("rejects empty RPC targets and malformed JSON profile payloads", async () => {
    const { client, server, clientSession, serverSession } = await rpcPair();
    expect(() => server.register("", {})).toThrow(TypeError);
    await expect(client.requestResponse("", null)).rejects.toBeInstanceOf(TypeError);
    expect(() => jsonCodec.encode(() => undefined)).toThrow(TypeError);
    expect(() => jsonCodec.decode(Uint8Array.of(0xff))).toThrow();
    await Promise.all([clientSession.close(), serverSession.close()]);
  });
});
