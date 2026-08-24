import { describe, expect, it } from "vitest";
import { accept, attribute, attributeText, connect } from "../src/index";
import { createRuntime } from "../src/core/runtime";
import { connectWithRuntime } from "../src/core/session";
import { PureReactiveProtocolError } from "../src/core/errors";
import { createMemoryTransportPair } from "../src/transport/memory";
import type { ReactiveTransport, TransportConnection } from "../src/transport/types";

const pair = async () => {
  const [left, right] = createMemoryTransportPair();
  const [server, client] = await Promise.all([accept(right), connect(left)]);
  return { client, server };
};

describe("PRP/1 core", () => {
  it("models request-response as a generic duplex stream with iterator-driven demand", async () => {
    const { client, server } = await pair();
    const serverTask = (async () => {
      const opened = await server[Symbol.asyncIterator]().next();
      const stream = opened.value!;
      expect(attributeText(stream.attributes, "example.kind")).toBe("echo");
      const input = await stream[Symbol.asyncIterator]().next();
      await stream.send(input.value!.data);
      await stream.complete();
    })();

    const stream = await client.open([attribute("example.kind", "echo")]);
    await stream.send(new TextEncoder().encode("hello"));
    await stream.complete();
    const response = await stream[Symbol.asyncIterator]().next();
    expect(new TextDecoder().decode(response.value!.data)).toBe("hello");
    await serverTask;
    await Promise.all([client.close(), server.close()]);
  });

  it("expresses a 0-to-N subscription without a dedicated frame kind", async () => {
    const { client, server } = await pair();
    const serverTask = (async () => {
      const opened = await server[Symbol.asyncIterator]().next();
      for (const value of ["a", "b", "c"]) await opened.value!.send(new TextEncoder().encode(value));
      await opened.value!.complete();
    })();
    const stream = await client.open();
    const values: string[] = [];
    for await (const message of stream) values.push(new TextDecoder().decode(message.data));
    expect(values).toEqual(["a", "b", "c"]);
    await serverTask;
    await Promise.all([client.close(), server.close()]);
  });

  it("requires an installed capability before policy can require it", () => {
    expect(() => createRuntime({ policy: { require: ["example.not-installed"] } })).toThrow(TypeError);
  });

  it("fails negotiation when an installed required capability is unavailable remotely", async () => {
    const [left, right] = createMemoryTransportPair();
    const runtime = createRuntime({
      extensions: [{
        capability: { id: "example.required", minVersion: 1, maxVersion: 1 },
        attach() {}
      }],
      policy: { require: ["example.required"] }
    });
    const result = await Promise.allSettled([
      accept(right),
      connectWithRuntime(left, runtime, { origin: "initiator" })
    ]);
    expect(result.some((entry) => entry.status === "rejected" && entry.reason?.code === "CAPABILITY_MISMATCH")).toBe(true);
  });

  it("times out a peer that never completes session negotiation", async () => {
    const never = async function* (): AsyncGenerator<Uint8Array> { await new Promise(() => {}); };
    const transport: ReactiveTransport = {
      id: "silent",
      async connect(): Promise<TransportConnection> {
        return {
          description: { id: "silent", traits: ["reliable", "ordered"] },
          closed: new Promise(() => {}),
          async openLane() { return { id: "silent:0", incoming: never(), async write() {}, close() {} }; },
          close() {}
        };
      }
    };
    await expect(connectWithRuntime(transport, createRuntime({ handshakeTimeoutMs: 5 }), { origin: "initiator" }))
      .rejects.toMatchObject({ code: "HANDSHAKE_TIMEOUT" });
  });

  it("does not consume a stream id when OPEN fails local validation", async () => {
    const [left, right] = createMemoryTransportPair();
    const limits = {
      maxFrameBytes: 1024,
      maxAttributeBytes: 512,
      maxAttributeIdBytes: 64,
      maxAttributeValueBytes: 128
    };
    const [server, client] = await Promise.all([
      connectWithRuntime(right, createRuntime({ limits }), { origin: "acceptor" }),
      connectWithRuntime(left, createRuntime({ limits }), { origin: "initiator" })
    ]);
    await expect(client.open([attribute("x", new Uint8Array(129))])).rejects.toThrow();
    const stream = await client.open();
    expect(stream.id).toBe(1n);
    const remote = await server[Symbol.asyncIterator]().next();
    expect(remote.value?.id).toBe(1n);
    await Promise.all([stream.cancel(), remote.value!.cancel(), client.close(), server.close()]);
  });

  it("honors the peer maxInboundStreams limit before sending another OPEN", async () => {
    const [left, right] = createMemoryTransportPair();
    const [server, client] = await Promise.all([
      connectWithRuntime(right, createRuntime({ limits: { maxInboundStreams: 1, maxPendingIncomingStreams: 1 } }), { origin: "acceptor" }),
      connect(left)
    ]);
    const first = await client.open();
    const remote = await server[Symbol.asyncIterator]().next();
    await expect(client.open()).rejects.toMatchObject({ code: "RESOURCE_EXHAUSTED" });
    await Promise.all([first.cancel(), remote.value!.cancel(), client.close(), server.close()]);
  });

  it("maps raw iterator return to protocol cancellation", async () => {
    const { client, server } = await pair();
    const local = await client.open();
    const remote = await server[Symbol.asyncIterator]().next();
    const iterator = local[Symbol.asyncIterator]();
    await iterator.return?.();
    for (let attempt = 0; attempt < 50 && !remote.value!.signal.aborted; attempt += 1) await new Promise((resolve) => setTimeout(resolve, 1));
    expect(remote.value!.signal.aborted).toBe(true);
    await Promise.all([client.close(), server.close()]);
  });
});
