import { describe, expect, it } from "vitest";
import { CORE_DATAGRAM_CAPABILITY_ID } from "../src/core/datagram";
import { createRuntime } from "../src/core/runtime";
import { connectWithRuntime } from "../src/core/session";
import { createMemoryTransportPair } from "../src/transport/memory";
import type { LaneRequirements, ReactiveTransport, TransportConnection } from "../src/transport/types";

const connectPair = async (
  leftRuntime = createRuntime(),
  rightRuntime = createRuntime()
) => {
  const [left, right] = createMemoryTransportPair();
  const [server, client] = await Promise.all([
    connectWithRuntime(right, rightRuntime, { origin: "acceptor" }),
    connectWithRuntime(left, leftRuntime, { origin: "initiator" })
  ]);
  return { client, server };
};

const reliableOnly = (transport: ReactiveTransport): ReactiveTransport => ({
  id: `${transport.id}:reliable-only`,
  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    const connection = await transport.connect(signal);
    return {
      description: { ...connection.description, traits: connection.description.traits.filter((trait) => !["best-effort", "unordered", "datagrams"].includes(trait)) },
      closed: connection.closed,
      supportsLane(requirements: LaneRequirements): boolean {
        return requirements.reliability === "reliable" && requirements.ordering === "ordered";
      },
      openLane: (requirements) => connection.openLane(requirements),
      close: (code, reason) => connection.close(code, reason)
    };
  }
});

describe("native PRP datagrams", () => {
  it("negotiates and transfers best-effort datagrams independently of reliable streams", async () => {
    const { client, server } = await connectPair();
    expect(client.supports(CORE_DATAGRAM_CAPABILITY_ID)).toBe(true);
    expect(server.supports(CORE_DATAGRAM_CAPABILITY_ID)).toBe(true);
    expect(client.maxDatagramBytes).toBeGreaterThan(0);

    const nextDatagram = server.datagrams[Symbol.asyncIterator]().next();
    await client.sendDatagram(Uint8Array.of(1, 2, 3, 4));
    await expect(nextDatagram).resolves.toEqual({ done: false, value: { data: Uint8Array.of(1, 2, 3, 4) } });

    // A datagram must not consume or disturb reliable PRP stream ids/sequences.
    const local = await client.open();
    const remote = await server[Symbol.asyncIterator]().next();
    expect(local.id).toBe(1n);
    expect(remote.value?.id).toBe(1n);

    await Promise.all([local.cancel(), remote.value!.cancel(), client.close(), server.close()]);
  });

  it("uses the peer-advertised maximum and rejects oversize datagrams before writing", async () => {
    const serverRuntime = createRuntime({ datagrams: { maxInboundBytes: 8 } });
    const { client, server } = await connectPair(createRuntime(), serverRuntime);

    expect(client.maxDatagramBytes).toBe(8);
    await client.sendDatagram(new Uint8Array(8));
    await expect(client.sendDatagram(new Uint8Array(9))).rejects.toThrow(/8 application bytes/);
    expect(client.state).toBe("ready");

    await Promise.all([client.close(), server.close()]);
  });

  it("does not advertise the datagram capability when the physical carrier has no best-effort lane", async () => {
    const [left, right] = createMemoryTransportPair();
    const [server, client] = await Promise.all([
      connectWithRuntime(right, createRuntime(), { origin: "acceptor" }),
      connectWithRuntime(reliableOnly(left), createRuntime(), { origin: "initiator" })
    ]);

    expect(client.supports(CORE_DATAGRAM_CAPABILITY_ID)).toBe(false);
    expect(server.supports(CORE_DATAGRAM_CAPABILITY_ID)).toBe(false);
    expect(client.maxDatagramBytes).toBe(0);
    await expect(client.sendDatagram(Uint8Array.of(1))).rejects.toMatchObject({ code: "DATAGRAM_UNAVAILABLE" });

    await Promise.all([client.close(), server.close()]);
  });

  it("fails negotiation when prp.core.datagram is explicitly required but the carrier cannot provide it", async () => {
    const [left, right] = createMemoryTransportPair();
    const required = createRuntime({ policy: { require: [CORE_DATAGRAM_CAPABILITY_ID] } });
    const result = await Promise.allSettled([
      connectWithRuntime(right, createRuntime(), { origin: "acceptor" }),
      connectWithRuntime(reliableOnly(left), required, { origin: "initiator" })
    ]);

    expect(result.some((entry) => entry.status === "rejected" && entry.reason?.code === "CAPABILITY_MISMATCH")).toBe(true);
  });
});
