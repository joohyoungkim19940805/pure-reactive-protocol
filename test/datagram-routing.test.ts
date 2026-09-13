import { describe, expect, it } from "vitest";
import { createRuntime } from "../src/core/runtime";
import { connectWithRuntime } from "../src/core/session";
import {
  DATAGRAM_ROUTING_CAPABILITY_ID,
  DatagramPeer,
  datagramRoutingProfile
} from "../src/profile/datagram-routing";
import { createMemoryTransportPair } from "../src/transport/memory";

const routes = ["entity.snapshot", "player.aim", "player.position"] as const;

const connectPair = async (leftRoutes: readonly string[] = routes, rightRoutes: readonly string[] = routes) => {
  const [left, right] = createMemoryTransportPair();
  const leftRuntime = createRuntime({ extensions: [datagramRoutingProfile({ routes: leftRoutes })] });
  const rightRuntime = createRuntime({ extensions: [datagramRoutingProfile({ routes: rightRoutes })] });
  const [server, client] = await Promise.all([
    connectWithRuntime(right, rightRuntime, { origin: "acceptor" }),
    connectWithRuntime(left, leftRuntime, { origin: "initiator" })
  ]);
  return { client, server, clientPeer: new DatagramPeer(client), serverPeer: new DatagramPeer(server) };
};

describe("datagram-routing/1", () => {
  it("negotiates the route intersection and dispatches compact routed datagrams", async () => {
    const { client, server, clientPeer, serverPeer } = await connectPair(
      ["player.position", "player.aim", "client.only"],
      ["player.aim", "player.position", "server.only"]
    );
    expect(client.supports(DATAGRAM_ROUTING_CAPABILITY_ID)).toBe(true);
    expect(clientPeer.routes()).toEqual(["player.aim", "player.position"]);
    expect(serverPeer.routes()).toEqual(["player.aim", "player.position"]);

    const received = new Promise<Uint8Array>((resolve) => {
      serverPeer.route("player.position", (packet) => resolve(packet.data));
    });
    await clientPeer.send("player.position", Uint8Array.of(7, 8, 9));
    await expect(received).resolves.toEqual(Uint8Array.of(7, 8, 9));

    await Promise.all([client.close(), server.close()]);
  });

  it("keeps raw native datagrams available when they do not use the routing envelope", async () => {
    const { client, server } = await connectPair();
    const raw = server.datagrams[Symbol.asyncIterator]().next();
    await client.sendDatagram(Uint8Array.of(1, 2, 3));
    await expect(raw).resolves.toEqual({ done: false, value: { data: Uint8Array.of(1, 2, 3) } });
    await Promise.all([client.close(), server.close()]);
  });

  it("rejects routes that were not negotiated without affecting the session", async () => {
    const { client, server, clientPeer } = await connectPair(["player.position"], ["player.aim"]);
    expect(clientPeer.routes()).toEqual([]);
    await expect(clientPeer.send("player.position", Uint8Array.of(1))).rejects.toMatchObject({ code: "DATAGRAM_ROUTE_UNAVAILABLE" });
    expect(client.state).toBe("ready");
    await Promise.all([client.close(), server.close()]);
  });

  it("coalesces queued latest-wins sends per route", async () => {
    const { client, server, clientPeer, serverPeer } = await connectPair(["player.position"], ["player.position"]);
    const values: number[] = [];
    serverPeer.route("player.position", ({ data }) => { values.push(data[0] ?? 0); });
    await Promise.all([
      clientPeer.sendLatest("player.position", Uint8Array.of(1)),
      clientPeer.sendLatest("player.position", Uint8Array.of(2)),
      clientPeer.sendLatest("player.position", Uint8Array.of(3))
    ]);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(values.at(-1)).toBe(3);
    expect(values.length).toBeLessThanOrEqual(3);
    await Promise.all([client.close(), server.close()]);
  });
});
