import { describe, expect, it } from "vitest";
import { createRuntime } from "../src/core/runtime";
import { connectWithRuntime, registerSignalAcceptor } from "../src/core/session";
import { attribute } from "../src/core/attributes";
import { createMemoryTransportPair } from "../src/transport/memory";

describe("protocol extensions", () => {
  it("attaches negotiated behavior before ready, dispatches signals, and disposes it", async () => {
    const [left, right] = createMemoryTransportPair();
    let attached = 0;
    let disposed = 0;
    let received = 0;
    const extension = {
      capability: { id: "example.signal", minVersion: 1, maxVersion: 1 },
      attach(session: Parameters<typeof registerSignalAcceptor>[0]) {
        attached += 1;
        const unregister = registerSignalAcceptor(session, {
          accepts: (signal) => signal.attributes.some((item) => item.id === "example.signal"),
          handle: () => { received += 1; }
        });
        return () => { unregister(); disposed += 1; };
      }
    };
    const [server, client] = await Promise.all([
      connectWithRuntime(right, createRuntime({ extensions: [extension] }), { origin: "acceptor" }),
      connectWithRuntime(left, createRuntime({ extensions: [extension] }), { origin: "initiator" })
    ]);
    expect(attached).toBe(2);
    await client.signal([attribute("example.signal", "yes")]);
    for (let attempt = 0; attempt < 50 && received === 0; attempt += 1) await new Promise((resolve) => setTimeout(resolve, 1));
    expect(received).toBe(1);
    await Promise.all([client.close(), server.close()]);
    expect(disposed).toBe(2);
  });
});
