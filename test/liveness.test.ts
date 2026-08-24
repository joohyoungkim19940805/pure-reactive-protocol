import { describe, expect, it } from "vitest";
import { decodeFrame } from "../src/core/codec";
import { FrameKind } from "../src/core/frame";
import { createRuntime } from "../src/core/runtime";
import { connectWithRuntime } from "../src/core/session";
import { createMemoryTransportPair } from "../src/transport/memory";
import type { ReactiveTransport } from "../src/transport/types";

const dropAfterWelcome = (delegate: ReactiveTransport): ReactiveTransport => ({
  id: `silent:${delegate.id}`,
  async connect(signal) {
    const connection = await delegate.connect(signal);
    return {
      ...connection,
      async openLane(requirements) {
        const lane = await connection.openLane(requirements);
        return {
          ...lane,
          incoming: {
            async *[Symbol.asyncIterator]() {
              let welcomeSeen = false;
              for await (const raw of lane.incoming) {
                const frame = decodeFrame(raw);
                if (!welcomeSeen) {
                  yield raw;
                  if (frame.kind === FrameKind.WELCOME) welcomeSeen = true;
                }
              }
            }
          }
        };
      }
    };
  }
});

describe("PRP liveness", () => {
  it("keeps an idle healthy session alive through PING/PONG", async () => {
    const [left, right] = createMemoryTransportPair();
    const runtimeA = createRuntime({ liveness: { intervalMs: 10, timeoutMs: 60 } });
    const runtimeB = createRuntime({ liveness: { intervalMs: 10, timeoutMs: 60 } });
    const [server, client] = await Promise.all([
      connectWithRuntime(right, runtimeB, { origin: "acceptor" }),
      connectWithRuntime(left, runtimeA, { origin: "initiator" })
    ]);
    await new Promise((resolve) => setTimeout(resolve, 90));
    expect(client.state).toBe("ready");
    expect(server.state).toBe("ready");
    await Promise.all([client.close(), server.close()]);
  });

  it("detaches a physically-open attachment that becomes silent", async () => {
    const [left, right] = createMemoryTransportPair();
    const runtimeA = createRuntime({ liveness: { intervalMs: 10, timeoutMs: 50 } });
    const runtimeB = createRuntime({ liveness: { intervalMs: 10, timeoutMs: 50 } });
    const [server, client] = await Promise.all([
      connectWithRuntime(right, runtimeB, { origin: "acceptor" }),
      connectWithRuntime(dropAfterWelcome(left), runtimeA, { origin: "initiator" })
    ]);
    await new Promise((resolve) => setTimeout(resolve, 90));
    expect(client.state).toBe("detached");
    await server.close().catch(() => {});
  });
});
