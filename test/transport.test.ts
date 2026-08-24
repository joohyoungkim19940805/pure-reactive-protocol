import { describe, expect, it } from "vitest";
import { accept, connect } from "../src/core/session";
import { AutoTransport } from "../src/transport/auto";
import { createMemoryTransportPair } from "../src/transport/memory";
import type { ReactiveTransport, TransportConnection } from "../src/transport/types";

class DelayedTransport implements ReactiveTransport {
  readonly id = "delayed";
  activeWrites = 0;
  maxActiveWrites = 0;

  constructor(private readonly delegate: ReactiveTransport) {}

  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    const connection = await this.delegate.connect(signal);
    return {
      description: connection.description,
      closed: connection.closed,
      openLane: async (requirements) => {
        const lane = await connection.openLane(requirements);
        return {
          ...lane,
          write: async (frame) => {
            this.activeWrites += 1;
            this.maxActiveWrites = Math.max(this.maxActiveWrites, this.activeWrites);
            try {
              await new Promise((resolve) => setTimeout(resolve, 2));
              await lane.write(frame);
            } finally {
              this.activeWrites -= 1;
            }
          }
        };
      },
      close: (code, reason) => connection.close(code, reason)
    };
  }
}

describe("transport boundary", () => {
  it("serializes all physical writes at the session boundary", async () => {
    const [left, right] = createMemoryTransportPair();
    const delayed = new DelayedTransport(left);
    const [server, client] = await Promise.all([accept(right), connect(delayed)]);
    delayed.maxActiveWrites = 0;
    await Promise.all(Array.from({ length: 20 }, () => client.signal()));
    expect(delayed.maxActiveWrites).toBe(1);
    await Promise.all([client.close(), server.close()]);
  });

  it("does not fall through to another AutoTransport after cancellation", async () => {
    const controller = new AbortController();
    let fallbackUsed = false;
    const transport = new AutoTransport([
      {
        id: "blocking",
        priority: 100,
        isSupported: () => true,
        create: () => ({
          id: "blocking",
          connect: (signal) => new Promise((_resolve, reject) => {
            signal?.addEventListener("abort", () => reject(new Error("cancelled")), { once: true });
          })
        })
      },
      {
        id: "fallback",
        priority: 1,
        isSupported: () => true,
        create: () => ({
          id: "fallback",
          async connect() { fallbackUsed = true; throw new Error("must not run"); }
        })
      }
    ]);
    const reason = new Error("stop transport selection");
    const result = transport.connect(controller.signal);
    controller.abort(reason);
    await expect(result).rejects.toBe(reason);
    expect(fallbackUsed).toBe(false);
  });
});
