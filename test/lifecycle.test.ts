import { describe, expect, it } from "vitest";
import { attribute } from "../src/core/attributes";
import { capabilitiesToAttributes } from "../src/core/capabilities";
import { encodeFrame } from "../src/core/codec";
import { FrameKind } from "../src/core/frame";
import { createRuntime } from "../src/core/runtime";
import { connectWithRuntime } from "../src/core/session";
import { AsyncQueue } from "../src/core/async-queue";
import type { ReactiveTransport, TransportConnection } from "../src/transport/types";

class ManualTransport implements ReactiveTransport {
  readonly id = "manual";
  readonly incoming = new AsyncQueue<Uint8Array>();
  readonly writes: Uint8Array[] = [];

  async connect(): Promise<TransportConnection> {
    return {
      description: { id: this.id, traits: ["reliable", "ordered", "message-boundaries"] },
      closed: new Promise(() => {}),
      openLane: async () => ({
        id: "manual:0",
        incoming: this.incoming,
        write: async (frame) => { this.writes.push(frame.slice()); },
        close: () => { this.incoming.end(); }
      }),
      close: () => { this.incoming.end(); }
    };
  }
}

describe("session lifecycle invariants", () => {
  it("rejects remote stream-id reuse even after old tombstones are evicted", async () => {
    const transport = new ManualTransport();
    const runtime = createRuntime({ limits: { maxRetiredStreams: 1 } });
    const sessionPromise = connectWithRuntime(transport, runtime, { origin: "acceptor" });
    transport.incoming.push(encodeFrame({
      kind: FrameKind.HELLO,
      streamId: 0n,
      sequence: 1n,
      attributes: [
        attribute("prp.session.id", "manual-session", { required: true }),
        ...capabilitiesToAttributes(runtime.capabilities, runtime.policy)
      ]
    }));
    const session = await sessionPromise;

    transport.incoming.push(encodeFrame({ kind: FrameKind.OPEN, streamId: 1n, sequence: 2n }));
    const first = (await session[Symbol.asyncIterator]().next()).value!;
    await first.complete();
    transport.incoming.push(encodeFrame({ kind: FrameKind.COMPLETE, streamId: 1n, sequence: 3n }));

    transport.incoming.push(encodeFrame({ kind: FrameKind.OPEN, streamId: 3n, sequence: 4n }));
    const second = (await session[Symbol.asyncIterator]().next()).value!;
    await second.complete();
    transport.incoming.push(encodeFrame({ kind: FrameKind.COMPLETE, streamId: 3n, sequence: 5n }));

    transport.incoming.push(encodeFrame({ kind: FrameKind.OPEN, streamId: 1n, sequence: 6n }));
    for (let attempt = 0; attempt < 50 && session.state !== "detached"; attempt += 1) await new Promise((resolve) => setTimeout(resolve, 1));
    expect(session.state).toBe("detached");
  });
});
