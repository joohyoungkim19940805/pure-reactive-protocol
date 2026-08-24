import { deferred } from "../core/async-queue";
import { DEFAULT_PROTOCOL_LIMITS } from "../core/limits";
import { encodeLengthPrefixedFrame, LengthPrefixedFrameDecoder } from "./framing";
import { IncomingFrameQueue } from "./incoming";
import { RELIABLE_ORDERED_LANE, type LaneRequirements, type ReactiveTransport, type TransportCloseEvent, type TransportConnection, type TransportDescription, type TransportLane } from "./types";

export interface ByteStreamEndpoint {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  close?(reason?: string): void | Promise<void>;
}

export interface ByteStreamTransportOptions {
  readonly id: string;
  readonly traits?: readonly string[];
  readonly open: (signal?: AbortSignal) => Promise<ByteStreamEndpoint>;
}

const assertRequirements = (requirements: LaneRequirements): void => {
  if (requirements.reliability !== "reliable" || requirements.ordering !== "ordered") throw new TypeError("The PRP base lane requires reliable + ordered delivery.");
};

export class ByteStreamTransport implements ReactiveTransport {
  readonly id: string;
  constructor(private readonly options: ByteStreamTransportOptions) { this.id = options.id; }

  static from(endpoint: ByteStreamEndpoint, options: Omit<ByteStreamTransportOptions, "open">): ByteStreamTransport {
    return new ByteStreamTransport({ ...options, open: async () => endpoint });
  }

  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    if (signal?.aborted) throw signal.reason ?? Object.assign(new Error("Aborted"), { name: "AbortError" });
    const endpoint = await this.options.open(signal);
    const reader = endpoint.readable.getReader();
    const writer = endpoint.writable.getWriter();
    const closed = deferred<TransportCloseEvent>();
    let laneClaimed = false;
    let didClose = false;
    let readStarted = false;
    let incoming: IncomingFrameQueue | undefined;

    const closeEndpoint = async (reason?: string): Promise<void> => {
      await Promise.resolve(endpoint.close?.(reason)).catch(() => {});
    };

    const startReadLoop = (maxFrameBytes = DEFAULT_PROTOCOL_LIMITS.maxFrameBytes): IncomingFrameQueue => {
      if (readStarted) throw new Error("Byte-stream read loop already started.");
      readStarted = true;
      incoming = new IncomingFrameQueue(maxFrameBytes);
      const decoder = new LengthPrefixedFrameDecoder(maxFrameBytes);
      void (async () => {
        try {
          while (true) {
            const result = await reader.read();
            if (result.done) break;
            for (const frame of decoder.push(result.value)) await incoming!.pushWait(frame);
          }
          incoming!.end();
          if (!didClose) closed.resolve({ reason: "stream-complete" });
        } catch (error) {
          incoming!.end(error);
          closed.resolve({ error });
          await Promise.resolve().then(() => writer.abort(error)).catch(() => {});
          await closeEndpoint("stream-error");
        } finally {
          try { reader.releaseLock(); } catch { /* already released */ }
        }
      })();
      return incoming;
    };

    const description: TransportDescription = {
      id: this.options.id,
      traits: Object.freeze(["reliable", "ordered", "byte-stream", ...(this.options.traits ?? [])])
    };

    let lane: TransportLane | undefined;
    return {
      description,
      closed: closed.promise,
      async openLane(requirements = RELIABLE_ORDERED_LANE): Promise<TransportLane> {
        assertRequirements(requirements);
        if (laneClaimed) throw new Error("This byte-stream connection exposes one base protocol lane.");
        laneClaimed = true;
        const queue = startReadLoop(requirements.maxFrameBytes);
        lane = {
          id: `${description.id}:0`,
          incoming: queue,
          write: async (frame) => writer.write(encodeLengthPrefixedFrame(frame)),
          close: async (reason) => {
            if (didClose) return;
            didClose = true;
            queue.end(undefined, true);
            await Promise.resolve().then(() => reader.cancel(reason)).catch(() => {});
            await writer.close().catch(() => {});
            await closeEndpoint(reason);
            closed.resolve(reason === undefined ? {} : { reason });
          }
        };
        return lane;
      },
      async close(_code?: number, reason?: string): Promise<void> {
        if (lane) await lane.close(reason);
        else {
          if (didClose) return;
          didClose = true;
          await Promise.resolve().then(() => reader.cancel(reason)).catch(() => {});
          await Promise.resolve().then(() => writer.abort(reason)).catch(() => {});
          await closeEndpoint(reason);
          closed.resolve(reason === undefined ? {} : { reason });
        }
      }
    };
  }
}
