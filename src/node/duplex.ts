import type { Duplex } from "node:stream";
import { deferred } from "../core/async-queue";
import { DEFAULT_PROTOCOL_LIMITS } from "../core/limits";
import { encodeLengthPrefixedFrame, LengthPrefixedFrameDecoder } from "../transport/framing";
import { IncomingFrameQueue } from "../transport/incoming";
import { RELIABLE_ORDERED_LANE, type ReactiveTransport, type TransportCloseEvent, type TransportConnection, type TransportLane } from "../transport/types";

export interface NodeDuplexTransportOptions {
  readonly id: string;
  readonly traits?: readonly string[];
  readonly open: (signal?: AbortSignal) => Promise<Duplex>;
  readonly close?: (reason?: string) => void | Promise<void>;
}

export class NodeDuplexTransport implements ReactiveTransport {
  readonly id: string;
  constructor(private readonly options: NodeDuplexTransportOptions) { this.id = options.id; }

  static from(stream: Duplex, options: Omit<NodeDuplexTransportOptions, "open">): NodeDuplexTransport {
    return new NodeDuplexTransport({ ...options, open: async () => stream });
  }

  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    if (signal?.aborted) throw signal.reason ?? Object.assign(new Error("Aborted"), { name: "AbortError" });
    const stream = await this.options.open(signal);
    const closeHook = this.options.close;
    const closed = deferred<TransportCloseEvent>();
    let claimed = false;
    let didClose = false;
    let incoming: IncomingFrameQueue | undefined;
    let processing = Promise.resolve();
    stream.pause();

    const startReadLoop = (maxFrameBytes = DEFAULT_PROTOCOL_LIMITS.maxFrameBytes): IncomingFrameQueue => {
      incoming = new IncomingFrameQueue(maxFrameBytes);
      const decoder = new LengthPrefixedFrameDecoder(maxFrameBytes);
      stream.on("data", (chunk: Buffer | Uint8Array) => {
        stream.pause();
        const value = new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength).slice();
        processing = processing.then(async () => {
          for (const frame of decoder.push(value)) await incoming!.pushWait(frame);
        }).then(() => { if (!stream.destroyed) stream.resume(); }).catch((error) => {
          incoming!.end(error, true);
          closed.resolve({ error });
          stream.destroy();
        });
      });
      stream.once("end", () => { void processing.finally(() => incoming?.end()); });
      stream.once("close", () => { void processing.finally(() => { incoming?.end(); closed.resolve({ reason: didClose ? "local-close" : "remote-close" }); }); });
      stream.once("error", (error) => { incoming?.end(error, true); closed.resolve({ error }); });
      stream.resume();
      return incoming;
    };

    let lane: TransportLane | undefined;
    return {
      description: { id: this.options.id, traits: ["reliable", "ordered", "byte-stream", ...(this.options.traits ?? [])] },
      closed: closed.promise,
      openLane: async (requirements = RELIABLE_ORDERED_LANE): Promise<TransportLane> => {
        if (requirements.reliability !== "reliable" || requirements.ordering !== "ordered") throw new TypeError("Node duplex base lane is reliable + ordered.");
        if (claimed) throw new Error("Node duplex connection exposes one base lane.");
        claimed = true;
        const queue = startReadLoop(requirements.maxFrameBytes);
        lane = {
          id: `${this.options.id}:0`,
          incoming: queue,
          write: (frame) => new Promise<void>((resolve, reject) => {
            stream.write(encodeLengthPrefixedFrame(frame), (error?: Error | null) => error ? reject(error) : resolve());
          }),
          close: async (reason) => {
            if (didClose) return;
            didClose = true;
            stream.end();
            await Promise.resolve(closeHook?.(reason)).catch(() => {});
          }
        };
        return lane;
      },
      async close(_code?: number, reason?: string): Promise<void> {
        if (lane) await lane.close(reason);
        else {
          if (didClose) return;
          didClose = true;
          stream.destroy();
          await Promise.resolve(closeHook?.(reason)).catch(() => {});
        }
      }
    };
  }
}
