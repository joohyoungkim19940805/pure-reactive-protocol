import { deferred } from "../../core/async-queue";
import { IncomingFrameQueue } from "../../transport/incoming";
import { RELIABLE_ORDERED_LANE, type ReactiveTransport, type TransportCloseEvent, type TransportConnection, type TransportLane } from "../../transport/types";
import { RSOCKET_MAX_FRAME_BYTES } from "./frame";
import { encodeRSocketLengthPrefixedFrame, RSocketLengthPrefixedDecoder } from "./framing";

export interface RSocketByteStreamEndpoint {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  close?(reason?: string): void | Promise<void>;
}

export interface RSocketByteStreamTransportOptions {
  readonly id: string;
  readonly traits?: readonly string[];
  readonly open: (signal?: AbortSignal) => Promise<RSocketByteStreamEndpoint>;
}

export class RSocketByteStreamTransport implements ReactiveTransport {
  readonly id: string;
  constructor(private readonly options: RSocketByteStreamTransportOptions) { this.id = options.id; }

  static from(endpoint: RSocketByteStreamEndpoint, options: Omit<RSocketByteStreamTransportOptions, "open">): RSocketByteStreamTransport {
    return new RSocketByteStreamTransport({ ...options, open: async () => endpoint });
  }

  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    if (signal?.aborted) throw signal.reason ?? Object.assign(new Error("Aborted"), { name: "AbortError" });
    const endpoint = await this.options.open(signal);
    const reader = endpoint.readable.getReader();
    const writer = endpoint.writable.getWriter();
    const closed = deferred<TransportCloseEvent>();
    let claimed = false;
    let didClose = false;
    let queue: IncomingFrameQueue | undefined;

    const finish = async (event: TransportCloseEvent): Promise<void> => {
      if (didClose) return;
      didClose = true;
      queue?.end(event.error, event.error !== undefined);
      closed.resolve(event);
      await Promise.allSettled([reader.cancel(), writer.close(), Promise.resolve(endpoint.close?.(event.reason))]);
    };

    return {
      description: { id: this.id, traits: ["reliable", "ordered", "byte-stream", "rsocket-24bit-framing", ...(this.options.traits ?? [])] },
      closed: closed.promise,
      openLane: async (requirements = RELIABLE_ORDERED_LANE): Promise<TransportLane> => {
        if (requirements.reliability !== "reliable" || requirements.ordering !== "ordered") throw new TypeError("RSocket byte stream requires reliable ordered delivery.");
        if (claimed) throw new Error("RSocket byte-stream connection exposes one base lane.");
        claimed = true;
        const maxFrameBytes = Math.min(requirements.maxFrameBytes ?? RSOCKET_MAX_FRAME_BYTES, RSOCKET_MAX_FRAME_BYTES);
        queue = new IncomingFrameQueue(maxFrameBytes);
        const decoder = new RSocketLengthPrefixedDecoder(maxFrameBytes);
        void (async () => {
          try {
            while (!didClose) {
              const next = await reader.read();
              if (next.done) break;
              for (const frame of decoder.push(next.value)) {
                if (frame.byteLength > maxFrameBytes) throw new RangeError(`RSocket frame exceeds ${maxFrameBytes} bytes.`);
                await queue!.pushWait(frame);
              }
            }
            if (!didClose) { queue!.end(); closed.resolve({ reason: "remote-close" }); didClose = true; }
          } catch (error) { await finish({ error }); }
        })();
        return {
          id: `${this.id}:0`,
          incoming: queue,
          write: async (frame) => {
            if (didClose) throw new Error("RSocket byte-stream transport is closed.");
            if (frame.byteLength > maxFrameBytes) throw new RangeError(`RSocket frame exceeds ${maxFrameBytes} bytes.`);
            await writer.write(encodeRSocketLengthPrefixedFrame(frame));
          },
          close: async (reason) => { await finish(reason === undefined ? {} : { reason }); }
        };
      },
      close: async (_code?: number, reason?: string) => { await finish(reason === undefined ? {} : { reason }); }
    };
  }
}
