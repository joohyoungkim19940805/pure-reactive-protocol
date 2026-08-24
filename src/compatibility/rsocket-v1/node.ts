import type { Duplex } from "node:stream";
import { createConnection, type Socket } from "node:net";
import { connect as connectTls, type ConnectionOptions as TlsConnectionOptions, type TLSSocket } from "node:tls";
import { deferred } from "../../core/async-queue";
import { IncomingFrameQueue } from "../../transport/incoming";
import { RELIABLE_ORDERED_LANE, type ReactiveTransport, type TransportCloseEvent, type TransportConnection, type TransportLane } from "../../transport/types";
import { RSOCKET_MAX_FRAME_BYTES } from "./frame";
import { encodeRSocketLengthPrefixedFrame, RSocketLengthPrefixedDecoder } from "./framing";

export interface RSocketNodeDuplexTransportOptions {
  readonly id: string;
  readonly traits?: readonly string[];
  readonly open: (signal?: AbortSignal) => Promise<Duplex>;
  readonly close?: (reason?: string) => void | Promise<void>;
}

export class RSocketNodeDuplexTransport implements ReactiveTransport {
  readonly id: string;
  constructor(private readonly options: RSocketNodeDuplexTransportOptions) { this.id = options.id; }
  static from(stream: Duplex, options: Omit<RSocketNodeDuplexTransportOptions, "open">): RSocketNodeDuplexTransport {
    return new RSocketNodeDuplexTransport({ ...options, open: async () => stream });
  }

  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    if (signal?.aborted) throw signal.reason ?? Object.assign(new Error("Aborted"), { name: "AbortError" });
    const stream = await this.options.open(signal);
    const closeHook = this.options.close;
    const closed = deferred<TransportCloseEvent>();
    let queue: IncomingFrameQueue | undefined;
    let claimed = false;
    let didClose = false;
    let processing = Promise.resolve();
    stream.pause();

    const start = (maxFrameBytes: number): IncomingFrameQueue => {
      queue = new IncomingFrameQueue(maxFrameBytes);
      const decoder = new RSocketLengthPrefixedDecoder(maxFrameBytes);
      stream.on("data", (chunk: Buffer | Uint8Array) => {
        stream.pause();
        const copy = new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength).slice();
        processing = processing.then(async () => {
          for (const frame of decoder.push(copy)) {
            if (frame.byteLength > maxFrameBytes) throw new RangeError(`RSocket frame exceeds ${maxFrameBytes} bytes.`);
            await queue!.pushWait(frame);
          }
        }).then(() => { if (!stream.destroyed) stream.resume(); }).catch((error) => {
          queue!.end(error, true); closed.resolve({ error }); stream.destroy();
        });
      });
      stream.once("end", () => { void processing.finally(() => queue?.end()); });
      stream.once("close", () => { void processing.finally(() => { queue?.end(); closed.resolve({ reason: didClose ? "local-close" : "remote-close" }); }); });
      stream.once("error", (error) => { queue?.end(error, true); closed.resolve({ error }); });
      stream.resume();
      return queue;
    };

    let lane: TransportLane | undefined;
    return {
      description: { id: this.id, traits: ["reliable", "ordered", "byte-stream", "rsocket-24bit-framing", ...(this.options.traits ?? [])] },
      closed: closed.promise,
      openLane: async (requirements = RELIABLE_ORDERED_LANE): Promise<TransportLane> => {
        if (requirements.reliability !== "reliable" || requirements.ordering !== "ordered") throw new TypeError("RSocket node duplex requires reliable ordered delivery.");
        if (claimed) throw new Error("RSocket node duplex exposes one base lane.");
        claimed = true;
        const maxFrameBytes = Math.min(requirements.maxFrameBytes ?? RSOCKET_MAX_FRAME_BYTES, RSOCKET_MAX_FRAME_BYTES);
        const incoming = start(maxFrameBytes);
        lane = {
          id: `${this.id}:0`, incoming,
          write: (frame) => new Promise<void>((resolve, reject) => {
            if (frame.byteLength > maxFrameBytes) { reject(new RangeError(`RSocket frame exceeds ${maxFrameBytes} bytes.`)); return; }
            stream.write(encodeRSocketLengthPrefixedFrame(frame), (error?: Error | null) => error ? reject(error) : resolve());
          }),
          close: async (reason) => {
            if (didClose) return; didClose = true; stream.end();
            await Promise.resolve(closeHook?.(reason)).catch(() => {});
          }
        };
        return lane;
      },
      close: async (_code?: number, reason?: string) => {
        if (lane) await lane.close(reason);
        else if (!didClose) { didClose = true; stream.destroy(); await Promise.resolve(closeHook?.(reason)).catch(() => {}); }
      }
    };
  }
}

export interface RSocketTcpTransportOptions {
  readonly host: string;
  readonly port: number;
  readonly tls?: boolean | TlsConnectionOptions;
  readonly noDelay?: boolean;
}

const awaitSocket = (socket: Socket | TLSSocket, event: "connect" | "secureConnect", signal?: AbortSignal): Promise<void> => new Promise((resolve, reject) => {
  const cleanup = () => { socket.off(event, connected); socket.off("error", failed); signal?.removeEventListener("abort", aborted); };
  const connected = () => { cleanup(); resolve(); };
  const failed = (error: Error) => { cleanup(); reject(error); };
  const aborted = () => { cleanup(); socket.destroy(); reject(signal?.reason ?? Object.assign(new Error("RSocket TCP connection aborted."), { name: "AbortError" })); };
  if (signal?.aborted) return aborted();
  socket.once(event, connected); socket.once("error", failed); signal?.addEventListener("abort", aborted, { once: true });
});

export class RSocketTcpTransport implements ReactiveTransport {
  readonly id = "rsocket-tcp";
  constructor(private readonly options: RSocketTcpTransportOptions) {}
  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    return new RSocketNodeDuplexTransport({
      id: this.options.tls ? "rsocket-tcp+tls" : "rsocket-tcp",
      traits: this.options.tls ? ["tls"] : [],
      open: async () => {
        const socket = this.options.tls
          ? connectTls({ host: this.options.host, port: this.options.port, ...(typeof this.options.tls === "object" ? this.options.tls : {}) })
          : createConnection({ host: this.options.host, port: this.options.port });
        socket.setNoDelay(this.options.noDelay ?? true);
        await awaitSocket(socket, this.options.tls ? "secureConnect" : "connect", signal);
        return socket;
      }
    }).connect(signal);
  }
}
