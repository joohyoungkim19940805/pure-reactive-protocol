import { deferred } from "../core/async-queue";
import { BOOTSTRAP_PROTOCOL_LIMITS, DEFAULT_PROTOCOL_LIMITS } from "../core/limits";
import { TransportUnavailableError } from "../core/errors";
import { IncomingFrameQueue } from "./incoming";
import { RELIABLE_ORDERED_LANE, type ReactiveTransport, type TransportCloseEvent, type TransportConnection, type TransportLane } from "./types";

const BUFFERED_HIGH_WATER_BYTES = 4 * 1024 * 1024;
const reasonEncoder = new TextEncoder();

export interface WebSocketTransportOptions {
  readonly protocols?: string | string[];
  readonly webSocketCtor?: typeof WebSocket;
}

const closeReason = (reason?: string): string | undefined => {
  if (!reason) return undefined;
  return reasonEncoder.encode(reason).length <= 123 ? reason : undefined;
};

export class WebSocketTransport implements ReactiveTransport {
  readonly id = "websocket";
  constructor(private readonly url: string, private readonly options: WebSocketTransportOptions = {}) {}

  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    const WebSocketCtor = this.options.webSocketCtor ?? globalThis.WebSocket;
    if (!WebSocketCtor) throw new TransportUnavailableError("WebSocket is not available in this runtime.");
    const socket = this.options.protocols === undefined ? new WebSocketCtor(this.url) : new WebSocketCtor(this.url, this.options.protocols);
    socket.binaryType = "arraybuffer";

    await new Promise<void>((resolve, reject) => {
      const cleanup = () => {
        signal?.removeEventListener("abort", aborted);
        socket.removeEventListener("open", opened);
        socket.removeEventListener("error", failed);
        socket.removeEventListener("close", closedBeforeOpen);
      };
      const aborted = () => {
        cleanup();
        try { socket.close(1000, "aborted"); } catch { /* connecting sockets may reject close */ }
        reject(signal?.reason ?? new DOMException("Aborted", "AbortError"));
      };
      const opened = () => { cleanup(); resolve(); };
      const failed = () => { cleanup(); reject(new Error("WebSocket connection failed.")); };
      const closedBeforeOpen = (event: CloseEvent) => { cleanup(); reject(new Error(`WebSocket closed before opening${event.reason ? `: ${event.reason}` : "."}`)); };
      if (signal?.aborted) return aborted();
      signal?.addEventListener("abort", aborted, { once: true });
      socket.addEventListener("open", opened, { once: true });
      socket.addEventListener("error", failed, { once: true });
      socket.addEventListener("close", closedBeforeOpen, { once: true });
    });

    const closed = deferred<TransportCloseEvent>();
    let laneClaimed = false;
    let didClose = false;
    let failed = false;
    let incoming: IncomingFrameQueue | undefined;
    const preLaneFrames: Uint8Array[] = [];
    let preLaneBytes = 0;
    let messageTail: Promise<void> = Promise.resolve();

    const fail = (error: unknown): void => {
      if (failed) return;
      failed = true;
      incoming?.end(error, true);
      closed.resolve({ error });
      try {
        if (socket.readyState === WebSocketCtor.OPEN || socket.readyState === WebSocketCtor.CONNECTING) socket.close(1003, "binary protocol error");
      } catch { /* best effort */ }
    };

    socket.addEventListener("message", (event) => {
      messageTail = messageTail.then(async () => {
        if (failed) return;
        let frame: Uint8Array;
        if (event.data instanceof ArrayBuffer) frame = new Uint8Array(event.data);
        else if (ArrayBuffer.isView(event.data)) frame = new Uint8Array(event.data.buffer, event.data.byteOffset, event.data.byteLength).slice();
        else if (event.data instanceof Blob) frame = new Uint8Array(await event.data.arrayBuffer());
        else throw new TypeError("PRP requires binary WebSocket messages.");
        if (incoming) {
          incoming.pushOrThrow(frame);
          return;
        }
        if (frame.byteLength > BOOTSTRAP_PROTOCOL_LIMITS.maxFrameBytes ||
            preLaneFrames.length >= 8 ||
            preLaneBytes + frame.byteLength > BOOTSTRAP_PROTOCOL_LIMITS.maxFrameBytes * 2) {
          throw new Error("WebSocket delivered excessive PRP data before the protocol lane was opened.");
        }
        preLaneFrames.push(frame.slice());
        preLaneBytes += frame.byteLength;
      }).catch(fail);
    });
    socket.addEventListener("close", (event) => {
      void messageTail.finally(() => {
        incoming?.end();
        closed.resolve({ code: event.code, reason: event.reason });
      });
    });
    socket.addEventListener("error", () => fail(new Error("WebSocket transport error.")));

    let lane: TransportLane | undefined;
    return {
      description: { id: "websocket", traits: ["reliable", "ordered", "message-boundaries"] },
      closed: closed.promise,
      async openLane(requirements = RELIABLE_ORDERED_LANE): Promise<TransportLane> {
        if (requirements.reliability !== "reliable" || requirements.ordering !== "ordered") throw new TypeError("WebSocket exposes a reliable ordered lane.");
        if (laneClaimed) throw new Error("WebSocket connection exposes one base lane.");
        laneClaimed = true;
        incoming = new IncomingFrameQueue(requirements.maxFrameBytes ?? DEFAULT_PROTOCOL_LIMITS.maxFrameBytes);
        for (const frame of preLaneFrames.splice(0)) incoming.pushOrThrow(frame);
        preLaneBytes = 0;
        lane = {
          id: "websocket:0",
          incoming,
          async write(frame): Promise<void> {
            while (socket.bufferedAmount > BUFFERED_HIGH_WATER_BYTES) {
              if (socket.readyState !== WebSocketCtor.OPEN) throw new Error("WebSocket is not open.");
              await new Promise<void>((resolve) => setTimeout(resolve, 1));
            }
            if (socket.readyState !== WebSocketCtor.OPEN) throw new Error("WebSocket is not open.");
            socket.send(frame);
          },
          close(reason): void {
            if (didClose) return;
            didClose = true;
            try { socket.close(1000, closeReason(reason)); } catch { socket.close(); }
          }
        };
        return lane;
      },
      close(code, reason): void {
        if (didClose) return;
        didClose = true;
        try { code === undefined ? socket.close() : socket.close(code, closeReason(reason)); }
        catch { socket.close(); }
      }
    };
  }
}
