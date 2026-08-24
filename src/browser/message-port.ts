import { deferred } from "../core/async-queue";
import { DEFAULT_PROTOCOL_LIMITS } from "../core/limits";
import { IncomingFrameQueue } from "../transport/incoming";
import { RELIABLE_ORDERED_LANE, type ReactiveTransport, type TransportCloseEvent, type TransportConnection, type TransportLane } from "../transport/types";

export interface MessagePortLike extends EventTarget {
  postMessage(message: unknown, transfer?: Transferable[]): void;
  start?: () => void;
  close?: () => void;
}

export class MessagePortTransport implements ReactiveTransport {
  readonly id: string;
  constructor(private readonly port: MessagePortLike, id = "message-port") { this.id = id; }

  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    if (signal?.aborted) throw signal.reason ?? Object.assign(new Error("Aborted"), { name: "AbortError" });
    const closed = deferred<TransportCloseEvent>();
    let claimed = false;
    let didClose = false;
    let incoming: IncomingFrameQueue | undefined;

    const finish = (event: TransportCloseEvent): void => {
      if (didClose) return;
      didClose = true;
      this.port.removeEventListener("message", onMessage);
      incoming?.end(event.error, true);
      closed.resolve(event);
      try { this.port.close?.(); } catch { /* best effort */ }
    };
    const onMessage = (event: Event) => {
      const message = (event as MessageEvent).data as { kind?: string; data?: ArrayBuffer; reason?: string };
      try {
        if (message?.kind === "frame" && message.data instanceof ArrayBuffer) {
          if (!incoming) throw new Error("MessagePort frame arrived before the protocol lane was opened.");
          incoming.pushOrThrow(new Uint8Array(message.data));
          return;
        }
        if (message?.kind === "close") finish(message.reason === undefined ? {} : { reason: message.reason });
      } catch (error) {
        finish({ error });
      }
    };
    this.port.addEventListener("message", onMessage);

    let lane: TransportLane | undefined;
    return {
      description: { id: this.id, traits: ["reliable", "ordered", "message-boundaries", "local-process"] },
      closed: closed.promise,
      openLane: async (requirements = RELIABLE_ORDERED_LANE): Promise<TransportLane> => {
        if (requirements.reliability !== "reliable" || requirements.ordering !== "ordered") throw new TypeError("MessagePort exposes a reliable ordered lane.");
        if (claimed) throw new Error("MessagePort connection exposes one base lane.");
        claimed = true;
        incoming = new IncomingFrameQueue(requirements.maxFrameBytes ?? DEFAULT_PROTOCOL_LIMITS.maxFrameBytes);
        this.port.start?.();
        lane = {
          id: `${this.id}:0`,
          incoming,
          write: async (frame) => {
            if (didClose) throw new Error("MessagePort transport is closed.");
            const copy = frame.slice();
            try { this.port.postMessage({ kind: "frame", data: copy.buffer }, [copy.buffer]); }
            catch (error) { finish({ error }); throw error; }
          },
          close: (reason) => {
            if (didClose) return;
            try { this.port.postMessage({ kind: "close", ...(reason === undefined ? {} : { reason }) }); }
            catch { /* remote may already be gone */ }
            finish(reason === undefined ? {} : { reason });
          }
        };
        return lane;
      },
      close: (_code?: number, reason?: string): void => { if (lane) lane.close(reason); else finish(reason === undefined ? {} : { reason }); }
    };
  }
}
