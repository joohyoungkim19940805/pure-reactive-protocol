import { AsyncQueue, deferred, type Deferred } from "../core/async-queue";
import {
  BEST_EFFORT_UNORDERED_LANE,
  RELIABLE_ORDERED_LANE,
  type LaneRequirements,
  type ReactiveTransport,
  type TransportCloseEvent,
  type TransportConnection,
  type TransportLane
} from "./types";

class MemoryEndpoint {
  readonly reliableIncoming = new AsyncQueue<Uint8Array>();
  readonly datagramIncoming = new AsyncQueue<Uint8Array>();
  readonly closed: Deferred<TransportCloseEvent> = deferred();
  peer?: MemoryEndpoint;
  claimed = false;
  reliableClaimed = false;
  datagramClaimed = false;
  isClosed = false;

  writeReliable(frame: Uint8Array): void {
    if (this.isClosed || !this.peer || this.peer.isClosed) throw new Error("Memory transport is closed.");
    this.peer.reliableIncoming.push(frame.slice());
  }

  writeDatagram(frame: Uint8Array): void {
    if (this.isClosed || !this.peer || this.peer.isClosed) throw new Error("Memory transport is closed.");
    this.peer.datagramIncoming.push(frame.slice());
  }

  close(reason?: string): void {
    if (this.isClosed) return;
    this.isClosed = true;
    this.reliableIncoming.end();
    this.datagramIncoming.end();
    this.closed.resolve(reason === undefined ? {} : { reason });
    if (this.peer && !this.peer.isClosed) this.peer.remoteClose(reason);
  }

  private remoteClose(reason?: string): void {
    if (this.isClosed) return;
    this.isClosed = true;
    this.reliableIncoming.end();
    this.datagramIncoming.end();
    this.closed.resolve(reason === undefined ? {} : { reason });
  }
}

const isReliable = (requirements: LaneRequirements): boolean =>
  requirements.reliability === "reliable" && requirements.ordering === "ordered";
const isDatagram = (requirements: LaneRequirements): boolean =>
  requirements.reliability === "best-effort" && requirements.ordering === "unordered";

const transportFor = (endpoint: MemoryEndpoint, id: string): ReactiveTransport => ({
  id,
  async connect(): Promise<TransportConnection> {
    if (endpoint.claimed) throw new Error("Memory transport endpoint already connected.");
    endpoint.claimed = true;
    return {
      description: { id, traits: ["reliable", "ordered", "best-effort", "unordered", "message-boundaries", "memory"] },
      closed: endpoint.closed.promise,
      supportsLane(requirements): boolean { return isReliable(requirements) || isDatagram(requirements); },
      async openLane(requirements: LaneRequirements = RELIABLE_ORDERED_LANE): Promise<TransportLane> {
        if (isReliable(requirements)) {
          if (endpoint.reliableClaimed) throw new Error("Memory connection exposes one reliable lane.");
          endpoint.reliableClaimed = true;
          return {
            id: `${id}:reliable`,
            ...(requirements.maxFrameBytes === undefined ? {} : { maxFrameBytes: requirements.maxFrameBytes }),
            incoming: endpoint.reliableIncoming,
            async write(frame): Promise<void> { endpoint.writeReliable(frame); },
            close(reason): void { endpoint.close(reason); }
          };
        }
        if (isDatagram(requirements)) {
          if (endpoint.datagramClaimed) throw new Error("Memory connection exposes one datagram lane.");
          endpoint.datagramClaimed = true;
          return {
            id: `${id}:datagram`,
            ...(requirements.maxFrameBytes === undefined ? {} : { maxFrameBytes: requirements.maxFrameBytes }),
            incoming: endpoint.datagramIncoming,
            async write(frame): Promise<void> { endpoint.writeDatagram(frame); },
            close() { /* logical lane close; connection owns endpoint lifetime */ }
          };
        }
        throw new TypeError("Memory transport supports reliable/ordered and best-effort/unordered lanes.");
      },
      close(_code?: number, reason?: string): void { endpoint.close(reason); }
    };
  }
});

export const createMemoryTransportPair = (): readonly [ReactiveTransport, ReactiveTransport] => {
  const left = new MemoryEndpoint();
  const right = new MemoryEndpoint();
  left.peer = right;
  right.peer = left;
  return [transportFor(left, "memory:left"), transportFor(right, "memory:right")];
};

export { BEST_EFFORT_UNORDERED_LANE };
