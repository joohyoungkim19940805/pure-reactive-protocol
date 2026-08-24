import { AsyncQueue, deferred, type Deferred } from "../core/async-queue";
import { RELIABLE_ORDERED_LANE, type LaneRequirements, type ReactiveTransport, type TransportCloseEvent, type TransportConnection, type TransportLane } from "./types";

class MemoryEndpoint {
  readonly incoming = new AsyncQueue<Uint8Array>();
  readonly closed: Deferred<TransportCloseEvent> = deferred();
  peer?: MemoryEndpoint;
  claimed = false;
  isClosed = false;

  write(frame: Uint8Array): void {
    if (this.isClosed || !this.peer || this.peer.isClosed) throw new Error("Memory transport is closed.");
    this.peer.incoming.push(frame.slice());
  }

  close(reason?: string): void {
    if (this.isClosed) return;
    this.isClosed = true;
    this.incoming.end();
    this.closed.resolve(reason === undefined ? {} : { reason });
    if (this.peer && !this.peer.isClosed) this.peer.remoteClose(reason);
  }

  private remoteClose(reason?: string): void {
    if (this.isClosed) return;
    this.isClosed = true;
    this.incoming.end();
    this.closed.resolve(reason === undefined ? {} : { reason });
  }
}

const transportFor = (endpoint: MemoryEndpoint, id: string): ReactiveTransport => ({
  id,
  async connect(): Promise<TransportConnection> {
    if (endpoint.claimed) throw new Error("Memory transport endpoint already connected.");
    endpoint.claimed = true;
    let laneClaimed = false;
    const lane: TransportLane = {
      id: `${id}:0`,
      incoming: endpoint.incoming,
      async write(frame): Promise<void> { endpoint.write(frame); },
      close(reason): void { endpoint.close(reason); }
    };
    return {
      description: { id, traits: ["reliable", "ordered", "message-boundaries", "memory"] },
      closed: endpoint.closed.promise,
      async openLane(requirements: LaneRequirements = RELIABLE_ORDERED_LANE): Promise<TransportLane> {
        if (requirements.reliability !== "reliable" || requirements.ordering !== "ordered") {
          throw new TypeError("Memory transport base lane is reliable + ordered.");
        }
        if (laneClaimed) throw new Error("Memory connection exposes one base lane.");
        laneClaimed = true;
        return lane;
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
