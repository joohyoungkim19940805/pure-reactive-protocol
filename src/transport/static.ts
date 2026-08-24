import type { ReactiveTransport, TransportConnection } from "./types";

export class StaticTransport implements ReactiveTransport {
  readonly id: string;
  private claimed = false;
  constructor(private readonly connection: TransportConnection) { this.id = `static:${connection.description.id}`; }
  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    if (signal?.aborted) throw signal.reason ?? Object.assign(new Error("Aborted"), { name: "AbortError" });
    if (this.claimed) throw new Error("StaticTransport connection has already been claimed.");
    this.claimed = true;
    return this.connection;
  }
}
