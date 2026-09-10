import { ByteStreamTransport } from "./byte-stream";
import { openWebTransportBidirectionalEndpoint, type WebTransportEndpointOptions } from "./webtransport-endpoint";
import type { ReactiveTransport, TransportConnection } from "./types";

export interface WebTransportTransportOptions extends WebTransportEndpointOptions {}

/** Native PRP/1 over one reliable WebTransport bidirectional stream. */
export class WebTransportTransport implements ReactiveTransport {
  readonly id = "webtransport";
  constructor(private readonly url: string, private readonly options: WebTransportTransportOptions = {}) {}

  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    return new ByteStreamTransport({
      id: this.id,
      traits: ["webtransport", "http3", "quic", "single-bidirectional-stream"],
      open: (openSignal) => openWebTransportBidirectionalEndpoint(this.url, this.options, openSignal)
    }).connect(signal);
  }
}
