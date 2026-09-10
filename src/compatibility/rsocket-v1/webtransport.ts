import { openWebTransportBidirectionalEndpoint, type WebTransportEndpointOptions } from "../../transport/webtransport-endpoint";
import type { ReactiveTransport, TransportConnection } from "../../transport/types";
import { RSocketByteStreamTransport } from "./transport";

export interface RSocketWebTransportTransportOptions extends WebTransportEndpointOptions {}

/** RSocket 1.0 over one reliable WebTransport bidirectional stream with 24-bit framing. */
export class RSocketWebTransportTransport implements ReactiveTransport {
  readonly id = "rsocket-webtransport";
  constructor(private readonly url: string, private readonly options: RSocketWebTransportTransportOptions = {}) {}

  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    return new RSocketByteStreamTransport({
      id: this.id,
      traits: ["webtransport", "http3", "quic", "single-bidirectional-stream"],
      open: (openSignal) => openWebTransportBidirectionalEndpoint(this.url, this.options, openSignal)
    }).connect(signal);
  }
}
