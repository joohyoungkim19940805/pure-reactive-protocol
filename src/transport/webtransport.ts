import { TransportUnavailableError } from "../core/errors";
import { ByteStreamTransport, type ByteStreamEndpoint } from "./byte-stream";
import type { ReactiveTransport, TransportConnection } from "./types";

export interface WebTransportTransportOptions {
  readonly options?: WebTransportOptions;
  readonly webTransportCtor?: typeof WebTransport;
}

export class WebTransportTransport implements ReactiveTransport {
  readonly id = "webtransport";
  constructor(private readonly url: string, private readonly options: WebTransportTransportOptions = {}) {}

  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    const Ctor = this.options.webTransportCtor ?? globalThis.WebTransport;
    if (!Ctor) throw new TransportUnavailableError("WebTransport is not available in this runtime.");
    if (signal?.aborted) throw signal.reason ?? Object.assign(new Error("Aborted"), { name: "AbortError" });
    const transport = new Ctor(this.url, this.options.options);
    void transport.closed.catch(() => {});
    let rejectAbort!: (reason?: unknown) => void;
    const abortedPromise = new Promise<never>((_resolve, reject) => { rejectAbort = reject; });
    const aborted = () => {
      try { transport.close({ reason: "aborted" }); } catch { /* best effort */ }
      rejectAbort(signal?.reason ?? Object.assign(new Error("Aborted"), { name: "AbortError" }));
    };
    signal?.addEventListener("abort", aborted, { once: true });
    try {
      if (signal?.aborted) aborted();
      await Promise.race([transport.ready, abortedPromise]);
      const stream = await Promise.race([transport.createBidirectionalStream(), abortedPromise]);
      const endpoint: ByteStreamEndpoint = {
        readable: stream.readable,
        writable: stream.writable,
        close: (reason) => transport.close(reason === undefined ? undefined : { reason })
      };
      return await ByteStreamTransport.from(endpoint, { id: "webtransport", traits: ["native-multiplexing"] }).connect(signal);
    } catch (error) {
      try { transport.close({ reason: "connection failed" }); } catch { /* best effort */ }
      throw error;
    } finally {
      signal?.removeEventListener("abort", aborted);
    }
  }
}
