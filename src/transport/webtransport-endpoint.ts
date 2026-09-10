import { TransportUnavailableError } from "../core/errors";
import type { ByteStreamEndpoint } from "./byte-stream";

export interface WebTransportEndpointOptions {
  readonly options?: WebTransportOptions;
  readonly webTransportCtor?: typeof WebTransport;
}

const abortError = (signal?: AbortSignal): unknown =>
  signal?.reason ?? Object.assign(new Error("Aborted"), { name: "AbortError" });

/** Opens one reliable bidirectional WebTransport stream as an ordered byte-stream endpoint. */
export const openWebTransportBidirectionalEndpoint = async (
  url: string,
  options: WebTransportEndpointOptions = {},
  signal?: AbortSignal
): Promise<ByteStreamEndpoint> => {
  const WebTransportCtor = options.webTransportCtor ?? globalThis.WebTransport;
  if (!WebTransportCtor) throw new TransportUnavailableError("WebTransport is not available in this runtime.");
  if (signal?.aborted) throw abortError(signal);

  const transport = new WebTransportCtor(url, options.options);
  void transport.closed.catch(() => {});
  let rejectAbort!: (reason?: unknown) => void;
  const abortedPromise = new Promise<never>((_resolve, reject) => { rejectAbort = reject; });
  const aborted = () => {
    try { transport.close({ reason: "aborted" }); } catch { /* best effort */ }
    rejectAbort(abortError(signal));
  };
  signal?.addEventListener("abort", aborted, { once: true });

  try {
    if (signal?.aborted) aborted();
    await Promise.race([transport.ready, abortedPromise]);
    const stream = await Promise.race([transport.createBidirectionalStream(), abortedPromise]);
    return {
      readable: stream.readable,
      writable: stream.writable,
      close: (reason) => transport.close(reason === undefined ? undefined : { reason })
    };
  } catch (error) {
    try { transport.close({ reason: "connection failed" }); } catch { /* best effort */ }
    throw error;
  } finally {
    signal?.removeEventListener("abort", aborted);
  }
};
