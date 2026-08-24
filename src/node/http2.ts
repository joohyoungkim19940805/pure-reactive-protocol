import { connect, constants, type ClientHttp2Session, type ClientHttp2Stream, type ClientSessionOptions, type OutgoingHttpHeaders } from "node:http2";
import { NodeDuplexTransport } from "./duplex";
import type { ReactiveTransport, TransportConnection } from "../transport/types";

export interface Http2TransportOptions {
  readonly authority: string;
  readonly path?: string;
  readonly headers?: OutgoingHttpHeaders;
  readonly connectOptions?: ClientSessionOptions;
}

const awaitSession = (session: ClientHttp2Session, signal?: AbortSignal): Promise<void> => new Promise((resolve, reject) => {
  const cleanup = () => { session.off("connect", connected); session.off("error", failed); signal?.removeEventListener("abort", aborted); };
  const connected = () => { cleanup(); resolve(); };
  const failed = (error: Error) => { cleanup(); reject(error); };
  const aborted = () => { cleanup(); session.destroy(); reject(signal?.reason ?? Object.assign(new Error("HTTP/2 connection aborted."), { name: "AbortError" })); };
  if (signal?.aborted) return aborted();
  session.once("connect", connected);
  session.once("error", failed);
  signal?.addEventListener("abort", aborted, { once: true });
});

export class Http2Transport implements ReactiveTransport {
  readonly id = "http2";
  constructor(private readonly options: Http2TransportOptions) {}
  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    const session = connect(this.options.authority, this.options.connectOptions);
    try { await awaitSession(session, signal); }
    catch (error) { session.destroy(); throw error; }
    let request: ClientHttp2Stream | undefined;
    return new NodeDuplexTransport({
      id: "http2",
      traits: ["native-multiplexing"],
      open: async () => {
        request = session.request({
          ...this.options.headers,
          [constants.HTTP2_HEADER_METHOD]: "POST",
          [constants.HTTP2_HEADER_PATH]: this.options.path ?? "/pure-reactive-protocol",
          "content-type": "application/x-pure-reactive-protocol; version=1"
        }, { endStream: false });
        return request;
      },
      close: () => { request?.end(); session.close(); }
    }).connect(signal);
  }
}
