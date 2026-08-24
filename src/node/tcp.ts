import { createConnection, type Socket } from "node:net";
import { connect as connectTls, type ConnectionOptions as TlsConnectionOptions, type TLSSocket } from "node:tls";
import { NodeDuplexTransport } from "./duplex";
import type { ReactiveTransport, TransportConnection } from "../transport/types";

export interface TcpTransportOptions {
  readonly host: string;
  readonly port: number;
  readonly tls?: boolean | TlsConnectionOptions;
  readonly noDelay?: boolean;
}

const awaitSocket = (socket: Socket | TLSSocket, event: "connect" | "secureConnect", signal?: AbortSignal): Promise<void> => new Promise((resolve, reject) => {
  const cleanup = () => { socket.off(event, connected); socket.off("error", failed); signal?.removeEventListener("abort", aborted); };
  const connected = () => { cleanup(); resolve(); };
  const failed = (error: Error) => { cleanup(); reject(error); };
  const aborted = () => { cleanup(); socket.destroy(); reject(signal?.reason ?? Object.assign(new Error("TCP connection aborted."), { name: "AbortError" })); };
  if (signal?.aborted) return aborted();
  socket.once(event, connected);
  socket.once("error", failed);
  signal?.addEventListener("abort", aborted, { once: true });
});

export class TcpTransport implements ReactiveTransport {
  readonly id = "tcp";
  constructor(private readonly options: TcpTransportOptions) {}
  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    return new NodeDuplexTransport({
      id: this.options.tls ? "tcp+tls" : "tcp",
      traits: this.options.tls ? ["tls"] : [],
      open: async () => {
        const socket = this.options.tls
          ? connectTls({ host: this.options.host, port: this.options.port, ...(typeof this.options.tls === "object" ? this.options.tls : {}) })
          : createConnection({ host: this.options.host, port: this.options.port });
        socket.setNoDelay(this.options.noDelay ?? true);
        await awaitSocket(socket, this.options.tls ? "secureConnect" : "connect", signal);
        return socket;
      }
    }).connect(signal);
  }
}
