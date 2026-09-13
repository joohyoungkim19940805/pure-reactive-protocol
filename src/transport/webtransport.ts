import { deferred } from "../core/async-queue";
import { DEFAULT_PROTOCOL_LIMITS } from "../core/limits";
import { TransportUnavailableError } from "../core/errors";
import { encodeLengthPrefixedFrame, LengthPrefixedFrameDecoder } from "./framing";
import { IncomingFrameQueue } from "./incoming";
import {
  BEST_EFFORT_UNORDERED_LANE,
  RELIABLE_ORDERED_LANE,
  type LaneRequirements,
  type ReactiveTransport,
  type TransportCloseEvent,
  type TransportConnection,
  type TransportLane
} from "./types";
import type { WebTransportEndpointOptions } from "./webtransport-endpoint";

export interface WebTransportTransportOptions extends WebTransportEndpointOptions {}

const abortError = (signal?: AbortSignal): unknown =>
  signal?.reason ?? Object.assign(new Error("Aborted"), { name: "AbortError" });

const isReliableOrdered = (requirements: LaneRequirements): boolean =>
  requirements.reliability === "reliable" && requirements.ordering === "ordered";

const isBestEffortUnordered = (requirements: LaneRequirements): boolean =>
  requirements.reliability === "best-effort" && requirements.ordering === "unordered";

/**
 * Native PRP over WebTransport.
 *
 * The base PRP/1 lane uses one reliable bidirectional stream with unsigned 32-bit outer framing.
 * When available, a second logical lane maps directly to WebTransport datagrams and preserves
 * native datagram message boundaries without adding reliable-stream semantics.
 */
export class WebTransportTransport implements ReactiveTransport {
  readonly id = "webtransport";
  constructor(private readonly url: string, private readonly options: WebTransportTransportOptions = {}) {}

  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    const WebTransportCtor = this.options.webTransportCtor ?? globalThis.WebTransport;
    if (!WebTransportCtor) throw new TransportUnavailableError("WebTransport is not available in this runtime.");
    if (signal?.aborted) throw abortError(signal);

    const transport = new WebTransportCtor(this.url, this.options.options);
    let rejectAbort!: (reason?: unknown) => void;
    const abortedPromise = new Promise<never>((_resolve, reject) => { rejectAbort = reject; });
    const aborted = () => {
      try { transport.close({ reason: "aborted" }); } catch { /* best effort */ }
      rejectAbort(abortError(signal));
    };
    signal?.addEventListener("abort", aborted, { once: true });

    try {
      await Promise.race([transport.ready, abortedPromise]);
    } catch (error) {
      try { transport.close({ reason: "connection failed" }); } catch { /* best effort */ }
      throw error;
    } finally {
      signal?.removeEventListener("abort", aborted);
    }

    const closed = deferred<TransportCloseEvent>();
    void transport.closed.then(
      (info) => closed.resolve({
        ...(info.closeCode === undefined ? {} : { code: info.closeCode }),
        ...(info.reason === undefined ? {} : { reason: info.reason })
      }),
      (error) => closed.resolve({ error })
    );

    const datagramCarrier = transport.datagrams;
    const carrierDatagramMax = Number(datagramCarrier?.maxDatagramSize ?? 0);
    const hasNativeDatagrams = Number.isFinite(carrierDatagramMax) && carrierDatagramMax > 0;

    let reliableClaimed = false;
    let datagramClaimed = false;
    let didClose = false;
    let reliableLane: TransportLane | undefined;
    let datagramLane: TransportLane | undefined;

    const connection: TransportConnection = {
      description: {
        id: this.id,
        traits: Object.freeze([
          "webtransport",
          "http3",
          "quic",
          "reliable",
          "ordered",
          ...(hasNativeDatagrams ? ["best-effort", "unordered", "datagrams"] : []),
          "single-bidirectional-stream"
        ])
      },
      closed: closed.promise,
      supportsLane(requirements): boolean {
        return isReliableOrdered(requirements) || (hasNativeDatagrams && isBestEffortUnordered(requirements));
      },
      async openLane(requirements = RELIABLE_ORDERED_LANE): Promise<TransportLane> {
        if (isReliableOrdered(requirements)) {
          if (reliableClaimed) throw new Error("WebTransport connection exposes one PRP reliable lane.");
          reliableClaimed = true;
          const stream = await transport.createBidirectionalStream();
          const reader = stream.readable.getReader();
          const writer = stream.writable.getWriter();
          const maxFrameBytes = requirements.maxFrameBytes ?? DEFAULT_PROTOCOL_LIMITS.maxFrameBytes;
          const incoming = new IncomingFrameQueue(maxFrameBytes);
          const decoder = new LengthPrefixedFrameDecoder(maxFrameBytes);
          let laneClosed = false;

          void (async () => {
            try {
              while (!laneClosed) {
                const result = await reader.read();
                if (result.done) break;
                for (const frame of decoder.push(result.value)) await incoming.pushWait(frame);
              }
              incoming.end();
            } catch (error) {
              incoming.end(error, true);
            } finally {
              try { reader.releaseLock(); } catch { /* already released */ }
            }
          })();

          reliableLane = {
            id: "webtransport:reliable",
            maxFrameBytes,
            incoming,
            async write(frame): Promise<void> {
              if (laneClosed) throw new Error("WebTransport reliable lane is closed.");
              await writer.write(encodeLengthPrefixedFrame(frame));
            },
            async close(reason): Promise<void> {
              if (laneClosed) return;
              laneClosed = true;
              incoming.end(undefined, true);
              await Promise.resolve().then(() => reader.cancel(reason)).catch(() => {});
              await writer.close().catch(() => {});
            }
          };
          return reliableLane;
        }

        if (isBestEffortUnordered(requirements)) {
          if (!hasNativeDatagrams) throw new TransportUnavailableError("WebTransport datagrams are unavailable on this connection.");
          if (datagramClaimed) throw new Error("WebTransport connection exposes one native datagram lane.");
          datagramClaimed = true;
          const reader = datagramCarrier.readable.getReader();
          const writer = datagramCarrier.writable.getWriter();
          const carrierMax = carrierDatagramMax;
          const admittedMax = requirements.maxFrameBytes ?? carrierMax;
          const maxFrameBytes = Math.min(carrierMax, admittedMax);
          const incoming = new IncomingFrameQueue(maxFrameBytes, 256, Math.max(maxFrameBytes * 256, 64 * 1024));
          let laneClosed = false;

          void (async () => {
            try {
              while (!laneClosed) {
                const result = await reader.read();
                if (result.done) break;
                const frame = result.value instanceof Uint8Array ? result.value.slice() : new Uint8Array(result.value).slice();
                // Best-effort means congestion/loss is allowed, not unbounded buffering. Local
                // pressure therefore drops a datagram instead of stalling or poisoning the session.
                if (frame.byteLength > maxFrameBytes) continue;
                incoming.tryPush(frame);
              }
              incoming.end();
            } catch (error) {
              incoming.end(error, true);
            } finally {
              try { reader.releaseLock(); } catch { /* already released */ }
            }
          })();

          datagramLane = {
            id: "webtransport:datagram",
            maxFrameBytes,
            incoming,
            async write(frame): Promise<void> {
              if (laneClosed) throw new Error("WebTransport datagram lane is closed.");
              if (frame.byteLength > maxFrameBytes) throw new RangeError(`WebTransport datagram exceeds ${maxFrameBytes} bytes.`);
              await writer.write(frame);
            },
            async close(reason): Promise<void> {
              if (laneClosed) return;
              laneClosed = true;
              incoming.end(undefined, true);
              await Promise.resolve().then(() => reader.cancel(reason)).catch(() => {});
              await writer.close().catch(() => {});
            }
          };
          return datagramLane;
        }

        throw new TypeError("WebTransport supports PRP reliable/ordered and best-effort/unordered lanes only.");
      },
      async close(code, reason): Promise<void> {
        if (didClose) return;
        didClose = true;
        await Promise.allSettled([
          Promise.resolve(reliableLane?.close(reason)),
          Promise.resolve(datagramLane?.close(reason))
        ]);
        try {
          transport.close({ ...(code === undefined ? {} : { closeCode: code }), ...(reason === undefined ? {} : { reason }) });
        } catch {
          transport.close();
        }
      }
    };

    return connection;
  }
}
