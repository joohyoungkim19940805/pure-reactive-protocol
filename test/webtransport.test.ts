import { describe, expect, it } from "vitest";
import { RSocketWebTransportTransport } from "../src/compatibility/rsocket-v1/webtransport";
import { RSocketLengthPrefixedDecoder } from "../src/compatibility/rsocket-v1/framing";
import { RELIABLE_ORDERED_LANE } from "../src/transport/types";
import { LengthPrefixedFrameDecoder } from "../src/transport/framing";
import { WebTransportTransport } from "../src/transport/webtransport";

class MockWebTransport {
  static last: MockWebTransport | undefined;
  readonly ready = Promise.resolve();
  readonly closed = new Promise<WebTransportCloseInfo>(() => {});
  readonly writes: Uint8Array[] = [];
  closeInfo: WebTransportCloseInfo | undefined;
  private controller!: ReadableStreamDefaultController<Uint8Array>;
  private readonly readable = new ReadableStream<Uint8Array>({ start: (controller) => { this.controller = controller; } });
  private readonly writable = new WritableStream<Uint8Array>({ write: (chunk) => { this.writes.push(chunk.slice()); } });

  constructor(_url: string | URL, _options?: WebTransportOptions) { MockWebTransport.last = this; }

  async createBidirectionalStream(): Promise<WebTransportBidirectionalStream> {
    return { readable: this.readable, writable: this.writable };
  }

  push(chunk: Uint8Array): void { this.controller.enqueue(chunk); }
  close(closeInfo?: WebTransportCloseInfo): void { this.closeInfo = closeInfo; }
}

const mockCtor = MockWebTransport as unknown as typeof WebTransport;

describe("WebTransport carriers", () => {
  it("uses PRP unsigned 32-bit framing without claiming native multiplexing", async () => {
    const connection = await new WebTransportTransport("https://localhost/prp", { webTransportCtor: mockCtor }).connect();
    const lane = await connection.openLane({ ...RELIABLE_ORDERED_LANE, maxFrameBytes: 1024 });
    const mock = MockWebTransport.last!;
    const frame = Uint8Array.of(1, 2, 3, 4);

    await lane.write(frame);
    expect(new LengthPrefixedFrameDecoder(1024).push(mock.writes[0]!)).toEqual([frame]);
    expect(connection.description.traits).toContain("single-bidirectional-stream");
    expect(connection.description.traits).not.toContain("native-multiplexing");

    const next = lane.incoming[Symbol.asyncIterator]().next();
    mock.push(Uint8Array.of(0, 0));
    mock.push(Uint8Array.of(0, 4, 1, 2, 3, 4));
    await expect(next).resolves.toEqual({ done: false, value: frame });
    await connection.close(undefined, "done");
    expect(mock.closeInfo).toEqual({ reason: "done" });
  });

  it("uses RSocket 24-bit framing over the same WebTransport endpoint", async () => {
    const connection = await new RSocketWebTransportTransport("https://localhost/rsocket", { webTransportCtor: mockCtor }).connect();
    const lane = await connection.openLane({ ...RELIABLE_ORDERED_LANE, maxFrameBytes: 1024 });
    const mock = MockWebTransport.last!;
    const frame = Uint8Array.of(9, 8, 7);

    await lane.write(frame);
    expect(new RSocketLengthPrefixedDecoder(1024).push(mock.writes[0]!)).toEqual([frame]);

    const next = lane.incoming[Symbol.asyncIterator]().next();
    mock.push(Uint8Array.of(0));
    mock.push(Uint8Array.of(0, 3, 9, 8, 7));
    await expect(next).resolves.toEqual({ done: false, value: frame });
    await connection.close(undefined, "done");
  });

  it("closes an in-progress WebTransport handshake when aborted", async () => {
    class PendingWebTransport extends MockWebTransport {
      override readonly ready = new Promise<void>(() => {});
    }
    const controller = new AbortController();
    const reason = new Error("stop");
    const connected = new WebTransportTransport("https://localhost/prp", {
      webTransportCtor: PendingWebTransport as unknown as typeof WebTransport
    }).connect(controller.signal);

    controller.abort(reason);
    await expect(connected).rejects.toBe(reason);
    expect(MockWebTransport.last?.closeInfo).toEqual({ reason: "connection failed" });
  });
});
