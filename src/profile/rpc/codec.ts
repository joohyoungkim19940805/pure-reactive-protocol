export interface PayloadCodec<T = unknown> {
  /** Stable wire-compatibility identifier negotiated by the RPC profile capability. */
  readonly id: string;
  encode(value: T): Uint8Array;
  decode(value: Uint8Array): T;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

export const jsonCodec: PayloadCodec = Object.freeze({
  id: "application/json",
  encode(value: unknown): Uint8Array {
    if (value === undefined) return new Uint8Array(0);
    let encoded: string | undefined;
    try {
      encoded = JSON.stringify(value);
    } catch (cause) {
      throw new TypeError("RPC JSON payload is not JSON-serializable.", { cause });
    }
    if (encoded === undefined) throw new TypeError("RPC JSON payload is not JSON-serializable.");
    return encoder.encode(encoded);
  },
  decode(value: Uint8Array): unknown {
    if (value.length === 0) return undefined;
    return JSON.parse(decoder.decode(value));
  }
});

export const binaryCodec: PayloadCodec<Uint8Array> = Object.freeze({
  id: "application/octet-stream",
  encode: (value: Uint8Array) => value,
  decode: (value: Uint8Array) => value
});
