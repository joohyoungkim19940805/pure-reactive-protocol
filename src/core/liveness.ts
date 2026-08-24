export interface LivenessOptions {
  readonly intervalMs?: number;
  readonly timeoutMs?: number;
}

export interface ResolvedLivenessOptions {
  readonly intervalMs: number;
  readonly timeoutMs: number;
}

export const DEFAULT_LIVENESS: Readonly<ResolvedLivenessOptions> = Object.freeze({
  intervalMs: 15_000,
  timeoutMs: 45_000
});

const positiveU31 = (name: string, value: number): number => {
  if (!Number.isSafeInteger(value) || value <= 0 || value > 0x7fffffff) {
    throw new RangeError(`${name} must be an integer between 1 and 2^31-1 milliseconds.`);
  }
  return value;
};

export const resolveLiveness = (options: LivenessOptions = {}): Readonly<ResolvedLivenessOptions> => {
  const intervalMs = positiveU31("liveness.intervalMs", options.intervalMs ?? DEFAULT_LIVENESS.intervalMs);
  const timeoutMs = positiveU31("liveness.timeoutMs", options.timeoutMs ?? DEFAULT_LIVENESS.timeoutMs);
  if (timeoutMs <= intervalMs) throw new RangeError("liveness.timeoutMs must be greater than liveness.intervalMs.");
  return Object.freeze({ intervalMs, timeoutMs });
};

export const LIVENESS_PARAMETERS_BYTES = 8;

export const encodeLivenessParameters = (options: Readonly<ResolvedLivenessOptions>): Uint8Array => {
  const output = new Uint8Array(LIVENESS_PARAMETERS_BYTES);
  const view = new DataView(output.buffer);
  view.setUint32(0, options.intervalMs);
  view.setUint32(4, options.timeoutMs);
  return output;
};

export const decodeLivenessParameters = (value: Uint8Array | undefined): Readonly<ResolvedLivenessOptions> => {
  if (!value || value.byteLength !== LIVENESS_PARAMETERS_BYTES) {
    throw new RangeError(`PRP core liveness capability requires exactly ${LIVENESS_PARAMETERS_BYTES} parameter bytes.`);
  }
  return resolveLiveness({
    intervalMs: new DataView(value.buffer, value.byteOffset, value.byteLength).getUint32(0),
    timeoutMs: new DataView(value.buffer, value.byteOffset, value.byteLength).getUint32(4)
  });
};

export const encodeProbe = (probe: bigint): Uint8Array => {
  if (probe <= 0n || probe > 0xffffffffffffffffn) throw new RangeError("Liveness probe id must fit unsigned 64 bits and be nonzero.");
  const output = new Uint8Array(8);
  new DataView(output.buffer).setBigUint64(0, probe);
  return output;
};

export const decodeProbe = (value: Uint8Array | undefined): bigint => {
  if (!value || value.byteLength !== 8) throw new RangeError("PRP PING/PONG payload must contain exactly one unsigned 64-bit probe id.");
  const probe = new DataView(value.buffer, value.byteOffset, value.byteLength).getBigUint64(0);
  if (probe === 0n) throw new RangeError("PRP PING/PONG probe id must be nonzero.");
  return probe;
};
