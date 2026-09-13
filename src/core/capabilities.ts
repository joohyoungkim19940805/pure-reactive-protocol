import { attribute, bytes, type ProtocolAttribute } from "./attributes";
import { CapabilityMismatchError, ProtocolViolationError } from "./errors";
import { DEFAULT_PROTOCOL_LIMITS, encodePeerProtocolLimits } from "./limits";
import { DEFAULT_LIVENESS, encodeLivenessParameters } from "./liveness";
import type { ReactiveSession } from "./session";

const CAPABILITY_PREFIX = "prp.capability/";
export const CORE_LIMITS_CAPABILITY_ID = "prp.core.limits";
export const CORE_LIVENESS_CAPABILITY_ID = "prp.core.liveness";

export interface CapabilityDescriptor {
  readonly id: string;
  readonly minVersion: number;
  readonly maxVersion: number;
  readonly parameters?: Uint8Array;
}

export interface NegotiatedCapability {
  readonly id: string;
  readonly version: number;
  readonly local: CapabilityDescriptor;
  readonly remote: CapabilityDescriptor;
}

export interface CapabilityPolicy {
  readonly require?: readonly string[];
}

export interface ProtocolExtension {
  readonly capability: CapabilityDescriptor;
  /** Capabilities that must also be offered/negotiated for this extension to be meaningful. */
  readonly requires?: readonly string[];
  attach(session: ReactiveSession): void | (() => void | Promise<void>) | Promise<void | (() => void | Promise<void>)>;
}

export class CapabilitySet implements Iterable<NegotiatedCapability> {
  private readonly valuesById: ReadonlyMap<string, NegotiatedCapability>;

  constructor(values: Iterable<NegotiatedCapability>) {
    this.valuesById = new Map([...values].map((value) => [value.id, value]));
  }

  has(id: string): boolean { return this.valuesById.has(id); }
  get(id: string): NegotiatedCapability | undefined { return this.valuesById.get(id); }
  [Symbol.iterator](): Iterator<NegotiatedCapability> { return this.valuesById.values(); }
  toArray(): readonly NegotiatedCapability[] { return [...this.valuesById.values()]; }
}

export const BASE_CAPABILITIES: readonly CapabilityDescriptor[] = Object.freeze([
  Object.freeze({ id: "prp.core.duplex-stream", minVersion: 1, maxVersion: 1 }),
  Object.freeze({ id: "prp.core.tlv-attributes", minVersion: 1, maxVersion: 1 }),
  Object.freeze({ id: "prp.core.sequence-u64", minVersion: 1, maxVersion: 1 }),
  Object.freeze({ id: "prp.core.fragmentation", minVersion: 1, maxVersion: 1 }),
  Object.freeze({ id: CORE_LIVENESS_CAPABILITY_ID, minVersion: 1, maxVersion: 1, parameters: encodeLivenessParameters(DEFAULT_LIVENESS) }),
  Object.freeze({ id: CORE_LIMITS_CAPABILITY_ID, minVersion: 1, maxVersion: 1, parameters: encodePeerProtocolLimits(DEFAULT_PROTOCOL_LIMITS) })
]);

export const validateCapabilityDescriptor = (capability: CapabilityDescriptor): void => {
  if (!capability.id) throw new TypeError("Capability id must not be empty.");
  bytes(capability.id);
  if (!Number.isInteger(capability.minVersion) || capability.minVersion < 0 || capability.minVersion > 0xffff) throw new RangeError(`Invalid minimum capability version for ${capability.id}.`);
  if (!Number.isInteger(capability.maxVersion) || capability.maxVersion < capability.minVersion || capability.maxVersion > 0xffff) throw new RangeError(`Invalid maximum capability version for ${capability.id}.`);
};

const encodeRange = (capability: CapabilityDescriptor): Uint8Array => {
  validateCapabilityDescriptor(capability);
  const parameters = capability.parameters ?? new Uint8Array(0);
  const output = new Uint8Array(4 + parameters.length);
  const view = new DataView(output.buffer);
  view.setUint16(0, capability.minVersion);
  view.setUint16(2, capability.maxVersion);
  output.set(parameters, 4);
  return output;
};

const decodeRange = (id: string, value: Uint8Array): CapabilityDescriptor => {
  if (!id) throw new ProtocolViolationError("Capability id must not be empty.");
  if (value.length < 4) throw new ProtocolViolationError(`Capability ${id} has an invalid descriptor.`);
  const view = new DataView(value.buffer, value.byteOffset, value.byteLength);
  const minVersion = view.getUint16(0);
  const maxVersion = view.getUint16(2);
  if (maxVersion < minVersion) throw new ProtocolViolationError(`Capability ${id} has an inverted version range.`);
  return { id, minVersion, maxVersion, ...(value.length === 4 ? {} : { parameters: value.slice(4) }) };
};

export const capabilitiesToAttributes = (
  capabilities: readonly CapabilityDescriptor[],
  policy: CapabilityPolicy = {}
): readonly ProtocolAttribute[] => {
  const required = new Set(policy.require ?? []);
  return capabilities.map((capability) => attribute(
    `${CAPABILITY_PREFIX}${capability.id}`,
    encodeRange(capability),
    required.has(capability.id) ? { required: true } : {}
  ));
};

export interface DecodedCapabilityOffer {
  readonly descriptor: CapabilityDescriptor;
  readonly required: boolean;
}

export const capabilitiesFromAttributes = (
  attributes: readonly ProtocolAttribute[]
): readonly DecodedCapabilityOffer[] => {
  const offers: DecodedCapabilityOffer[] = [];
  const ids = new Set<string>();
  for (const item of attributes) {
    if (!item.id.startsWith(CAPABILITY_PREFIX)) continue;
    const id = item.id.slice(CAPABILITY_PREFIX.length);
    if (!id) throw new ProtocolViolationError("Capability id must not be empty.");
    if (ids.has(id)) throw new ProtocolViolationError(`Capability ${id} was offered more than once.`);
    ids.add(id);
    offers.push({ descriptor: decodeRange(id, item.value), required: item.required === true });
  }
  return offers;
};

export const negotiateCapabilities = (
  local: readonly CapabilityDescriptor[],
  remote: readonly DecodedCapabilityOffer[],
  localPolicy: CapabilityPolicy = {}
): CapabilitySet => {
  const localMap = new Map<string, CapabilityDescriptor>();
  for (const capability of local) {
    validateCapabilityDescriptor(capability);
    if (localMap.has(capability.id)) throw new TypeError(`Duplicate local capability: ${capability.id}`);
    localMap.set(capability.id, capability);
  }
  const remoteMap = new Map(remote.map((value) => [value.descriptor.id, value]));
  for (const offered of remote) {
    if (offered.required && !localMap.has(offered.descriptor.id)) throw new CapabilityMismatchError(`Required remote capability is unsupported: ${offered.descriptor.id}`);
  }
  for (const id of localPolicy.require ?? []) {
    if (!remoteMap.has(id)) throw new CapabilityMismatchError(`Required local capability is unavailable remotely: ${id}`);
  }
  const negotiated: NegotiatedCapability[] = [];
  for (const [id, own] of localMap) {
    const remoteOffer = remoteMap.get(id);
    const theirs = remoteOffer?.descriptor;
    if (!theirs) continue;
    const minimum = Math.max(own.minVersion, theirs.minVersion);
    const maximum = Math.min(own.maxVersion, theirs.maxVersion);
    if (minimum <= maximum) negotiated.push({ id, version: maximum, local: own, remote: theirs });
    else if (remoteOffer?.required || localPolicy.require?.includes(id)) throw new CapabilityMismatchError(`No compatible version exists for required capability: ${id}`);
  }
  return new CapabilitySet(negotiated);
};
