import {
  BASE_CAPABILITIES,
  CORE_LIMITS_CAPABILITY_ID,
  CORE_LIVENESS_CAPABILITY_ID,
  validateCapabilityDescriptor,
  type CapabilityDescriptor,
  type CapabilityPolicy,
  type ProtocolExtension
} from "./capabilities";
import { encodePeerProtocolLimits, resolveProtocolLimits, type ProtocolLimits } from "./limits";
import { encodeLivenessParameters, resolveLiveness, type LivenessOptions, type ResolvedLivenessOptions } from "./liveness";
import { CORE_DATAGRAM_CAPABILITY_ID, encodeDatagramCapability, resolveDatagramOptions, type DatagramOptions, type ResolvedDatagramOptions } from "./datagram";

export interface RuntimeOptions {
  readonly extensions?: readonly ProtocolExtension[];
  readonly policy?: CapabilityPolicy;
  readonly limits?: Partial<ProtocolLimits>;
  readonly handshakeTimeoutMs?: number;
  readonly liveness?: LivenessOptions;
  /** Native PRP best-effort datagrams. Enabled by default when the carrier exposes a matching lane. */
  readonly datagrams?: DatagramOptions | false;
}

export const DEFAULT_HANDSHAKE_TIMEOUT_MS = 30_000;

export class ProtocolRuntime {
  readonly capabilities: readonly CapabilityDescriptor[];
  readonly policy: CapabilityPolicy;
  readonly limits: Readonly<ProtocolLimits>;
  readonly extensions: readonly ProtocolExtension[];
  readonly handshakeTimeoutMs: number;
  readonly liveness: Readonly<ResolvedLivenessOptions>;
  readonly datagrams: Readonly<ResolvedDatagramOptions> | undefined;

  constructor(options: RuntimeOptions = {}) {
    this.limits = resolveProtocolLimits(options.limits);
    this.liveness = resolveLiveness(options.liveness);
    this.datagrams = options.datagrams === false ? undefined : resolveDatagramOptions(options.datagrams);
    this.handshakeTimeoutMs = options.handshakeTimeoutMs ?? DEFAULT_HANDSHAKE_TIMEOUT_MS;
    if (!Number.isSafeInteger(this.handshakeTimeoutMs) || this.handshakeTimeoutMs <= 0) {
      throw new RangeError("handshakeTimeoutMs must be a positive safe integer.");
    }
    const baseCapabilities = BASE_CAPABILITIES.map((capability) => {
      if (capability.id === CORE_LIMITS_CAPABILITY_ID) return Object.freeze({ ...capability, parameters: encodePeerProtocolLimits(this.limits) });
      if (capability.id === CORE_LIVENESS_CAPABILITY_ID) return Object.freeze({ ...capability, parameters: encodeLivenessParameters(this.liveness) });
      return capability;
    });
    const optionalCoreCapabilities: CapabilityDescriptor[] = this.datagrams ? [{
      id: CORE_DATAGRAM_CAPABILITY_ID,
      minVersion: 1,
      maxVersion: 1,
      parameters: encodeDatagramCapability(this.datagrams.maxInboundBytes)
    }] : [];
    const extensions = [...(options.extensions ?? [])];
    const extensionCapabilities = extensions.map((extension) => {
      if (typeof extension.attach !== "function") throw new TypeError(`Protocol extension ${extension.capability.id} must implement attach(session).`);
      validateCapabilityDescriptor(extension.capability);
      return Object.freeze({
        ...extension.capability,
        ...(extension.capability.parameters === undefined ? {} : { parameters: extension.capability.parameters.slice() })
      });
    });
    const ids = new Set<string>();
    for (const capability of [...baseCapabilities, ...optionalCoreCapabilities, ...extensionCapabilities]) {
      validateCapabilityDescriptor(capability);
      if (ids.has(capability.id)) throw new TypeError(`Duplicate protocol capability: ${capability.id}`);
      ids.add(capability.id);
    }
    for (const extension of extensions) {
      for (const required of extension.requires ?? []) {
        if (!ids.has(required)) throw new TypeError(`Protocol extension ${extension.capability.id} requires unavailable capability: ${required}`);
      }
    }
    for (const required of options.policy?.require ?? []) {
      if (!ids.has(required)) throw new TypeError(`Required capability is not installed in this runtime: ${required}`);
    }
    this.extensions = Object.freeze(extensions.map((extension, index) => Object.freeze({
      capability: extensionCapabilities[index]!,
      ...(extension.requires === undefined ? {} : { requires: Object.freeze([...extension.requires]) }),
      attach: (session: Parameters<ProtocolExtension["attach"]>[0]) => extension.attach(session)
    })));
    this.capabilities = Object.freeze([
      ...baseCapabilities,
      ...optionalCoreCapabilities.map((capability): CapabilityDescriptor => Object.freeze({
        ...capability,
        ...(capability.parameters === undefined ? {} : { parameters: capability.parameters.slice() })
      })),
      ...extensionCapabilities
    ]);
    this.policy = Object.freeze({
      require: Object.freeze([...new Set([
        ...baseCapabilities.map((capability) => capability.id),
        ...(options.policy?.require ?? [])
      ])])
    });
  }
}

export const createRuntime = (options?: RuntimeOptions): ProtocolRuntime => new ProtocolRuntime(options);
