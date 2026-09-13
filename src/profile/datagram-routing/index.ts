import type { ProtocolExtension } from "../../core/capabilities";
import { CORE_DATAGRAM_CAPABILITY_ID } from "../../core/datagram";
import { CapabilityMismatchError, PureReactiveProtocolError } from "../../core/errors";
import {
  getNegotiatedCapabilities,
  registerDatagramAcceptor,
  type ReactiveSession,
  type SessionDatagram
} from "../../core/session";

export const DATAGRAM_ROUTING_CAPABILITY_ID = "prp.profile.datagram-routing";
export const DATAGRAM_ROUTING_PROFILE_ID = "datagram-routing/1";
export const DATAGRAM_ROUTING_MAGIC = 0x50525231; // PRR1
export const DATAGRAM_ROUTING_HEADER_BYTES = 6;

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
const ROUTE_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._/-]*$/;
const MAX_ROUTE_BYTES = 255;
const MAX_ROUTES = 0xffff;

export interface DatagramRoutingProfileOptions {
  readonly routes: readonly string[];
}

export interface RoutedDatagram {
  readonly route: string;
  readonly data: Uint8Array;
}

export type DatagramRouteHandler = (datagram: RoutedDatagram) => void | Promise<void>;

interface RouteState {
  readonly routeToId: ReadonlyMap<string, number>;
  readonly idToRoute: ReadonlyMap<number, string>;
  readonly handlers: Map<string, Set<DatagramRouteHandler>>;
  readonly sendTails: Map<string, Promise<void>>;
  readonly latest: Map<string, LatestEntry>;
  readonly disposeAcceptor: () => void;
}

interface LatestEntry {
  data: Uint8Array;
  waiters: Array<{ resolve: () => void; reject: (error: unknown) => void }>;
  running: boolean;
}

const states = new WeakMap<ReactiveSession, RouteState>();

const validateRoute = (route: string): Uint8Array => {
  if (!ROUTE_PATTERN.test(route)) throw new TypeError(`Invalid datagram route: ${route}`);
  const bytes = encoder.encode(route);
  if (bytes.byteLength === 0 || bytes.byteLength > MAX_ROUTE_BYTES) {
    throw new RangeError(`Datagram route must encode to 1..${MAX_ROUTE_BYTES} bytes: ${route}`);
  }
  return bytes;
};

const normalizeRoutes = (routes: readonly string[]): readonly string[] => {
  const values = [...new Set(routes)];
  if (values.length !== routes.length) throw new TypeError("Datagram routing profile routes must be unique.");
  if (values.length > MAX_ROUTES) throw new RangeError(`Datagram routing profile supports at most ${MAX_ROUTES} routes.`);
  for (const route of values) validateRoute(route);
  values.sort();
  return Object.freeze(values);
};

export const encodeDatagramRoutes = (routes: readonly string[]): Uint8Array => {
  const normalized = normalizeRoutes(routes);
  const encoded = normalized.map((route) => validateRoute(route));
  const total = 2 + encoded.reduce((sum, value) => sum + 1 + value.byteLength, 0);
  const output = new Uint8Array(total);
  const view = new DataView(output.buffer);
  view.setUint16(0, encoded.length);
  let offset = 2;
  for (const value of encoded) {
    output[offset] = value.byteLength;
    offset += 1;
    output.set(value, offset);
    offset += value.byteLength;
  }
  return output;
};

export const decodeDatagramRoutes = (parameters: Uint8Array | undefined): readonly string[] => {
  if (!parameters || parameters.byteLength < 2) throw new CapabilityMismatchError("datagram-routing/1 is missing its route table.");
  const view = new DataView(parameters.buffer, parameters.byteOffset, parameters.byteLength);
  const count = view.getUint16(0);
  const routes: string[] = [];
  let offset = 2;
  for (let index = 0; index < count; index += 1) {
    if (offset >= parameters.byteLength) throw new CapabilityMismatchError("datagram-routing/1 route table is truncated.");
    const length = parameters[offset++]!;
    if (length <= 0 || offset + length > parameters.byteLength) throw new CapabilityMismatchError("datagram-routing/1 contains an invalid route length.");
    let route: string;
    try { route = decoder.decode(parameters.subarray(offset, offset + length)); }
    catch (cause) { throw new CapabilityMismatchError("datagram-routing/1 contains invalid UTF-8.", { cause }); }
    offset += length;
    validateRoute(route);
    routes.push(route);
  }
  if (offset !== parameters.byteLength) throw new CapabilityMismatchError("datagram-routing/1 route table has trailing bytes.");
  return normalizeRoutes(routes);
};

const encodeRoutedDatagram = (routeId: number, data: Uint8Array): Uint8Array => {
  if (!Number.isInteger(routeId) || routeId <= 0 || routeId > MAX_ROUTES) throw new RangeError("Datagram route id is outside uint16 range.");
  const output = new Uint8Array(DATAGRAM_ROUTING_HEADER_BYTES + data.byteLength);
  const view = new DataView(output.buffer);
  view.setUint32(0, DATAGRAM_ROUTING_MAGIC);
  view.setUint16(4, routeId);
  output.set(data, DATAGRAM_ROUTING_HEADER_BYTES);
  return output;
};

const isRoutingEnvelope = (data: Uint8Array): boolean => {
  if (data.byteLength < 4) return false;
  return new DataView(data.buffer, data.byteOffset, data.byteLength).getUint32(0) === DATAGRAM_ROUTING_MAGIC;
};

const decodeRoutedDatagram = (data: Uint8Array): { routeId: number; data: Uint8Array } | undefined => {
  if (data.byteLength < DATAGRAM_ROUTING_HEADER_BYTES) return undefined;
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  if (view.getUint32(0) !== DATAGRAM_ROUTING_MAGIC) return undefined;
  const routeId = view.getUint16(4);
  if (routeId === 0) return undefined;
  return { routeId, data: data.slice(DATAGRAM_ROUTING_HEADER_BYTES) };
};

export const datagramRoutingProfile = (options: DatagramRoutingProfileOptions): ProtocolExtension => {
  const localRoutes = normalizeRoutes(options.routes);
  return {
    capability: {
      id: DATAGRAM_ROUTING_CAPABILITY_ID,
      minVersion: 1,
      maxVersion: 1,
      parameters: encodeDatagramRoutes(localRoutes)
    },
    requires: [CORE_DATAGRAM_CAPABILITY_ID],
    attach(session) {
      if (!session.supports(CORE_DATAGRAM_CAPABILITY_ID)) {
        throw new CapabilityMismatchError("datagram-routing/1 requires negotiated prp.core.datagram.");
      }
      const capability = getNegotiatedCapabilities(session).get(DATAGRAM_ROUTING_CAPABILITY_ID);
      if (!capability) throw new CapabilityMismatchError("datagram-routing/1 was attached without negotiation.");
      const local = new Set(decodeDatagramRoutes(capability.local.parameters));
      const remote = new Set(decodeDatagramRoutes(capability.remote.parameters));
      const common = [...local].filter((route) => remote.has(route)).sort();
      const routeToId = new Map<string, number>();
      const idToRoute = new Map<number, string>();
      common.forEach((route, index) => {
        const id = index + 1;
        routeToId.set(route, id);
        idToRoute.set(id, route);
      });
      const handlers = new Map<string, Set<DatagramRouteHandler>>();
      const acceptor = {
        accepts(datagram: SessionDatagram): boolean {
          // Once the PRR1 magic is present the routing profile owns the packet; malformed or unknown routes are dropped.
          return isRoutingEnvelope(datagram.data);
        },
        async handle(datagram: SessionDatagram): Promise<void> {
          const decoded = decodeRoutedDatagram(datagram.data);
          if (!decoded) return;
          const route = idToRoute.get(decoded.routeId);
          if (!route) return;
          const routed: RoutedDatagram = { route, data: decoded.data };
          for (const handler of [...(handlers.get(route) ?? [])]) {
            try { await handler(routed); } catch { /* application datagram handlers are isolated */ }
          }
        }
      };
      const disposeAcceptor = registerDatagramAcceptor(session, acceptor);
      states.set(session, {
        routeToId,
        idToRoute,
        handlers,
        sendTails: new Map(),
        latest: new Map(),
        disposeAcceptor
      });
      return () => {
        disposeAcceptor();
        states.delete(session);
      };
    }
  };
};

export class DatagramPeer {
  constructor(private readonly session: ReactiveSession) {
    if (!session.supports(DATAGRAM_ROUTING_CAPABILITY_ID)) {
      throw new CapabilityMismatchError("datagram-routing/1 is not negotiated for this session.");
    }
    if (!states.has(session)) throw new CapabilityMismatchError("datagram-routing/1 profile is not attached to this session.");
  }

  get maxPayloadBytes(): number {
    return Math.max(0, this.session.maxDatagramBytes - DATAGRAM_ROUTING_HEADER_BYTES);
  }

  routes(): readonly string[] {
    return Object.freeze([...this.state().routeToId.keys()]);
  }

  supports(route: string): boolean {
    return this.state().routeToId.has(route);
  }

  route(route: string, handler: DatagramRouteHandler): () => void {
    if (!this.supports(route)) throw new PureReactiveProtocolError(`Datagram route was not negotiated: ${route}`, "DATAGRAM_ROUTE_UNAVAILABLE");
    const state = this.state();
    let handlers = state.handlers.get(route);
    if (!handlers) {
      handlers = new Set();
      state.handlers.set(route, handlers);
    }
    handlers.add(handler);
    return () => {
      handlers?.delete(handler);
      if (handlers?.size === 0) state.handlers.delete(route);
    };
  }

  async send(route: string, data: Uint8Array): Promise<void> {
    const state = this.state();
    const routeId = state.routeToId.get(route);
    if (routeId === undefined) throw new PureReactiveProtocolError(`Datagram route was not negotiated: ${route}`, "DATAGRAM_ROUTE_UNAVAILABLE");
    if (!(data instanceof Uint8Array)) throw new TypeError("Routed datagram payload must be a Uint8Array.");
    if (data.byteLength > this.maxPayloadBytes) {
      throw new RangeError(`Routed datagram exceeds ${this.maxPayloadBytes} application bytes for route ${route}.`);
    }
    await this.session.sendDatagram(encodeRoutedDatagram(routeId, data));
  }

  /**
   * Coalesces queued sends for one route while a previous send is in flight. All callers resolve
   * when a datagram at least as new as their value has been handed to the carrier.
   */
  sendLatest(route: string, data: Uint8Array): Promise<void> {
    if (!this.supports(route)) return Promise.reject(new PureReactiveProtocolError(`Datagram route was not negotiated: ${route}`, "DATAGRAM_ROUTE_UNAVAILABLE"));
    if (!(data instanceof Uint8Array)) return Promise.reject(new TypeError("Routed datagram payload must be a Uint8Array."));
    if (data.byteLength > this.maxPayloadBytes) return Promise.reject(new RangeError(`Routed datagram exceeds ${this.maxPayloadBytes} application bytes for route ${route}.`));
    const state = this.state();
    let entry = state.latest.get(route);
    if (!entry) {
      entry = { data: data.slice(), waiters: [], running: false };
      state.latest.set(route, entry);
    } else {
      entry.data = data.slice();
    }
    const promise = new Promise<void>((resolve, reject) => entry!.waiters.push({ resolve, reject }));
    if (!entry.running) {
      entry.running = true;
      void this.drainLatest(route, entry);
    }
    return promise;
  }

  private async drainLatest(route: string, entry: LatestEntry): Promise<void> {
    const state = this.state();
    while (state.latest.get(route) === entry && entry.waiters.length > 0) {
      const data = entry.data;
      const waiters = entry.waiters.splice(0);
      try {
        await this.send(route, data);
        for (const waiter of waiters) waiter.resolve();
      } catch (error) {
        for (const waiter of waiters) waiter.reject(error);
      }
    }
    entry.running = false;
    if (entry.waiters.length === 0) state.latest.delete(route);
    else if (!entry.running) {
      entry.running = true;
      void this.drainLatest(route, entry);
    }
  }

  private state(): RouteState {
    const state = states.get(this.session);
    if (!state) throw new PureReactiveProtocolError("Datagram routing profile is detached.", "DATAGRAM_ROUTING_DETACHED");
    return state;
  }
}
