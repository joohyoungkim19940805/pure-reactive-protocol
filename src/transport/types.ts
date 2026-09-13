export type LaneReliability = "reliable" | "best-effort";
export type LaneOrdering = "ordered" | "unordered";

export interface LaneRequirements {
  readonly reliability: LaneReliability;
  readonly ordering: LaneOrdering;
  /** Maximum raw PRP frame size this receiver admits on the lane. */
  readonly maxFrameBytes?: number;
}

export const RELIABLE_ORDERED_LANE: LaneRequirements = Object.freeze({
  reliability: "reliable",
  ordering: "ordered"
});

export const BEST_EFFORT_UNORDERED_LANE: LaneRequirements = Object.freeze({
  reliability: "best-effort",
  ordering: "unordered"
});

export interface TransportDescription {
  readonly id: string;
  readonly traits: readonly string[];
}

export interface TransportCloseEvent {
  readonly code?: number;
  readonly reason?: string;
  readonly error?: unknown;
}

export interface TransportLane {
  readonly id: string;
  /** Maximum raw frame bytes this lane can send, when known. */
  readonly maxFrameBytes?: number;
  readonly incoming: AsyncIterable<Uint8Array>;
  write(frame: Uint8Array): Promise<void>;
  close(reason?: string): void | Promise<void>;
}

export interface TransportConnection {
  readonly description: TransportDescription;
  /** Diagnostic close event. The ordered lane is authoritative for draining protocol input. */
  readonly closed: Promise<TransportCloseEvent>;
  /** Synchronous carrier capability probe. Older transports may omit this and expose only the base lane. */
  supportsLane?(requirements: LaneRequirements): boolean;
  openLane(requirements?: LaneRequirements): Promise<TransportLane>;
  close(code?: number, reason?: string): void | Promise<void>;
}

export interface ReactiveTransport {
  readonly id: string;
  connect(signal?: AbortSignal): Promise<TransportConnection>;
}

export interface ReactiveTransportFactory {
  readonly id: string;
  readonly priority?: number;
  isSupported(): boolean;
  create(): ReactiveTransport;
}
