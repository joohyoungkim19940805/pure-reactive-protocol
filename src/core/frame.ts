import type { ProtocolAttribute } from "./attributes";

export const PROTOCOL_MAGIC = 0x50525031;
export const PROTOCOL_MAJOR = 1;
export const PROTOCOL_MINOR = 0;
export const FRAME_HEADER_BYTES = 36;

/** DATA carries the first bytes of a fragmented logical item. The header value stores total payload bytes. */
export const DATA_FRAGMENTED_FLAG = 0x01;

export enum FrameKind {
  HELLO = 0x01,
  WELCOME = 0x02,
  CLOSE = 0x03,
  PING = 0x04,
  PONG = 0x05,
  OPEN = 0x10,
  DATA = 0x11,
  DEMAND = 0x12,
  COMPLETE = 0x13,
  CANCEL = 0x14,
  ERROR = 0x15,
  SIGNAL = 0x16,
  FRAGMENT = 0x17
}

export interface ProtocolFrame {
  readonly kind: FrameKind;
  readonly streamId: bigint;
  readonly sequence: bigint;
  /** Frame-kind-specific flags. PRP/1 currently defines DATA_FRAGMENTED_FLAG on DATA only. */
  readonly flags?: number;
  /** DEMAND credit. */
  readonly credit?: number;
  /** Total logical DATA payload bytes when DATA_FRAGMENTED_FLAG is set. */
  readonly fragmentLength?: number;
  readonly attributes?: readonly ProtocolAttribute[];
  readonly payload?: Uint8Array;
}
