export const RSOCKET_VERSION_MAJOR = 1;
export const RSOCKET_VERSION_MINOR = 0;
export const RSOCKET_HEADER_BYTES = 6;
export const RSOCKET_MAX_FRAME_BYTES = 0x00ffffff;
export const RSOCKET_MAX_STREAM_ID = 0x7fffffff;
export const RSOCKET_MAX_REQUEST_N = 0x7fffffff;

export enum RSocketFrameType {
  SETUP = 0x01,
  LEASE = 0x02,
  KEEPALIVE = 0x03,
  REQUEST_RESPONSE = 0x04,
  REQUEST_FNF = 0x05,
  REQUEST_STREAM = 0x06,
  REQUEST_CHANNEL = 0x07,
  REQUEST_N = 0x08,
  CANCEL = 0x09,
  PAYLOAD = 0x0a,
  ERROR = 0x0b,
  METADATA_PUSH = 0x0c,
  RESUME = 0x0d,
  RESUME_OK = 0x0e,
  EXT = 0x3f
}

export const RSOCKET_FLAG_IGNORE = 0x0200;
export const RSOCKET_FLAG_METADATA = 0x0100;
export const RSOCKET_FLAG_FOLLOWS = 0x0080;
export const RSOCKET_FLAG_COMPLETE = 0x0040;
export const RSOCKET_FLAG_NEXT = 0x0020;
export const RSOCKET_FLAG_RESPOND = 0x0080;
export const RSOCKET_FLAG_RESUME = 0x0080;
export const RSOCKET_FLAG_LEASE = 0x0040;

export enum RSocketErrorCode {
  INVALID_SETUP = 0x00000001,
  UNSUPPORTED_SETUP = 0x00000002,
  REJECTED_SETUP = 0x00000003,
  REJECTED_RESUME = 0x00000004,
  CONNECTION_ERROR = 0x00000101,
  CONNECTION_CLOSE = 0x00000102,
  APPLICATION_ERROR = 0x00000201,
  REJECTED = 0x00000202,
  CANCELED = 0x00000203,
  INVALID = 0x00000204
}

export interface RSocketSetupFields {
  readonly major: number;
  readonly minor: number;
  readonly keepAliveMs: number;
  readonly lifetimeMs: number;
  readonly metadataMimeType: string;
  readonly dataMimeType: string;
  readonly resumeToken?: Uint8Array;
}

export interface RSocketFrame {
  readonly type: RSocketFrameType;
  readonly streamId: number;
  readonly flags?: number;
  readonly metadata?: Uint8Array;
  readonly data?: Uint8Array;
  readonly initialRequestN?: number;
  readonly requestN?: number;
  readonly errorCode?: number;
  readonly lastReceivedPosition?: bigint;
  readonly setup?: RSocketSetupFields;
  readonly leaseTtlMs?: number;
  readonly leaseRequests?: number;
}
