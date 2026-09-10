# RSocket 1.0 compatibility

This directory implements RSocket 1.0 as a compatibility wire/session outside the native PRP/1 core.

The public goal is not a second RSocket-shaped application API. `connectRSocket()` / `acceptRSocket()` return a PRP-facing `ReactiveSession`, and the existing `RpcPeer` maps onto RSocket interaction models internally.

## Implemented

- RSocket 1.0 `SETUP`;
- periodic requester/client keepalive and lifetime detection;
- request-response;
- fire-and-forget;
- request-stream;
- request-channel;
- `REQUEST_N` flow control;
- `PAYLOAD`, completion, cancellation and error semantics;
- RSocket Follows fragmentation/reassembly while preserving logical request-n semantics, including logical payloads larger than the 24-bit frame ceiling;
- `CANCEL`/`ERROR` termination of incomplete fragmented sequences and PAYLOAD `F+C` handling;
- bounded aggregate reassembly memory and bounded in-flight fragment count;
- IGNORE-flag handling for unknown frame types;
- strict unsigned 31-bit field checks and contiguous parity stream-ID validation;
- composite metadata encoding/decoding;
- routing metadata;
- metadata push;
- WebSocket message-boundary use through the normal PRP `WebSocketTransport`;
- one reliable WebTransport bidirectional stream with RSocket unsigned 24-bit framing;
- RSocket 24-bit length-prefixed ordered byte streams;
- Node accepted `Duplex`, TCP and TLS client transports;
- symmetric requests after connection establishment.

## Deliberately not advertised

- Lease;
- Resume / replay positions;
- RSocket extension frames with application-specific semantics beyond the implemented compatibility needs.

Unsupported optional features are not advertised in SETUP and are not represented as fake configuration switches.

## Resource and lifecycle options

`connectRSocket()` / `acceptRSocket()` accept advanced bounds for `maxFrameBytes`, `maxItemBytes`, `maxInFlightReassemblyBytes`, `keepAliveMs`, `lifetimeMs`, and `handshakeTimeoutMs`. Fragmentation remains internal to the compatibility session; callers still send one logical item. A local RSocket wire stream ID is committed only when the initial request is actually written, and remote requester IDs are validated as contiguous `+2` parity sequences.

## Browser / WebSocket

```ts
import { WebSocketTransport } from "@byeolnaerim/pure-reactive-protocol/browser";
import { connectRSocket } from "@byeolnaerim/pure-reactive-protocol/compatibility/rsocket-v1";
import { RpcPeer } from "@byeolnaerim/pure-reactive-protocol/profile/rpc";

const session = await connectRSocket(new WebSocketTransport("wss://host/rsocket"));
const rpc = new RpcPeer(session);
```

## Node / TCP

```ts
import { connectRSocket } from "@byeolnaerim/pure-reactive-protocol/compatibility/rsocket-v1";
import { RSocketTcpTransport } from "@byeolnaerim/pure-reactive-protocol/compatibility/rsocket-v1/node";

const session = await connectRSocket(new RSocketTcpTransport({ host: "localhost", port: 7000 }));
```

## Browser / WebTransport

```ts
import {
  connectRSocket,
  RSocketWebTransportTransport
} from "@byeolnaerim/pure-reactive-protocol/compatibility/rsocket-v1";

const session = await connectRSocket(
  new RSocketWebTransportTransport("https://host/rsocket")
);
```

The carrier opens one reliable ordered bidirectional stream. RSocket frames are written directly with their 24-bit byte-stream prefix; native PRP framing is not involved.

## Validation boundary

The compatibility layer has self/inter-transport tests and local TCP/byte-stream round trips. Independent Lab runs have also passed Spring WebFlux/rsocket-java interoperability over browser WebSocket and Node TCP, including a Spring `RSocketRequester` reverse request. The WebTransport carrier has deterministic framing/abort tests; browser-to-Java WebTransport is not claimed until the Lab is run with its HTTP/3 TLS endpoint.
