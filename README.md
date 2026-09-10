# @byeolnaerim/pure-reactive-protocol

Pure Reactive Protocol (PRP) is a transport-independent reactive protocol/runtime for browsers and Node.js.

The package is currently `0.0.1-alpha`; the native wire protocol is **PRP/1**. Package and wire versions are intentionally independent.

PRP is not an RSocket reimplementation. The native protocol has its own symmetric duplex-stream model, while RSocket 1.0 is provided as a compatibility wire/session that maps onto the same public `ReactiveSession` and RPC profile model.

## Core model

PRP has one application primitive: a logical full-duplex reactive stream. Each direction independently supports zero, one, or many logical `DATA` items, demand, completion, cancellation, and failure.

The native PRP/1 session control/data frames are:

- `HELLO`, `WELCOME`, `CLOSE`
- `PING`, `PONG`
- `OPEN`, `DATA`, `FRAGMENT`, `DEMAND`, `COMPLETE`, `CANCEL`, `ERROR`
- `SIGNAL`

`FRAGMENT` is wire-internal. A fragmented logical item still consumes exactly one demand unit and appears to application code as one `StreamMessage`.

## Native PRP

```ts
import { connect } from "@byeolnaerim/pure-reactive-protocol";
import { WebSocketTransport } from "@byeolnaerim/pure-reactive-protocol/browser";

const session = await connect(
  new WebSocketTransport("wss://example.com/prp")
);

const stream = await session.open();
await stream.send(new TextEncoder().encode("hello"));
await stream.complete();

for await (const message of stream) {
  console.log(new TextDecoder().decode(message.data));
}
```

`AsyncIterator.next()` grants one logical item of protocol demand. `return()` or breaking from `for await` cancels the logical stream. `request(n)` remains available for deliberate prefetch.

Native sessions negotiate limits, fragmentation, liveness, and installed optional profiles before becoming ready. Idle healthy sessions are probed automatically; applications do not need a second keepalive API.

### Browser / WebTransport

```ts
import { connect } from "@byeolnaerim/pure-reactive-protocol";
import { WebTransportTransport } from "@byeolnaerim/pure-reactive-protocol/browser";

const session = await connect(
  new WebTransportTransport("https://example.com/prp")
);
```

The alpha WebTransport carrier opens exactly one reliable bidirectional stream per session and applies native PRP/1 unsigned 32-bit byte-stream framing. Datagram and multi-lane scheduling are deliberately outside this transport revision.

## RPC/1 is an installed profile

RPC is not part of the PRP core vocabulary. Both peers must install and negotiate `rpc/1` with the same payload codec.

```ts
import { createRuntime } from "@byeolnaerim/pure-reactive-protocol/core";
import { connectWithRuntime } from "@byeolnaerim/pure-reactive-protocol/core";
import { rpcProfile, RpcPeer } from "@byeolnaerim/pure-reactive-protocol/profile/rpc";
import { WebSocketTransport } from "@byeolnaerim/pure-reactive-protocol/browser";

const runtime = createRuntime({
  extensions: [rpcProfile()]
});

const session = await connectWithRuntime(
  new WebSocketTransport("wss://example.com/prp"),
  runtime,
  { origin: "initiator" }
);

const rpc = new RpcPeer(session);
const account = await rpc.requestResponse("account.find", { id: 42 });
```

The default RPC codec is JSON. `binaryCodec` is also provided. A JSON peer and a binary peer do not silently connect as a usable RPC session: codec identity is part of the negotiated RPC capability.

## RSocket 1.0 compatibility

The compatibility entrypoint implements an RSocket 1.0 wire/session while preserving the same PRP-facing `ReactiveSession` and `RpcPeer` model.

### Browser / WebSocket

```ts
import { WebSocketTransport } from "@byeolnaerim/pure-reactive-protocol/browser";
import { connectRSocket } from "@byeolnaerim/pure-reactive-protocol/compatibility/rsocket-v1";
import { RpcPeer } from "@byeolnaerim/pure-reactive-protocol/profile/rpc";

const session = await connectRSocket(
  new WebSocketTransport("wss://example.com/rsocket")
);

const rpc = new RpcPeer(session);
const result = await rpc.requestResponse("hello", { name: "PRP" });
```

### Node / TCP

```ts
import { connectRSocket } from "@byeolnaerim/pure-reactive-protocol/compatibility/rsocket-v1";
import { RSocketTcpTransport } from "@byeolnaerim/pure-reactive-protocol/compatibility/rsocket-v1/node";
import { RpcPeer } from "@byeolnaerim/pure-reactive-protocol/profile/rpc";

const session = await connectRSocket(
  new RSocketTcpTransport({ host: "localhost", port: 7000 })
);

const rpc = new RpcPeer(session);
```

### Browser / WebTransport

```ts
import {
  connectRSocket,
  RSocketWebTransportTransport
} from "@byeolnaerim/pure-reactive-protocol/compatibility/rsocket-v1";

const session = await connectRSocket(
  new RSocketWebTransportTransport("https://example.com/rsocket")
);
```

This path puts RSocket 1.0 directly on one WebTransport bidirectional byte stream with RSocket's unsigned 24-bit length prefix. It does not wrap RSocket in PRP or PRP in RSocket.

The compatibility implementation covers RSocket 1.0 setup, periodic client keepalive, request-response, fire-and-forget, request-stream, request-channel, `REQUEST_N`, payload fragmentation/reassembly, cancel/error, metadata push, composite metadata, and routing metadata. Fragmentation is performed before whole-frame encoding, so one logical payload may exceed the 24-bit wire-frame ceiling while each emitted frame stays within the configured frame limit. Incomplete fragmented sequences are bounded by a session-wide byte budget and may be terminated by `CANCEL`/`ERROR` without corrupting the whole connection. Unknown frame types are skipped only when the RSocket IGNORE flag permits it, and requester stream IDs are validated as contiguous `+2` sequences. Lease and resume are not advertised or pretended to be implemented.

Advanced compatibility deployments may tune `maxFrameBytes`, `maxItemBytes`, `maxInFlightReassemblyBytes`, `keepAliveMs`, `lifetimeMs`, and `handshakeTimeoutMs`. The defaults keep these resource/liveness concerns inside the compatibility session rather than leaking fragmentation mechanics into application APIs.

Independent Lab runs have passed browser WebSocket and Node TCP interoperability against Spring WebFlux/rsocket-java, including reverse requests, cancellation, >16 MiB logical payload fragmentation, and periodic keepalive. Real browser-to-Java WebTransport execution remains a separate environment gate; the committed transport tests cover stream opening, framing, fragmentation boundaries, and abort cleanup without claiming that network run.

## Transport surfaces

Current transport/adaptation surfaces include:

- WebSocket
- native PRP/1 over one WebTransport reliable bidirectional stream
- generic Web `ReadableStream` / `WritableStream`
- MessagePort / Worker bridging
- Node TCP/TLS
- Node HTTP/2 full-duplex stream
- already-accepted Node `Duplex`
- in-memory transport for protocol tests
- RSocket 24-bit-framed WebTransport/byte-stream and Node TCP/TLS compatibility transports

The native core asks for reliable ordered lane semantics, not a transport brand name. WebTransport currently exposes one base lane; negotiated multi-lane scheduling remains a later protocol phase.

## Resource safety

The reference runtime bounds frame size, attributes, logical item size, concurrent incomplete reassembly bytes, inbound streams, pending unclaimed streams/signals, raw transport backlog, retained OPEN metadata, and retired-stream state. Native inbound item limits are enforced for both fragmented and unfragmented `DATA`. RSocket compatibility separately bounds logical payloads, aggregate incomplete reassembly bytes, and in-flight fragment count.

Wire-relevant native limits are negotiated. Dynamic local pressure limits remain local. Oversized logical data is rejected before sending when the peer limit is already known, and receivers independently enforce their advertised/native acceptance boundaries.

## Subpaths

```text
@byeolnaerim/pure-reactive-protocol
@byeolnaerim/pure-reactive-protocol/core
@byeolnaerim/pure-reactive-protocol/transport
@byeolnaerim/pure-reactive-protocol/browser
@byeolnaerim/pure-reactive-protocol/node
@byeolnaerim/pure-reactive-protocol/profile/rpc
@byeolnaerim/pure-reactive-protocol/rxjs
@byeolnaerim/pure-reactive-protocol/compatibility/rsocket-v1
@byeolnaerim/pure-reactive-protocol/compatibility/rsocket-v1/node
```

See `PHILOSOPHY.md`, `DX.md`, `ARCHITECTURE.md`, `PROTOCOL.md`, `SECURITY.md`, `VALIDATION.md`, and `ROADMAP.md` for the design contract and current validation boundary.
