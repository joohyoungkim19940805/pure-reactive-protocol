# Architecture

```text
Application
   |
   +-- optional profile (RPC / future pub-sub / event-log)
   |
ReactiveSession / ReactiveStream
   |
   +-- native PRP/1 session state machine
   |      capability + limit negotiation
   |      logical-item demand
   |      fragmentation/reassembly
   |      cancellation / lifecycle
   |      liveness
   |      one ordered writer
   |      extension lifecycle
   |
   +-- compatibility session (RSocket 1.0)
   |      same ReactiveSession integration surface
   |      RSocket interaction frames <-> generic stream patterns
   |      RSocket request-n / fragmentation / routing / keepalive
   |
TransportConnection
   +-- required reliable ordered lane
   +-- optional best-effort unordered datagram lane
   |
WebSocket | WebTransport | TCP/TLS | HTTP/2 | MessagePort | custom carrier
```

## Native core

The core owns logical session/stream semantics. It does not own routing, JSON, RPC vocabulary, RxJS, HTTP methods, or transport-specific behavior.

One session-level writer serializes reliable-frame sequence assignment and reliable-lane writes. Logical data fragmentation is core-owned so every profile gets the same item-level demand semantics without a profile-specific large-message API.

Native datagrams are deliberately outside that reliable writer. They are a separately negotiated `prp.core.datagram` lane with message boundaries, no PRP reliable sequence/stream ID, no fragmentation, and no delivery/order guarantee. A carrier cannot gain datagram capability by emulating it over its reliable lane.

## Profiles

A profile is implementation plus capability, not a string convention. The profile attaches only after capability negotiation succeeds. `rpc/1` additionally negotiates codec identity.

## Liveness

Liveness is native session control rather than an application profile. `PING/PONG` only appears on stream 0. User traffic counts as evidence of peer activity, so the runtime does not send needless probes while inbound traffic is already flowing.

## RSocket compatibility

The RSocket implementation is deliberately outside the native PRP/1 frame model. It implements a separate session that satisfies the same `ReactiveSession` integration contract used by profiles.

This proves the architectural boundary: RSocket frame vocabulary can be translated without adding `REQUEST_RESPONSE`, `REQUEST_STREAM`, `REQUEST_CHANNEL`, or `REQUEST_N` to PRP/1.

The compatibility wire supports both WebSocket message boundaries and the RSocket 24-bit frame-length prefix used by ordered byte streams such as TCP. Fragmentation is lazy and operates on metadata/data slices before whole-frame encoding. Reassembly has a session-wide resource budget, and terminal frames can abort an incomplete fragmented sequence without detaching an otherwise valid session.

RSocket wire IDs are owned by the compatibility state machine rather than the public stream object. Remote requester IDs must arrive as contiguous parity sequences, while a local wire ID is committed only after the first request frame has actually been written. This prevents local validation failures or never-started streams from creating holes in the on-wire RSocket sequence.

RSocket requester/client keepalive is periodic as required by the compatibility wire; unlike native PRP idle probing, unrelated inbound application traffic does not suppress those periodic requester keepalives.

## Session vs connection

A logical session and physical carrier are distinct concepts. Native clean close is explicit; physical loss becomes `detached`. Resume/replay/migration are not yet implemented, so detached alpha sessions terminate pending work instead of pretending transparent recovery exists.

## Routed native datagrams

`datagram-routing/1` is a profile above `prp.core.datagram`, not a new reliable frame kind. The core exposes a datagram-acceptor integration point so profiles can claim matching native datagrams while unmatched datagrams remain available to the raw session API. Capability dependencies ensure the routing profile disappears automatically on carriers that do not expose a real best-effort/unordered lane.

## Framework adapters

Framework integrations compile framework-facing declarations into PRP application semantics; they do not redefine those semantics per carrier. For example, a Spring integration can scan one PRP-specific route annotation into a single application route registry and let native `rpc/1`, `datagram-routing/1`, and RSocket compatibility adapters consume that registry. MVC, WebFlux, controller style, or functional HTTP routing therefore remain host-framework concerns rather than protocol concepts. Transport-specific controller annotations are not part of the PRP core contract.
