# Roadmap

The protocol-shaping alpha implementation and the internal alpha-closure pass are complete enough to begin external interoperability testing. Package version remains `0.0.1-alpha` until independent interoperability, full declared-toolchain CI, and the subsequent performance/soak work establish that the alpha label can be removed.

## Alpha semantic closure — implemented

- native PRP/1 session and duplex-stream state machine;
- demand, cancellation, clean close vs attachment loss;
- capability and peer-limit negotiation;
- transparent logical-item fragmentation/reassembly with bounded memory;
- RPC/1 profile capability negotiation including codec identity;
- native PRP/1 liveness through mandatory `PING`/`PONG` session control;
- RSocket 1.0 compatibility wire/session for the four interaction models, request-n, periodic requester keepalive, lazy large-item fragmentation, bounded reassembly, routing/composite metadata, cancel/error, IGNORE handling, and strict stream-ID/31-bit invariants;
- closure regressions for native unfragmented item limits and the RSocket conformance cases that previously remained open;
- browser and Node transport seams needed for the next test phase.

## Phase 5 — external interoperability matrix

Run independent implementations, especially Spring WebFlux / rsocket-java, rather than only PRP-to-PRP compatibility sessions.

Required paths:

- Browser PRP native -> WebFlux PRP endpoint;
- Next.js/Node PRP native -> WebFlux PRP endpoint;
- Browser PRP public API -> RSocket compatibility -> Spring WebFlux RSocket over WebSocket;
- Node PRP public API -> RSocket compatibility -> Spring WebFlux RSocket over TCP and/or WebSocket;
- Spring `RSocketRequester` -> PRP RSocket compatibility acceptor;
- browser WebSocket/WebTransport endpoint matrix;
- TLS and HTTP/2 endpoint matrix.

Include malformed-peer, disconnect, half-open, cancellation, backpressure, fragmentation, and long-idle liveness cases.

## Phase 6 — performance and wire optimization

Only after semantics and interoperability are stable, benchmark equal workloads across raw WebSocket, RSocket, native PRP, and PRP-over-RSocket compatibility.

Measure at least:

- messages/sec and payload bytes/sec;
- actual wire bytes;
- CPU and heap allocation;
- GC pressure;
- p50/p95/p99 latency;
- producer-faster-than-consumer memory growth;
- 1 / 10 / 100 / 1000 concurrent logical streams;
- 16 B / 64 B / 1 KiB / 64 KiB / 1 MiB payloads.

Use the results to revisit the 36-byte native PRP/1 header and any allocation hot paths before beta.

## Phase 7 — continuity and transport evolution

- durable replay journal and acknowledgement semantics;
- reconnect/resume across physical attachments;
- live transport migration while preserving logical stream identity;
- negotiated multi-lane scheduling for native-multiplexed transports;
- optional multi-tab/shared-worker ownership;
- native QUIC/HTTP3 and Aeron/IPC adapters when real runtime APIs and measurements justify them.

## Optional later profiles/extensions

- pub/sub profile;
- event-log/cursor profile;
- compression;
- application security/authentication profiles where carrier security is insufficient;
- best-effort datagram semantics only for applications that explicitly tolerate loss/reordering.
