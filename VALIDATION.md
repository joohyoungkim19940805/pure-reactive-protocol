# Validation status — 0.0.1-alpha alpha-closure snapshot

This file separates implemented semantics, local regression evidence, declared-toolchain status, and independent interoperability.

## Alpha-closure semantics implemented

### Native PRP/1

- strict frame/TLV/header/UTF-8 validation;
- session handshake, capability and peer-limit negotiation;
- exact stream-ID parity/+2 allocation and sequence ordering;
- single physical writer;
- logical-item demand and iterator cancellation;
- transparent fragmentation/reassembly with bounded aggregate memory;
- `maxInboundItemBytes` enforced for both fragmented and unfragmented inbound `DATA`;
- clean `CLOSE` vs physical detach;
- native `PING/PONG` liveness and silent-peer timeout;
- extension attach-before-ready and disposal;
- bounded raw transport queues and byte-stream framing;
- local validation failures do not consume stream IDs or detach a healthy session.

### RPC/1

- RPC is an installed/negotiated capability rather than an OPEN-string convention;
- codec identity is part of capability semantics;
- JSON/binary codec mismatch is rejected;
- unary, notification, server-stream, and duplex channel mappings;
- AbortSignal/cancellation and early iterator termination;
- transparent native fragmentation underneath the profile;
- committed fragmentation/RPC regression uses `rpcProfile()` on both peers.

### RSocket 1.0 compatibility alpha closure

- RSocket frame codec, SETUP, request-response, fire-and-forget, request-stream, request-channel, REQUEST_N, PAYLOAD, completion, cancel/error, metadata push, composite metadata, and routing metadata;
- lazy fragmentation before whole-frame encoding, including logical payloads larger than the RSocket 24-bit frame ceiling;
- `CANCEL`/`ERROR` termination of incomplete fragmented sequences without detaching a valid connection;
- PAYLOAD `F+C` rule (Complete wins over Follows);
- session-wide incomplete-reassembly byte budget plus bounded fragment count;
- periodic requester/client KEEPALIVE independent of unrelated inbound application traffic;
- unknown frame types ignored only when the IGNORE flag permits it;
- strict unsigned 31-bit field validation;
- remote requester stream IDs validated as contiguous parity `+2`;
- local wire stream IDs committed only after the initial request has actually been written;
- RSocket 24-bit length-prefixed byte-stream framing and Node TCP/TLS/accepted-Duplex adapters.

## Local regression evidence for this snapshot

The exact package dependencies could not be installed in this environment because npm registry access timed out. Therefore the following is **not** a claim that the package's actual Vitest/TypeScript-7/tsdown toolchain ran here.

What was run against the final source tree:

- strict TypeScript 5.8.3 compile of generic/core/browser/profile/RSocket/test sources using a minimal Vitest declaration shim;
- strict Node source compile using the Node 22 type declarations available in the environment;
- strict RxJS adapter-shape compile using a minimal local RxJS declaration shim;
- all 46 committed test cases executed through a small Vitest-compatible fallback runner: `TESTS 46 FAILED 0`;
- focused alpha-closure runtime harness: `ALPHA_CLOSURE_HARNESS_PASS`;
- actual localhost TCP compatibility round trip through `RSocketTcpTransport` and accepted `RSocketNodeDuplexTransport`, including a 3 MiB fragmented binary RPC payload: `RSOCKET_TCP_ALPHA_CLOSURE_PASS`.

The 46 committed tests include direct regressions for: native unfragmented item limits, >16 MiB RSocket logical fragmentation, cancellation during fragmentation, PAYLOAD F+C, IGNORE, periodic KEEPALIVE, remote stream-ID skip, local wire-ID rollback, aggregate reassembly pressure, and 31-bit reserved-field rejection.

## Still required before removing `alpha`

This snapshot does **not** yet claim independent interoperability with:

- Spring WebFlux / rsocket-java;
- browser-to-Spring RSocket over WebSocket;
- Spring `RSocketRequester` -> PRP RSocket acceptor;
- the full real WebSocket/WebTransport/TLS/HTTP2 endpoint matrix;
- long-running production soak/fault-injection results;
- comparative raw WebSocket vs RSocket vs PRP performance results.

It also does not claim that the exact declared npm build/test stack has run in this container. In normal CI/developer infrastructure, the first gate should be:

```bash
npm install
npm run check
cargo test --manifest-path rust-kernel/Cargo.toml
npm run build:wasm
```

`cargo`, `rustc`, and `wasm-pack` are unavailable in this environment, so the Rust/Wasm validator source can be kept aligned but was not compiled here.

After that gate is green, execute the external interoperability matrix in `ROADMAP.md`. Only after those tests and the later performance/soak phase should the package drop the alpha label.
