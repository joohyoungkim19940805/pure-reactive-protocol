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
- native PRP/1 unsigned 32-bit and RSocket unsigned 24-bit framing over one WebTransport bidirectional stream.

## Local regression evidence for this snapshot

The declared npm dependencies were installed and the package's actual toolchain ran against the final source tree:

- `tsc --noEmit`: pass;
- Vitest: 11 files, 51 tests passed;
- `tsdown`: ESM/CJS declarations and bundles built successfully;
- WebTransport regressions cover native unsigned 32-bit framing, the independent best-effort/unordered datagram lane, RSocket unsigned 24-bit framing, reliable-stream transport traits, and abort cleanup.
- Native datagram regressions cover capability negotiation, peer/carrier size intersection, best-effort delivery, local oversize rejection, carrier absence, and isolation from reliable stream ids/sequences.

The committed tests also include direct regressions for: native unfragmented item limits, >16 MiB RSocket logical fragmentation, cancellation during fragmentation, PAYLOAD F+C, IGNORE, periodic KEEPALIVE, remote stream-ID skip, local wire-ID rollback, aggregate reassembly pressure, and 31-bit reserved-field rejection.

The separate PRP Alpha Lab has reported passing independent browser WebSocket and Node TCP paths against the PRP for Java candidate and Spring WebFlux/rsocket-java. That evidence includes native unary/stream/channel/fragmentation/cancel/error/liveness, RSocket reverse request, >16 MiB fragmentation, local wire-ID rollback, and periodic keepalive.

## Still required before removing `alpha`

This snapshot does **not** yet claim independent interoperability with:

- browser-to-Java WebTransport for native PRP/1 or RSocket 1.0;
- the remaining real TLS/HTTP2 endpoint matrix;
- long-running production soak/fault-injection results;
- comparative raw WebSocket vs RSocket vs PRP performance results.

In normal CI/developer infrastructure, the complete gate remains:

```bash
npm install
npm run check
cargo test --manifest-path rust-kernel/Cargo.toml
npm run build:wasm
```

`cargo`, `rustc`, and `wasm-pack` are unavailable in this environment, so the Rust/Wasm validator source can be kept aligned but was not compiled here.

After that gate is green, execute the remaining interoperability matrix in `ROADMAP.md`. Only after those tests and the later performance/soak phase should the package drop the alpha label.
