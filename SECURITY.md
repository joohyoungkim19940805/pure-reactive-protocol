# Security model

`0.0.1-alpha` is not production-security reviewed.

## Transport security

Native PRP/1 does not itself provide encryption, endpoint authentication, authorization, certificate validation, or payload confidentiality. Use a secure authenticated carrier appropriate to the environment (`wss:`, TLS/TCP, HTTPS-backed HTTP/2, secure WebTransport, etc.).

RSocket compatibility inherits the same rule: RSocket wire compatibility is not peer authentication.

## Untrusted input

All frame bytes, TLV attributes, capability parameters, logical payloads, RPC targets, RSocket metadata, routes, error text, and extension signals are untrusted.

The reference implementation validates and bounds protocol-owned structures before exposing application data.

## Resource exhaustion

Native PRP bounds frames, attributes, logical item size, aggregate incomplete reassembly bytes, peer-opened streams, pending incoming work, signals, retained OPEN attributes, tombstone state, and raw transport backlog.

RSocket compatibility bounds encoded frames, one reassembled logical item, session-wide bytes retained by incomplete fragmented sequences, and in-flight fragment count, and applies request-n flow control. Resource-pressure rejection is scoped to the affected RSocket stream where the wire semantics allow it rather than automatically destroying the whole session. Applications still need authentication, rate limits, authorization, quotas, and business-level payload validation.

## Liveness

Native PRP liveness distinguishes a quiet-but-responsive connection from a silent peer. It is failure detection, not a security proof. Timing values must not be treated as authentication or anti-DoS mechanisms.

## Cancellation

`ReactiveStream.signal` aborts on cancellation/failure. Application handlers should propagate it to downstream I/O. JavaScript cannot forcibly stop arbitrary code that ignores cancellation.

## Alpha warning

Before security-sensitive deployment, run the external interoperability/malformed-peer matrix, fuzz/property suites, dependency audit, and an independent protocol/security review.
