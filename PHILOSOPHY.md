# Philosophy

Pure Reactive Protocol is based on a small set of non-negotiable beliefs.

## 1. One primitive, many patterns

The native protocol does not enumerate request-response, request-stream, fire-and-forget, channel, subscription, or upload as core interaction types. It provides a symmetric duplex reactive stream. Zero/one/many values in either direction are usage patterns.

## 2. Absence is a state, not an error

Missing optional information and unknown optional extension data are normal. This does not extend to semantic corruption: malformed known fields, invalid demand, illegal stream identity, invalid fragment sequences, and missing required capabilities fail explicitly.

> structural tolerance, semantic strictness

## 3. Capabilities are facts; policy is intent

Configuration cannot manufacture a protocol implementation. An installed extension contributes its capability and behavior together. RPC proves this by negotiating both `rpc/1` and codec identity before exposing `RpcPeer` semantics.

## 4. Reactive semantics should match host-language semantics

Iterator pull maps to protocol demand. Iterator termination maps to cancellation. Standard `AbortSignal` represents cancellation at public asynchronous boundaries.

## 5. Logical items are more important than wire packets

Backpressure counts application items, not fragments. Fragmentation is an internal encoding strategy constrained by negotiated peer limits and bounded memory.

## 6. Session owns semantics; connection carries bytes

TCP, WebSocket, HTTP/2, WebTransport, MessagePort, or a future carrier does not define logical stream identity, demand, or application lifecycle.

## 7. Compatibility does not define the core

RSocket 1.0 is implemented as an alternate wire/session mapping onto the same PRP-facing session/profile model. Supporting RSocket must not force RSocket interaction-frame names into the PRP/1 native core.

## 8. Simplicity must emerge from the model

A library is not simple because it exposes many optional settings with defaults. A layer is simple when concepts that do not belong to it are absent.

## 9. Future-oriented does not mean speculative API

Resume, migration, multi-lane scheduling, datagrams, compression, and future transports become public only with a real implementation and negotiation/lifecycle rule.
