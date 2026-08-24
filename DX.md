# Developer experience

PRP treats complexity as a layering problem rather than an optional-configuration problem.

## No fake switches

Applications do not enable implementation by writing `resume: true`, `fragmentation: true`, or `rpc: true`.

Capabilities come from code that is actually installed. Policy may require installed capabilities. RPC becomes usable only after the installed RPC profile and codec identity negotiate successfully.

## Host-language pull is protocol demand

JavaScript iterator `next()` already means “give me the next value”. PRP maps it to one logical-item demand unit rather than requiring the application to express the same intent twice. Fragmentation stays invisible to this contract.

`return()` / `for await ... break` maps to stream cancellation. `AbortSignal` is used at API boundaries rather than inventing a package-specific cancellation token.

## Large items do not create a second API

There is no `sendLarge`, `sendFragment`, `fragmentSize`, or `enableFragmentation` application option. `send()` always sends one logical item. Core chooses wire fragmentation from negotiated limits.

## Progressive disclosure

```text
root                           session / stream / connect / accept
/core                          protocol/runtime/limits/extension authoring
/transport                     generic lane adapters
/browser                       browser transports / MessagePort
/node                          Node transports
/profile/rpc                   RPC/1 profile
/rxjs                          RxJS adapter
/compatibility/rsocket-v1      RSocket 1.0 compatibility session/wire
/compatibility/rsocket-v1/node Node RSocket TCP/Duplex adapters
```

## RSocket does not create a second application model

A user who chooses RSocket compatibility still obtains `ReactiveSession` and can create `RpcPeer`. RSocket-specific frame types stay behind the compatibility subpath.

The public `ReactiveStream.id` is an adapter-level logical identity, not a promise that applications can infer or manage raw RSocket wire stream IDs. The compatibility state machine allocates and commits RSocket wire IDs only when an initial request is actually emitted. Likewise, applications never issue RSocket fragments directly; `send()` remains a logical-item API.

## Errors preserve useful causes

Examples:

- missing required capability -> `CAPABILITY_MISMATCH`;
- unavailable/mismatched RPC profile -> RPC capability error;
- malformed wire state -> `PROTOCOL_VIOLATION`;
- local oversized logical item -> local validation error without detaching a healthy session;
- silent native peer -> `LIVENESS_TIMEOUT`;
- physical attachment failure -> `CONNECTION_LOST`.

## Structural tolerance, semantic strictness

Unknown optional attributes are tolerated. Missing optional information is normal. Malformed known structures, unsupported required semantics, demand violations, illegal stream identity, invalid fragmentation, and corrupt core-owned UTF-8 are explicit errors.
