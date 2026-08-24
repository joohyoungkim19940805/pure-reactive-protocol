# PRP/1 protocol

Status: alpha native protocol shipped by package `0.0.1-alpha`.

`PRP` means **Pure Reactive Protocol**. PRP/1 is not RSocket wire format. RSocket 1.0 is a separate compatibility wire/session implemented outside this native core.

## 1. Base lane

A PRP/1 base lane provides reliable ordered frame delivery. The session serializes physical frame writes. Message-boundary transports carry one complete PRP frame per lane item. Ordered byte-stream transports prepend an unsigned 32-bit PRP frame length.

### Negotiation bootstrap

Before peers know each other's limits, `HELLO`, `WELCOME`, and negotiation-scoped session `ERROR` use a fixed bounded bootstrap envelope. After the session becomes ready, negotiated limits apply.

## 2. Identity and sequencing

- stream `0` is session-scoped;
- initiator-created streams are `1,3,5,...`;
- acceptor-created streams are `2,4,6,...`;
- each peer allocates exactly `+2`, never skipping or reusing an ID;
- each attachment independently sequences outgoing frames with a nonzero unsigned 64-bit sequence beginning at `1`;
- reception requires the exact next sequence.

## 3. Core header

All integers are unsigned big-endian.

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 4 | magic `PRP1` (`0x50525031`) |
| 4 | 1 | major `1` |
| 5 | 1 | minor `0` |
| 6 | 1 | frame kind |
| 7 | 1 | frame flags |
| 8 | 2 | header bytes, exactly `36` |
| 10 | 8 | logical stream ID |
| 18 | 8 | peer sequence |
| 26 | 4 | frame-specific header value |
| 30 | 4 | TLV attribute-area bytes |
| 34 | 2 | reserved, zero |
| 36 | n | attributes followed by payload |

PRP/1 recognizes only the defined flags for the current frame kind. Unknown flag bits are malformed. Additive semantic evolution belongs in TLV attributes/capabilities unless a later wire version explicitly changes the core header.

## 4. TLV attributes

Each attribute contains:

| Bytes | Field |
|---:|---|
| 1 | flags (`0x01` = required) |
| 2 | UTF-8 identifier length |
| 4 | opaque value length |
| n | identifier bytes |
| n | value bytes |

Attribute identifiers must be nonempty valid UTF-8. Unknown optional attributes may be ignored by a layer that does not own them. A layer that owns a context rejects unknown required semantics.

## 5. Frame kinds

| Value | Kind | Scope / purpose |
|---:|---|---|
| `0x01` | `HELLO` | session negotiation |
| `0x02` | `WELCOME` | session negotiation acknowledgement |
| `0x03` | `CLOSE` | clean logical session shutdown |
| `0x04` | `PING` | session liveness probe |
| `0x05` | `PONG` | liveness response |
| `0x10` | `OPEN` | open logical duplex stream |
| `0x11` | `DATA` | one logical item or its first fragment |
| `0x12` | `DEMAND` | grant logical-item credit |
| `0x13` | `COMPLETE` | close sender DATA direction |
| `0x14` | `CANCEL` | terminate logical stream |
| `0x15` | `ERROR` | stream- or session-scoped failure |
| `0x16` | `SIGNAL` | extension/session signal |
| `0x17` | `FRAGMENT` | continuation of a fragmented logical DATA item |

`PING` and `PONG` carry exactly one nonzero 64-bit probe ID as payload and no attributes.

## 6. Duplex stream semantics

`OPEN` creates a symmetric logical stream. Each direction independently owns sending completion and receives credit from the opposite peer.

`DEMAND(n)` grants permission for at most `n` additional **logical DATA items**. Fragment count never changes demand accounting.

At the JavaScript API, iterator `next()` grants one demand unit. Explicit `request(n)` pre-grants additional credit. Iterator `return()` sends cancellation.

`COMPLETE` ends only the sender's DATA direction. `CANCEL` ends the entire stream. `ERROR` ends the relevant stream/session with failure.

## 7. Fragmentation and reassembly

A logical item that fits the negotiated frame limit is sent as one `DATA` frame.

A larger item is encoded as:

1. a `DATA` frame with the fragmented flag, application attributes, total logical payload bytes in the frame-specific header value, and the first payload slice;
2. zero or more `FRAGMENT` frames carrying continuation bytes;
3. the receiver publishes exactly one `StreamMessage` only after the declared logical payload length is complete.

Rules:

- the declared item length must not exceed the peer-advertised `maxInboundItemBytes`;
- continuation fragments do not carry application attributes;
- no second logical DATA may start while the same direction has an incomplete item;
- `COMPLETE` during incomplete reassembly is a protocol violation;
- cancellation/error discards partial reassembly immediately;
- global incomplete-reassembly bytes are locally bounded;
- sender fragments are generated lazily rather than materializing an array of all fragments.

## 8. Mandatory native capabilities

PRP/1 requires compatible versions of:

- `prp.core.duplex-stream`
- `prp.core.tlv-attributes`
- `prp.core.sequence-u64`
- `prp.core.limits`
- `prp.core.fragmentation`
- `prp.core.liveness`

Capabilities are facts contributed by actual implementation code. User policy may require an installed capability but cannot create implementation by configuration.

### Core limits

`prp.core.limits` advertises wire-relevant inbound acceptance limits, including frame/attribute limits, `maxInboundStreams`, and `maxInboundItemBytes`. Outbound send uses the stricter applicable local/remote encoding boundary.

Dynamic pressure controls such as total in-flight reassembly bytes remain local safeguards.

### Liveness capability

`prp.core.liveness` v1 parameters contain peer liveness interval and timeout values. The reference runtime defaults to a 15 second idle probe interval and 45 second timeout. Inbound traffic resets idle observation. A silent peer that does not respond before the negotiated/local timeout is detached with `LIVENESS_TIMEOUT`.

## 9. Profiles

Profiles are not core frame kinds. An implementation installs a profile extension, advertises its capability, attaches behavior after negotiation, and only then exposes the profile API.

`rpc/1` uses capability `prp.profile.rpc` version 1. The capability parameter identifies the payload codec. Both peers must negotiate the profile and identical codec identity before `RpcPeer` is usable.

## 10. Session termination and terminal races

A clean local shutdown sends `CLOSE`. Carrier disappearance without a valid `CLOSE` is attachment loss and enters `detached` rather than being silently reclassified as an application close.

Recent retired streams are tracked with a bounded tombstone set. Stream-ID parity/frontier rules independently prevent ID reuse even after a tombstone ages out. Late idempotent terminal control frames for past IDs are tolerated where they cannot mutate a current stream.

## 11. Continuity

Logical session identity is conceptually separate from physical attachment identity. Durable replay/resume and live transport migration are intentionally not advertised in `0.0.1-alpha`; they are post-interoperability work.
