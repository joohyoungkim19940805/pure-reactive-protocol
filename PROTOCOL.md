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

## 9. Native datagrams

Native PRP may additionally negotiate `prp.core.datagram` version 1 when the physical carrier exposes a genuine best-effort unordered message lane. This capability is optional and is not part of the mandatory PRP/1 base-lane capability set. A transport that has only reliable ordered delivery MUST NOT advertise it.

The v1 capability parameter is a 4-byte unsigned big-endian maximum application datagram payload accepted by the advertising peer. The effective outbound application limit is the minimum of that peer-advertised value and the physical lane limit after the PRP datagram envelope. Public APIs expose this as `maxDatagramBytes`.

A native PRP datagram is exactly one carrier datagram:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 4 | magic `PRD1` (`0x50524431`) |
| 4 | n | opaque application payload |

Datagrams do not use the 36-byte PRP/1 reliable-frame header. They have no logical stream ID, reliable-frame sequence, demand credit, fragmentation, retransmission, acknowledgement, or completion semantics. They may be lost, duplicated, or reordered. One datagram is never split by PRP; an oversize send is rejected locally. Malformed/oversize incoming datagrams are dropped without detaching an otherwise valid reliable PRP session. Local receive pressure may also drop datagrams rather than applying reliable backpressure.

Datagram traffic counts as inbound session activity for liveness observation, but loss of the optional datagram lane does not by itself detach the reliable PRP session.

## 10. Profiles

Profiles are not core frame kinds. An implementation installs a profile extension, advertises its capability, attaches behavior after negotiation, and only then exposes the profile API.

`rpc/1` uses capability `prp.profile.rpc` version 1. The capability parameter identifies the payload codec. Both peers must negotiate the profile and identical codec identity before `RpcPeer` is usable.

## 11. Session termination and terminal races

A clean local shutdown sends `CLOSE`. Carrier disappearance without a valid `CLOSE` is attachment loss and enters `detached` rather than being silently reclassified as an application close.

Recent retired streams are tracked with a bounded tombstone set. Stream-ID parity/frontier rules independently prevent ID reuse even after a tombstone ages out. Late idempotent terminal control frames for past IDs are tolerated where they cannot mutate a current stream.

## 12. Continuity

Logical session identity is conceptually separate from physical attachment identity. Durable replay/resume and live transport migration are intentionally not advertised in `0.0.1-alpha`; they are post-interoperability work.

## Datagram routing profile (`datagram-routing/1`)

`prp.profile.datagram-routing` is an optional profile layered on the native `prp.core.datagram` lane. It MUST NOT be offered when the physical carrier cannot provide the native best-effort/unordered lane.

During capability negotiation each endpoint advertises its supported application route names. Route names are ASCII identifiers matching `[A-Za-z0-9][A-Za-z0-9._/-]*`. Both endpoints compute the sorted intersection and assign uint16 route ids starting at 1; therefore route strings are not repeated on every datagram.

A routed application payload is carried inside the native PRP datagram payload as:

```text
0               31 32            47 48 ...
+----------------+----------------+------
| PRR1 magic     | uint16 routeId | data
+----------------+----------------+------
```

The routed header is 6 bytes. Together with the core `PRD1` envelope, routed native datagrams add 10 bytes before the application payload. Routed datagrams inherit the core datagram semantics: no delivery guarantee, no ordering guarantee, possible duplication, no PRP fragmentation, and no reliable stream id/sequence/demand consumption.

Datagrams that do not begin with a valid negotiated `PRR1` route envelope remain visible through the raw native datagram API. Handler failures are isolated from the reliable PRP session.

## Optional compact frames (`prp.core.compact-frames/1`)

This optional capability is offered only with `RuntimeOptions.compactFrames: true`; it is not mandatory for PRP/1. Both peers must negotiate version 1. HELLO, WELCOME and negotiation-scoped session ERROR always retain the original bounded PRP/1 bootstrap encoding. Their existing frame-kind values are already one-byte codes. The compact capability adds a one-time handshake offer rather than changing the handshake vocabulary.

After the session becomes ready, both directions use compact reliable frames if negotiated. A compact frame before readiness or without agreement is a protocol violation. A legacy reliable frame after compact mode is active is also a protocol violation. With one or both peers opting out, both directions retain the original PRP/1 encoding.

| Order | Field |
| --- | --- |
| 1 | one-byte marker `0xC1` |
| 2 | one-byte control: low five bits retain the PRP frame kind; `0x20` means attributes; `0x40` means a header value; `0x80` represents DATA's fragmented flag |
| 3 | unsigned LEB128 absolute uint64 stream ID |
| 4 | unsigned LEB128 absolute uint64 peer sequence |
| 5 | unsigned LEB128 uint32 header value, only when `0x40` is set: DEMAND credit or fragmented DATA's total length |
| 6 | compact attributes when `0x20` is set |
| 7 | the remaining frame bytes are the unmodified payload |

All varints use the shortest representation; overlong, truncated and overflowing values are rejected. Sequence zero remains invalid. HELLO/WELCOME have no compact representation. Existing frame-shape rules, stream parity, peer sequence validation, demand and cancellation still apply. No sequence precision is discarded and no stateful sequence delta is required.

Compact attributes start with a nonzero uint32 varint count. Each entry is `tag`, an optional literal ID, then `valueLength` and raw value bytes. `tag` is a uint32 varint `(dictionaryId << 1) | requiredBit`. The fixed version-1 dictionary is:

| Dictionary ID | Attribute ID |
| ---: | --- |
| 1 | `prp.profile` |
| 2 | `prp.rpc.target` |
| 3 | `prp.rpc.pattern` |
| 4 | `prp.error.code` |
| 5 | `prp.session.id` |

Dictionary ID zero escapes to `uint32 varint idByteLength` followed by the nonempty UTF-8 ID. Other dictionary IDs are rejected. `valueLength` is a uint32 varint and values remain opaque bytes. Attribute count, individual lengths, and the logical original TLV cost `7 + utf8IdBytes + valueBytes` are checked against the existing metadata limits, so dictionary compression cannot bypass resource budgets. The physical frame is separately checked against the negotiated frame limit. Fragmentation conservatively reserves the original 36-byte header and the larger of the legacy/compact attribute encodings.

The same negotiated capability changes the native Datagram envelope from `PRD1` (four bytes) to marker `0xD1` (one byte) followed by the unchanged payload. A wrong-mode native packet is dropped without detaching the reliable session. The six-byte `PRR1` routed envelope remains unchanged, so total native+routing overhead changes from ten bytes to seven. The route-name dictionary is already negotiated by `datagram-routing/1` and is not repeated per packet.

Exact examples below exclude carrier length prefixes, TLS/QUIC and application-specific headers:

| Example | Legacy bytes | Compact bytes |
| --- | ---: | ---: |
| DATA, stream 1, sequence 2, 16-byte payload | 52 | 20 |
| RPC OPEN, `hunter.world.watch` / `rpc/1` / `server-stream`, three required attributes | 133 | 47 |
| Raw native Datagram with 16-byte payload | 20 | 17 |
| Routed native Datagram with 16-byte application payload | 26 | 23 |

The exact-size regression checks every payload length from 1 through 1,100 bytes. For the same small IDs, DATA with a 1-byte payload is 37/5 bytes (legacy/compact), and with 1,100 payload bytes is 1,136/1,104 bytes. Routed native Datagrams with those payloads are 11/8 and 1,110/1,107 bytes respectively. Thus the native saving is three bytes per packet even when the application payload grows; the much larger 36-to-4 header reduction belongs to reliable frames.

Absolute IDs/sequences above 127 need more varint bytes, so the four-byte reliable header is a minimum, not a fixed promise. The reliable carrier's four-byte length prefix is unchanged. This encoding is not encryption. It does not compress JSON or game simulation data and does not establish a runtime throughput claim.

[Shared compact fixtures](compatibility/fixtures/compact-frames-v1.json) specify exact Java/TypeScript bytes, including non-ASCII unknown attributes and fragmentation.
