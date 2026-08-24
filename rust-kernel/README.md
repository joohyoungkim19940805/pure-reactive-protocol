# PRP/1 Rust/Wasm kernel seam

This crate is an optional protocol-kernel seam, not a transport implementation. Browser and Node/native I/O remains host-owned.

The validator source mirrors the native PRP/1 wire contract for:

- PRP/1 magic and exact major/minor;
- exact 36-byte core header;
- known frame kinds including `CLOSE`, `PING`, `PONG`, and `FRAGMENT`;
- reserved/header flag rules;
- u64 sequence validation;
- TLV bounds and UTF-8 attribute identifiers;
- logical-data fragmentation flag/header-value shape;
- exact PING/PONG probe shape.

The current environment does not contain `cargo`, `rustc`, or `wasm-pack`, so this source must be compiled and conformance-tested in CI before publication.
