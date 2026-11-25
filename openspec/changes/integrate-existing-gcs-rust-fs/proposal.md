# Proposal

## Why

We want to validate `gcs-rust-fs` (`0.1.3`) as an [`ObjectStore`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/src/lib.rs#L921-L1318) backend inside `arrow-rs-object-store` immediately—gaining gRPC `BidiReadObject` transport, Linux-style async readahead, and shared `256 KiB` `PageCache` for Read, Write, and List workloads today without blocking on upstream `gcs-rust-fs` API additions.

## What Changes

- Add an optional `gcs-rust-fs` Cargo feature and `object_store::gcs_rust_fs` module (`GcsRustFs` and `GcsRustFsBuilder`) wrapping `Arc<dyn gcs_rust_fs::FileSystem>` (`CachedFs<GcsFs>` with Mode 2 `PageCache` by default, plus custom `FileSystem` injection for offline testing).
- Implement [`ObjectStore`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/src/lib.rs#L921-L1318) Read operations (`get_opts`, `head`, `get_ranges`) over `gcs_rust_fs::FileSystem::open` (`ReadaheadFile`), enforcing strict [`GetRange::as_range`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/src/util.rs#L238-L265) bounds, generation-pinned reads (`GetOptions::version`), and `NotFound` mapping on directory prefixes.
- Implement [`ObjectStore`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/src/lib.rs#L921-L1318) Write and lifecycle operations (`put_opts`, `put_multipart_opts`, `delete_stream`, `copy_opts`, `rename`) over `pipe_file`, an ordered `MultipartUpload` coordinator around `open(Write)`, `rm_file`, `copy_file`, and `move_file`.
- Implement [`ObjectStore`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/src/lib.rs#L921-L1318) List operations (`list`, `list_with_offset`, `list_with_delimiter`) over `find` and `ls`, bridging `fsspec` file-echo and missing-prefix `NotFound` behaviors to `ObjectStore` invariants.
- Add offline Read/Write/List integration tests (backed by `CachedFs` over an in-memory `gcs_rust_fs::FileSystem` test double) and env-gated live Cloud Storage Read/Write/List integration tests reusing [`src/integration.rs`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/src/integration.rs) (`put_get_delete_list`, `list_uses_directories_correctly`, `list_with_delimiter`, `stream_get`).

## Capabilities

### New Capabilities
- `gcs-rust-fs-adapter/reads`: `ObjectStore` metadata (`head`), streaming reads (`get_opts`), and single/vectored byte-range reads (`get_range`, `get_ranges`) backed by `gcs-rust-fs` `CachedFs` readahead and shared `PageCache`.
- `gcs-rust-fs-adapter/writes`: `ObjectStore` single-shot writes (`put_opts`), concurrent ordered multipart uploads (`put_multipart_opts`), bulk deletion (`delete_stream`), and server-side copy/move (`copy_opts`, `rename`) over `gcs-rust-fs`.
- `gcs-rust-fs-adapter/listing`: `ObjectStore` recursive prefix listing (`list`), exclusive offset listing (`list_with_offset`), and `/` delimiter listing (`list_with_delimiter`) over `gcs-rust-fs` `find` and `ls`.

### Modified Capabilities

## Impact

- **Dependencies & Features**: [Cargo.toml](file:///usr/local/google/home/princer/code/arrow-rs-object-store/Cargo.toml) adds optional dependency `gcs-rust-fs = { version = "0.1.3", optional = true }` and feature `gcs-rust-fs = ["dep:gcs-rust-fs", "tokio"]`. Default features, WASM targets, and [`src/gcp`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/src/gcp/mod.rs) remain completely unchanged (note: compiling with `--features gcs-rust-fs` requires Rust `>= 1.91` and `protoc`).
- **Public API**: [src/lib.rs](file:///usr/local/google/home/princer/code/arrow-rs-object-store/src/lib.rs) exports `#[cfg(all(feature = "gcs-rust-fs", not(target_arch = "wasm32")))] pub mod gcs_rust_fs;` containing `GcsRustFs` and `GcsRustFsBuilder`.
- **Unsupported Phase 2 `ObjectStore` options on `0.1.3`**: Conditional `GetOptions` HTTP headers (`if_match`, `if_none_match`, `if_modified_since`, `if_unmodified_since`), `PutMode::Update`, `CopyMode::Create`, and non-`Content-Type`/non-`Metadata` HTTP `Attributes` return explicit `NotSupported` / `NotImplemented` errors until `gcs-rust-fs` adds those primitives (tracked separately in [`gcs-rust-fs-object-store-primitives`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/openspec/changes/gcs-rust-fs-object-store-primitives)).
