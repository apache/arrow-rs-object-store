# Proposal

## Why

Integrating [`gcs-rust-fs`](https://docs.rs/gcs-rust-fs/0.1.3/gcs_rust_fs/) as a backend for Apache Arrow's [`ObjectStore`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/src/lib.rs) unlocks gRPC Multi-Range Downloads (`BidiReadObject`), Zonal Rapid Storage buckets, folio readahead, and a generation-pinned shared `PageCache` without re-implementing caching in every query engine. However, `gcs-rust-fs 0.1.3` is shaped around stateful `fsspec` handles and whole-prefix in-memory `Vec<Entry>` listings, creating round-trip, memory, and concurrency gaps on `ObjectStore`'s stateless Read, Write, and List hot paths.

## What Changes

- Add `FileSystem::open_stream` and `StreamRead` to `gcs-rust-fs` so callers receive `ObjectStat`, resolved `[start, end)` bounds, and an incremental `BoxStream<'static, Result<Bytes>>` in 1 gRPC round-trip (`open_object().send_and_read(...)`), engaging `CachedFs` readahead on sequential scans without buffering full objects in memory.
- Add `RangeBoundsMode` (`ClampEmpty` default vs. `Strict`) and `ReadPattern` to `ReadOptions` so `ObjectStore` callers receive `ErrorKind::OutOfRange` / `ErrorKind::InvalidRange` when `start >= object_size` instead of silent empty slices.
- Add `FileSystem::read_ranges` backed by `CachedFs`'s Mode 2 `PageCache` (`256 KiB` pages) and an internal LRU `DescriptorPool` in `GcsFs` (`<=64` open gRPC `ObjectDescriptor` streams, `30s` idle TTL) so stateless `ObjectStore::get_range` and `get_ranges` calls share cached pages and open `BidiReadObject` streams.
- Extend `WriteMode` with `WriteMode::IfGenerationMatch(i64)` for server-enforced compare-and-swap (`PutMode::Update`) and add `FileSystem::put_bytes` returning `Result<ObjectStat>` so single-shot writes return `etag` and `generation` in 1 round-trip.
- Add `FileSystem::open_multipart` and `MultipartWriter` (`write_part`, `complete`, `abort`) supporting concurrent out-of-order in-memory part uploads assembled via `ComposeObject` (including 32-ary tree reduction up to `1,024` parts) on Flat/Hierarchical buckets and sequential `AppendableObjectWriter` ordering on Zonal buckets.
- Add `FileSystem::list_page` (`ListPageOptions`, `ListPage`) exposing single-page `StorageControl::list_objects` pagination (`<=1,000` items per page) with `delimiter` support, continuation `page_token`, and server-side exclusive `start_after` offset pushdown.
- Explicitly keep HTTP conditional read headers (`if_match`/`if_none_match`/`if_modified_since`), extended HTTP write headers (`Cache-Control`, `Content-Encoding`, `Content-Disposition`, `Content-Language`, `Storage-Class`), `CopyMode::Create`, non-ADC credential builders, V4 Signed URLs, and `MultipartStore` out of MVP scope (deferred to Phase 2).

## Capabilities

### New Capabilities
- `reads/streaming-and-pooled-ranges`: Single-round-trip streaming reads (`open_stream`), strict RFC 9110 range-bound checking (`RangeBoundsMode::Strict`), and stateless vectored range reads (`read_ranges`) routed through `CachedFs` `PageCache` and an LRU pool of gRPC `ObjectDescriptor` streams.
- `writes/stat-cas-and-multipart`: Single-round-trip `put_bytes` returning `ObjectStat`, `WriteMode::IfGenerationMatch(i64)` generation compare-and-swap, and concurrent in-memory `open_multipart` (`MultipartWriter`) across Flat, Hierarchical, and Zonal buckets.
- `listing/paginated-list`: Bounded-memory single-page object and prefix listing (`list_page`) with `page_token` continuation, `/` delimiter grouping, and exclusive `start_after` server-side offset pushdown.

### Modified Capabilities
<!-- None — new OpenSpec root with no prior capability specs. -->

## Impact

- Target crate: `gcs-rust-fs` (`src/options.rs`, `src/filesystem.rs`, `src/cached.rs`, `src/gcs/{fs,backend,control,write}.rs`, `src/metrics.rs`).
- Backwards compatibility: Non-breaking for existing `gcs-rust-fs 0.1.3` callers (`WriteMode` and `ReadOptions` are already `#[non_exhaustive]` or extended with defaulted fields, and new `FileSystem` methods can carry default fallback implementations where appropriate while `GcsFs` and `CachedFs` implement native fast paths).
- Downstream consumers: Enables a thin `ObjectStore` adapter (`head` -> `info`, `delete_stream` -> `rm_file`, `copy` -> `copy_file`, `rename` -> `move_file`, plus the 5 new methods above) to run DataFusion, Parquet, and Iceberg Read, Write, and List workloads end-to-end.
