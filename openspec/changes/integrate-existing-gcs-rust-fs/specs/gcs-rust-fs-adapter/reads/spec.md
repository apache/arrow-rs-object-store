# Spec Delta

## Purpose

Defines observable `ObjectStore` read, range-read, and metadata semantics for the `object_store::gcs_rust_fs` adapter backed by `gcs-rust-fs` `0.1.3` (`CachedFs` readahead and shared `PageCache`).

## ADDED Requirements

### Requirement: Streaming object and byte-range reads with full metadata
`GcsRustFs` SHALL implement `ObjectStore::get_opts` and `ObjectStore::head` by resolving object metadata (`size`, `last_modified`, `e_tag`, and `version`) and streaming byte chunks via `gcs-rust-fs` file handles so `CachedFs` readahead and shared `256 KiB` `PageCache` are engaged without buffering whole objects upfront.

#### Scenario: Full and bounded reads return complete object size and exact range
- **WHEN** a caller writes a `14`-byte object and invokes `get_opts` with `GetRange::Bounded(2..5)`
- **THEN** `GetResult::meta.size` equals `14`, `GetResult::range` equals `2..5`, and the streamed payload equals the `3` requested bytes

#### Scenario: Version-pinned read fetches requested generation
- **WHEN** an object is overwritten and a caller invokes `get_opts` with `GetOptions::version` set to the earlier `ObjectMeta::version`
- **THEN** `GcsRustFs` reads the pinned `#<generation>` from `gcs-rust-fs` and returns the earlier generation's content

#### Scenario: Directory prefix lookup returns NotFound
- **WHEN** `dir/file.json` exists and a caller invokes `get("dir")` or `head("dir")`
- **THEN** `GcsRustFs` returns `object_store::Error::NotFound` rather than treating the directory prefix as a readable object

### Requirement: Strict ObjectStore range-bound error reporting
`GcsRustFs` SHALL validate requested byte ranges against the resolved object size using `ObjectStore` range rules rather than clamping out-of-bounds start offsets to empty slices.

#### Scenario: Range starting at or beyond object size fails with InvalidRange
- **WHEN** a caller invokes `get_range` or `get_opts` with `200..300` or `GetRange::Offset(100)` on a `14`-byte object
- **THEN** the call fails with `object_store::Error::InvalidRange`

#### Scenario: Range extending beyond EOF clamps upper bound
- **WHEN** a caller invokes `get_opts` with `GetRange::Bounded(2..100)` or `GetRange::Suffix(100)` on a `14`-byte object
- **THEN** the call succeeds with `range` clamped to `2..14` or `0..14` respectively

### Requirement: Concurrent vectored range reads over shared PageCache
`GcsRustFs` SHALL implement `ObjectStore::get_ranges` by opening a random-access `gcs-rust-fs` read handle, validating all requested ranges upfront, and executing `File::read_range` concurrently so `CachedFs` Mode 2 `PageCache` pages are populated and reused across calls.

#### Scenario: Multi-range read returns slices in request order
- **WHEN** a caller invokes `get_ranges` with `[0..1, 2..3, 0..5]` on an existing object
- **THEN** `GcsRustFs` returns a `Vec<Bytes>` of length `3` matching each requested byte slice in order
