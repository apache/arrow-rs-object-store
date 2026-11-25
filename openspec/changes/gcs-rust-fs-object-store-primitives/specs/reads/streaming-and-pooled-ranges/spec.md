# Spec Delta

## Purpose

Defines observable requirements for streaming byte-range reads, strict range-bound error reporting, and stateless multi-range reads backed by shared page caching and pooled gRPC read handles in `gcs-rust-fs`.

## ADDED Requirements

### Requirement: Streaming object and range reads with metadata
The filesystem SHALL provide an `open_stream` operation that resolves an object or byte range and returns the pinned `ObjectStat`, the resolved half-open `[start, end)` byte range, and an asynchronous byte-chunk stream without buffering the entire payload in memory first.

#### Scenario: Full-object gRPC streaming read in one round-trip
- **WHEN** a caller invokes `open_stream` on an existing object over the gRPC transport with `range = None`
- **THEN** the filesystem opens the object and initiates the read in a single round-trip, returning `ObjectStat`, `0..size`, and a stream yielding the object's bytes in order

#### Scenario: Sequential streaming read engages readahead decorator
- **WHEN** a caller invokes `open_stream` through `CachedFs` with `ReadPattern::Sequential` or `ReadPattern::Auto` on a range at least as large as the configured folio I/O size
- **THEN** the returned stream prefetches ahead asynchronously while yielding chunks sequentially without exceeding configured in-flight byte limits

### Requirement: Configurable strict range-bound validation
The filesystem SHALL support a `RangeBoundsMode::Strict` read option that rejects invalid or out-of-bounds byte ranges with an error instead of clamping them to an empty slice, while preserving `RangeBoundsMode::ClampEmpty` as the default.

#### Scenario: Strict read starting at or past object end fails with OutOfRange
- **WHEN** a caller reads a bounded or offset range with `RangeBoundsMode::Strict` where `start >= object_size`
- **THEN** the operation returns an error with `ErrorKind::OutOfRange`

#### Scenario: Strict read with inverted or empty bounds fails with InvalidRange
- **WHEN** a caller reads a bounded range with `RangeBoundsMode::Strict` where `start >= end`
- **THEN** the operation returns an error with `ErrorKind::InvalidRange`

#### Scenario: Strict read extending past EOF clamps upper bound
- **WHEN** a caller reads a bounded range `[start, end)` with `RangeBoundsMode::Strict` where `start < object_size` and `end > object_size`
- **THEN** the operation succeeds and returns bytes for `[start, object_size)`

### Requirement: Stateless multi-range reads through shared page cache
The filesystem SHALL provide a `read_ranges` operation that accepts a slice of `[start, end)` byte ranges for a single path, returns `Vec<Bytes>` in input order, and serves and populates `CachedFs`'s shared `PageCache` when Mode 2 caching is enabled.

#### Scenario: Repeated multi-range reads hit shared PageCache
- **WHEN** a caller invokes `read_ranges` twice for overlapping or identical byte ranges on the same object generation through `CachedFs` with `PageCache` enabled
- **THEN** the second call serves resident `256 KiB` pages from memory without re-fetching those ranges from Cloud Storage

#### Scenario: Adjacent cache misses are coalesced within threshold
- **WHEN** `read_ranges` encounters multiple uncached page spans separated by gaps smaller than or equal to the coalesce threshold
- **THEN** the filesystem merges adjacent misses up to the maximum folio size, stores the fetched pages in `PageCache`, and slices the exact requested byte ranges for the caller

### Requirement: Pooled gRPC read descriptors across stateless range calls
When using the gRPC transport, the filesystem SHALL pool open generation-pinned read descriptors in a bounded LRU cache with idle expiration so sequential stateless `read_ranges` calls reuse an active bidirectional read stream.

#### Scenario: Stateless range calls reuse pooled descriptor
- **WHEN** a caller issues multiple separate `read_ranges` calls against the same `(bucket, object, generation)` within the descriptor idle TTL
- **THEN** the filesystem multiplexes the range reads over the pooled gRPC read descriptor without opening a new bidirectional stream per call

#### Scenario: Reset pooled descriptor retries transparently
- **WHEN** a pooled gRPC read descriptor fails with a stream reset or transport error before delivering bytes for a range
- **THEN** the filesystem evicts the broken descriptor, opens a fresh descriptor pinned to the same generation, and retries the range read once
