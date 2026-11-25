# Design

## Context

See [proposal.md](file:///usr/local/google/home/princer/code/arrow-rs-object-store/openspec/changes/gcs-rust-fs-object-store-primitives/proposal.md) for motivation. Internally, `gcs-rust-fs 0.1.3` already contains most of the underlying `google-cloud-storage` SDK primitives inside `src/gcs/{backend,control,write}.rs`, including:
- `Backend::read_range_grpc` (`open_object().send_and_read(range)` returning `(ObjectDescriptor, ReadObjectResponse)` in 1 RPC).
- `Backend::upload_bytes` (`write_object().send_unbuffered()` returning `google_cloud_storage::model::Object`).
- `CompositeUploadGuard` and `Backend::compose_objects` (currently coupled to local `std::fs::File` slices via `FileSliceSource` in `put_file`).
- `Backend::list_objects` (using `StorageControl::list_objects().by_page()`, currently draining all pages into a single `Vec<Object>`).
- `CachedFs` (`src/cached.rs`), which attaches `ReadaheadFile` and the generation-keyed `PageCache` (`256 KiB` pages) exclusively inside `CachedFs::open`, leaving `cat_file` and `cat_ranges` uncached.

Meanwhile, [`ObjectStore`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/src/lib.rs#L921-L1318) callers issue stateless requests (`get_opts`, `get_range`, `get_ranges`, `put_opts`, `put_multipart_opts`, `list`, `list_with_offset`, `list_with_delimiter`) with no persistent `File` handle across calls.

## Goals / Non-Goals

**Goals:**
- Expose 5 non-breaking methods on `gcs_rust_fs::FileSystem` (`open_stream`, `read_ranges`, `put_bytes`, `open_multipart`, `list_page`) plus supporting option/trait types (`RangeBoundsMode`, `StreamRead`, `WriteMode::IfGenerationMatch`, `MultipartWriter`, `ListPageOptions`, `ListPage`) so an `ObjectStore` adapter maps all major Read, Write, and List flows with zero extra round-trips or whole-object/whole-bucket memory buffering.
- Ensure stateless `read_ranges` calls benefit from both Mode 2 `PageCache` in `CachedFs` and open gRPC `BidiReadObject` (`ObjectDescriptor`) stream reuse in `GcsFs`.
- Preserve existing `fsspec` / Python `gcsfs` default semantics (`RangeBoundsMode::ClampEmpty`, `ls` / `find` / `pipe_file` signatures) without breaking existing callers.

**Non-Goals:**
- Implementing `impl ObjectStore` inside this change (this design specifies the `gcs-rust-fs` crate primitives consumed by the adapter).
- Adding Phase 2 `ObjectStore` edge cases (`GetOptions` ETag/date headers, `Cache-Control`/`Content-Encoding`/`Storage-Class` write headers, `CopyMode::Create`, non-ADC auth builders, V4 Signed URLs, or `MultipartStore`), or modifying `RemoteFs` gRPC sidecar protobufs in MVP (`RemoteFs` inherits default trait fallbacks or returns `Unsupported` for new methods until Phase 2).

## Decisions

### 1. Fixed Trait & Option Contracts in `gcs-rust-fs`

We add five methods to `trait FileSystem` (with default fallback implementations where feasible for mock/memory filesystems, overridden natively by `GcsFs` and `CachedFs`):

```rust
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RangeBoundsMode {
    #[default]
    ClampEmpty,
    Strict,
}

// Extend ReadOptions (`#[non_exhaustive]`):
//   pub bounds_mode: RangeBoundsMode,
//   pub pattern: ReadPattern,

pub type ByteStream = futures_util::stream::BoxStream<'static, Result<Bytes>>;

pub struct StreamRead {
    pub stat: ObjectStat,
    pub range: std::ops::Range<u64>,
    pub stream: ByteStream,
}

// Extend WriteMode (`#[non_exhaustive]`):
//   WriteMode::IfGenerationMatch(i64)

#[async_trait]
pub trait MultipartWriter: Send + Sync + std::fmt::Debug {
    async fn write_part(&self, part_idx: usize, data: Bytes) -> Result<()>;
    async fn complete(&mut self) -> Result<ObjectStat>;
    async fn abort(&mut self) -> Result<()>;
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListPageOptions {
    pub delimiter: bool,
    pub start_after: Option<String>,
    pub page_size: Option<usize>,
    pub page_token: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ListPage {
    pub entries: Vec<Entry>,
    pub prefixes: Vec<String>,
    pub next_page_token: Option<String>,
}

// On `trait FileSystem`:
async fn open_stream(&self, path: &str, range: Option<ByteRange>, opts: ReadOptions) -> Result<StreamRead>;
async fn read_ranges(&self, path: &str, ranges: &[std::ops::Range<u64>], opts: ReadOptions) -> Result<Vec<Bytes>>;
async fn put_bytes(&self, path: &str, data: Bytes, opts: WriteOptions) -> Result<ObjectStat>;
async fn open_multipart(&self, path: &str, opts: WriteOptions) -> Result<Box<dyn MultipartWriter>>;
async fn list_page(&self, path: &str, opts: ListPageOptions) -> Result<ListPage>;
```

### 2. Stateless Range Reads via `PageCache` + `DescriptorPool` (Instead of Ephemeral `open()` Handles)

```text
  CachedFs::read_ranges(path, &[r0..rN], Strict)
       |
       +--> 1. Resolve (bucket, key, generation, size) via #gen, DirCache, or DescriptorPool/stat
       +--> 2. Validate strict bounds (`r.start < r.end`, `r.start < size`)
       +--> 3. Slice 256 KiB hits from PageCache; coalesce misses (gap <= 256 KiB, max 32 MiB)
       |
       v
  GcsFs::Backend::DescriptorPool (LRU, C_desc_max = 64, T_desc_idle = 30s)
       |
       +--> Acquire/open gRPC ObjectDescriptor -> concurrent `descriptor.read_range(...)`
       +--> Populate PageCache with fetched 256 KiB pages & return caller slices
```

- **Rationale**: Opening a temporary `ReadaheadFile` in Mode 1 (`readahead_only`) on each `ObjectStore::get_range` call triggers speculative 8–32 MiB folio downloads that are immediately discarded when the handle drops at the end of `get_range`, causing severe read amplification. Conversely, calling `GcsFs::cat_ranges` bypasses `CachedFs`'s `PageCache` and opens a fresh `BidiReadObject` stream per call.
- **Approach**:
  1. Inside `Backend` (`src/gcs/backend.rs`), add an LRU `DescriptorPool` keyed by `ObjectKey { bucket, name, generation }` holding at most `C_desc_max (64)` `ObjectDescriptor` instances (which are `Clone` handles over an active `BidiReadObject` stream in `google-cloud-storage`), evicting entries idle longer than `T_desc_idle (30) seconds`.
  2. In `CachedFs::read_ranges`, resolve `(generation, size)` once (reusing `#generation`, a non-expired `DirCache` entry, or an active `DescriptorPool` entry's `descriptor.object()` before falling back to `info()`), check `PageCache` on `C_page_size (256 KiB)` boundaries, coalesce missing spans within `C_coalesce_gap (256 KiB)` up to `max_folio_size (32 MiB)`, fetch misses concurrently via `inner.read_ranges("#generation")` (which multiplexes over the pooled `ObjectDescriptor` on gRPC or the 16-client striped HTTP pool on HTTP), populate `PageCache`, and assemble `Vec<Bytes>`.
- **Alternatives considered**: Keeping an LRU cache of `Box<dyn File>` (`ReadaheadFile`) handles inside the external `ObjectStore` adapter. Rejected because `ReadaheadFile` maintains per-handle sequential cursor state (`tell()`), whereas `DescriptorPool` + `PageCache` inside `gcs-rust-fs` benefits all concurrent callers (including `cat_ranges` if desired) without cursor contention.

### 3. Single-RTT `open_stream` with Threshold-Gated Readahead

- **Approach**: On `Transport::Grpc`, `GcsFs::open_stream` uses `open_object().send_and_read(resolved_range)` to retrieve `ObjectDescriptor` (`ObjectStat`) and the `ReadObjectResponse` stream in **1 RPC** (except when `ByteRange::Suffix` requires knowing `size` upfront on SDK versions that do not accept negative read offsets directly, in which case `get_object` resolves `size` and pins `generation`). `GcsFs` inserts a clone of `ObjectDescriptor` into `DescriptorPool` and wraps `Reader::next_chunk` via `futures_util::stream::try_unfold`. In `CachedFs::open_stream`, ranges `>= io_size (8 MiB)` with `ReadPattern::Sequential | Auto` stream through `ReadaheadFile` (`8–32 MiB` background folios); smaller ranges bypass `ReadaheadFile` to avoid prefetch waste.
- **Strict bounds**: Under `RangeBoundsMode::Strict`, `start >= end` fails immediately with `ErrorKind::InvalidRange`, and `start >= stat.size` (or HTTP 416 / gRPC `OUT_OF_RANGE` from `send_and_read`) propagates `ErrorKind::OutOfRange` instead of `GcsFs::cat_file_impl`'s empty-`Bytes` clamp.

### 4. In-Memory Parallel `MultipartWriter` via `ComposeObject` Tree + Zonal Reorder Stream

- **Rationale**: `ObjectStore` uploaders (`BufWriter`, `WriteMultipart`) invoke `MultipartUpload::put_part` synchronously and poll up to 8 returned `UploadPart` futures concurrently. Funneling them into `GcsFile::write(&mut self)` serializes uploads onto a single HTTP/gRPC stream.
- **Approach**:
  - **Flat & Hierarchical buckets**: Reuse `CompositeUploadGuard` and `Backend::compose_objects` from `src/gcs/write.rs`. `open_multipart` generates staging prefix `<object>.gstmp.<pid>.<nanos>.`. Each `write_part(part_idx, data)` uploads `<prefix><part_idx>` via `Storage::write_object(...).send_unbuffered()` concurrently without holding a lock across network I/O, storing `(part_idx, Object)` in a `Mutex<BTreeMap<usize, Object>>`. On `complete()`, `0` parts uploads an empty object; `1..=32` parts runs 1 `compose_objects` call (`delete_source_objects = true`) with destination `WriteMode`; `33..=1024` parts runs 1 round of parallel 32-way intermediate `compose_objects` calls into `<prefix>lvl1.<group>` followed by the final `compose_objects` call with destination `WriteOptions` (`WriteMode::Create` / `IfGenerationMatch`).
  - **Zonal buckets (`BucketKind::Zonal`)**: Because Zonal Rapid Storage lacks `ComposeObject`, `ZonalMultipartWriter` holds an `AppendableObjectWriter` plus a `BTreeMap<usize, Bytes>` reorder buffer (`next_idx: usize`). Concurrent `write_part` calls insert into the map and drain any contiguous run starting at `next_idx` into `AppendableObjectWriter::write`, finalizing (per builder config) on `complete()`.
- **Alternatives considered**: Implementing S3-compatible XML Multipart Uploads (`?uploads` / `?uploadId`) in `gcs-rust-fs`. Rejected because it requires bypassing `google-cloud-storage` with raw XML/HTTP signing and lacks atomic `if_generation_match` preconditions on completion.

### 5. Single-Page `list_page` with `\0`-Suffixed `lexicographic_start`

- **Approach**: `GcsFs::list_page` builds `StorageControl::list_objects()` with `.set_page_size(opts.page_size.unwrap_or(1000))` and awaits **only one step** of `builder.by_page().next().await`, returning `page.next_page_token` (as `Option<String>` when non-empty). Because Cloud Storage's `lexicographic_start` parameter is **inclusive** while `ListPageOptions::start_after` (`ObjectStore::list_with_offset`) is **exclusive**, `GcsFs` passes `format!("{after}\0")` (the smallest UTF-8 string strictly greater than `after`) to `.set_lexicographic_start(...)`. Missing prefixes in an existing bucket return `Ok(ListPage::default())`; only a missing bucket returns `ErrorKind::NotFound`.

## Risks / Trade-offs

- **[Orphaned `.gstmp.` objects on `SIGKILL` crash]** → `CompositeUploadGuard` deletes staged parts on `abort()`, upload error, and `Drop`, and documentation recommends a 1-day GCS bucket lifecycle rule on `.gstmp.` objects for ungraceful process termination (matching `object_store::gcp`'s multipart lifecycle guidance).
- **[Stale `DescriptorPool` streams after idle network drops]** → Evict entries after `T_desc_idle = 30s`, cap pool size at `C_desc_max = 64`, and retry once on a fresh generation-pinned `ObjectDescriptor` if `read_range` fails before yielding bytes.
- **[Unbounded memory if caller uploads parts `[1..100]` without ever uploading part `0` on Zonal buckets]** → Document that callers (`WriteMultipart` / `BufWriter`) bound concurrent in-flight `write_part` calls (typically `<=8`), and reject `part_idx` gaps exceeding `64` in `ZonalMultipartWriter` with `ErrorKind::InvalidConfig` / backpressure.
- **[Multi-writer `DirCache` staleness when used behind `ObjectStore`]** → Document that `ObjectStore` adapters wrapping `GcsFs` in `CachedFs` MUST set `CacheConfig::negative = false` and leave directory TTL disabled (or explicitly short) while keeping Mode 2 `PageCache` enabled (which is immutable-generation safe).

## Migration Plan

All changes in `gcs-rust-fs` are additive minor-version extensions (`0.1.x` -> `0.2.0` or `0.1.4`):
1. Land option types (`RangeBoundsMode`, `WriteMode::IfGenerationMatch`, `ListPageOptions`, `ListPage`, `StreamRead`, `MultipartWriter`) and `FileSystem` trait methods (with default fallbacks returning `ErrorKind::Unsupported` where appropriate so external `FileSystem` mocks keep compiling).
2. Implement `put_bytes` (refactoring `pipe_file` to delegate to `put_bytes`), `list_page`, `DescriptorPool` + `open_stream` + `read_ranges`, and `open_multipart` in `GcsFs` and `CachedFs`.
3. Validate with unit/mock tests and live/emulator integration tests before wiring the downstream `ObjectStore` adapter.
