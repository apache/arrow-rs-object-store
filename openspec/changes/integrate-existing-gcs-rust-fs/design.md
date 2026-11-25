# Design

## Context

See [proposal.md](file:///usr/local/google/home/princer/code/arrow-rs-object-store/openspec/changes/integrate-existing-gcs-rust-fs/proposal.md) for motivation. We are integrating the published `gcs-rust-fs = "0.1.3"` crate directly into `arrow-rs-object-store` without modifying `gcs-rust-fs` itself.

Inspecting `gcs-rust-fs 0.1.3` (`GcsFs`, `CachedFs`, `ReadaheadFile`, `Loc`) alongside [`src/integration.rs`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/src/integration.rs) reveals four concrete semantic differences between `fsspec` (`gcs-rust-fs`) and [`ObjectStore`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/src/lib.rs#L921-L1318) that the adapter must bridge:
1. `CachedFs` only attaches `ReadaheadFile` and Mode 2 `PageCache` (`256 KiB` pages) on `FileSystem::open(Read)`; `cat_file` and `cat_ranges` bypass `CachedFs` and clamp `start >= size` to empty `Bytes` instead of returning `Error::InvalidRange`.
2. `GcsFs::info("bucket/dir")` returns `EntryKind::Directory` and `GcsFs::open("bucket/dir")` returns `ErrorKind::IsADirectory`, whereas `ObjectStore::head("dir")` and `get("dir")` must return `Error::NotFound`.
3. `GcsFs::ls` and `GcsFs::find` on a file path `"bucket/foo/x.json"` return `[Entry::file("bucket/foo/x.json")]`, and `GcsFs::ls("bucket/missing_prefix")` returns `ErrorKind::NotFound`, whereas `ObjectStore::list` and `list_with_delimiter` treat prefixes strictly as directory prefixes and return empty results for file paths or missing prefixes inside an existing bucket.
4. `GcsFile` write handles (`open(Write)`) take sequential `&mut self` `write(Bytes)` calls, whereas `MultipartUpload::put_part` spawns concurrent futures that may resolve out of order.

## Goals / Non-Goals

**Goals:**
- Provide `object_store::gcs_rust_fs::{GcsRustFs, GcsRustFsBuilder}` behind an optional `gcs-rust-fs` feature flag, backed by `Arc<dyn gcs_rust_fs::FileSystem>` (`CachedFs<GcsFs>` by default).
- Engage `CachedFs` Mode 2 shared `PageCache` (`256 KiB` pages) and async folio readahead (`8–64 MiB`) on `ObjectStore` read and range-read paths today.
- Pass `object_store`'s core Read, Write, Multipart, Copy/Rename, Delete, and List test suites (`put_get_delete_list`, `list_uses_directories_correctly`, `list_with_delimiter`, `stream_get`, plus `PageCache` hit verification) both offline (against `CachedFs<InMemoryFs>`) and against live GCS when configured via environment variables.

**Non-Goals:**
- Replacing or altering [`src/gcp`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/src/gcp/mod.rs) (`GoogleCloudStorage`).
- Supporting `ObjectStore` features absent from `gcs-rust-fs 0.1.3` (`GetOptions` HTTP ETag/date conditionals, `PutMode::Update`, `CopyMode::Create`, `MultipartStore` cross-process upload IDs, `Signer`, or non-ADC credential providers).

## Decisions

### 1. Isolated Feature Flag and `Arc<dyn gcs_rust_fs::FileSystem>` Store Core

- **Choice**: Add optional `gcs-rust-fs = { version = "0.1.3", optional = true }` and feature `gcs-rust-fs = ["dep:gcs-rust-fs", "tokio"]` in [Cargo.toml](file:///usr/local/google/home/princer/code/arrow-rs-object-store/Cargo.toml), gating `pub mod gcs_rust_fs` with `#[cfg(all(feature = "gcs-rust-fs", not(target_arch = "wasm32")))]`. `GcsRustFs` holds `bucket: String` and `fs: Arc<dyn gcs_rust_fs::FileSystem>`.
- **Default caching**: `GcsRustFsBuilder::build()` constructs `GcsFs` via `GcsFsBuilder` and wraps it in `CachedFs::new(gcs_fs, CacheConfig { ttl: None, max_dirs: Some(1024), negative: false, populate_from_find: false, data: Some(DataConfig::page_cache(512 * 1024 * 1024)) })` unless the caller customizes `CacheConfig` or disables caching. Setting `negative: false` prevents cached parent listings from hiding objects created by external writers, while `DataConfig::page_cache` enables generation-safe `256 KiB` page caching and readahead.
- **Alternatives considered**: Putting the adapter inside `src/gcp` behind a feature flag. Rejected because `gcs-rust-fs 0.1.3` requires `rustc >= 1.91`, tonic/gRPC, and async builder initialization (`GcsFsBuilder::build().await`), whereas `GoogleCloudStorageBuilder::build()` is synchronous and `object_store`'s workspace MSRV is `1.85`.

### 2. Read Pipeline via `FileSystem::open` (`ReadaheadFile`) Instead of `cat_file`

```text
  get_opts(location, opts)                    get_ranges(location, ranges)
           |                                               |
           v                                               v
  fs.open(path, Read + Auto)                  fs.open(path, Read + Random)
  [1 open -> ReadaheadFile + ObjectStat]      [1 open -> Arc<dyn File> + ObjectStat]
           |                                               |
           +--> Validate via GetRange::as_range(stat.size) -> Error::InvalidRange
           |                                               |
           v                                               v
  file.seek(Start(r.start))                   try_join_all(ranges.map(|r|
  + stream::try_unfold(file.read(chunk))        file.read_range(ByteRange::from(r))))
```

- **Rationale**: In `gcs-rust-fs 0.1.3`, `CachedFs::open` wraps the `GcsFile` reader in `ReadaheadFile` connected to `Index::Shared` (`PageCache`) and already exposes `file.stat() -> Option<&ObjectStat>`.
- **`get_opts`**: Reject unsupported HTTP preconditions (`if_match`, `if_none_match`, `if_modified_since`, `if_unmodified_since`) with `Error::NotSupported`. If `opts.head` is `true` and `opts.range` is `None`, delegate to `head_stat` (`fs.info`) and return an empty stream. Otherwise open `full_path` (appending `#{version}` when `opts.version` is set) with `OpenOptions::read()`, extract `ObjectMeta` and `Attributes` from `file.stat()` (falling back to `fs.info` only if a custom `FileSystem` returns `None` from `stat()`), resolve `opts.range` via [`GetRange::as_range`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/src/util.rs#L238-L265), `seek(SeekFrom::Start(range.start))`, and stream `1 MiB` slices via `futures_util::stream::try_unfold`.
- **`get_ranges`**: Handle `ranges.is_empty()` by returning `Ok(Vec::new())`. Otherwise open a single handle with `OpenOptions::read().with_read_pattern(ReadPattern::Random)` (avoiding speculative 64 MiB sequential ramp), validate every range via `GetRange::Bounded(r).as_range(size)?`, wrap `Box<dyn File>` in `Arc`, and execute `file.read_range(ByteRange::from(clamped))` concurrently with `try_join_all`. In Mode 2 `CachedFs`, `ReadaheadFile::read_range` populates and serves `256 KiB` pages from the shared `PageCache`.

### 3. Write & Ordered Multipart Pipeline

- **`put_opts`**: Validate attributes (mapping `Attribute::ContentType` -> `WriteOptions::content_type` and `Attribute::Metadata(k)` -> `WriteOptions::metadata`, returning `Error::NotImplemented` for unsupported HTTP headers like `Cache-Control` / `Content-Encoding` so [`put_get_attributes`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/src/integration.rs#L458-L499) handles it gracefully). Map `PutMode::Overwrite` -> `WriteMode::Overwrite`, `PutMode::Create` -> `WriteMode::Create`, and `PutMode::Update(_)` -> `Err(Error::NotImplemented)`. Upload `Bytes::from(payload)` via `fs.pipe_file`, then call `fs.info(&full_path)` (with `invalidate_cache` already handled by `CachedFs::pipe_file`) to return `PutResult { e_tag, version }`.
- **`put_multipart_opts`**: Return `GcsRustFsMultipartUpload` holding an `Arc<tokio::sync::Mutex<MultipartState>>` and an atomic/local `part_idx` counter.
  - Because some `FileSystem` backends (such as Zonal appendable objects or simple mock filesystems) create a visible 0-byte entry immediately on `open(Write)`, while [`stream_get`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/src/integration.rs#L720-L733) requires `head(location)` and `list(None)` to NOT see the object before `complete()`, `MultipartState` buffers parts or opens the underlying `Box<dyn File>` on `complete()` when total buffered bytes are small, OR opens `GcsFile` (`OpenOptions::write()`) lazily on `complete()` / Flat-HNS resumable session!
  - Even simpler and cleaner: on Flat/HNS `GcsFs`, `open(Write)` starts an unpublished resumable upload, whereas on Zonal/mock filesystems `open(Write)` may publish immediately. If `GcsRustFsMultipartUpload` opens the `GcsFs` write stream lazily on `GcsFs` (Flat/HNS) when contiguous parts arrive *only* when known unpublished, OR streams through `open(Write)` on `GcsFs` (Flat/HNS) while keeping the mock `FileSystem` in tests faithful to GCS's unpublished-until-`close()` contract, both memory bounds AND pre-completion invisibility hold! Specifically, in our offline `MockMemoryFs`, `open(Write)` stages writes in a buffer and publishes to the map only on `File::close()` (matching real GCS Flat/HNS resumable uploads), while `GcsRustFsMultipartUpload` opens `fs.open(path, open_opts)` on the first contiguous write (so an immediate `abort()` with 0 parts does 0 RPCs, and a 0-part `complete()` calls `pipe_file(Bytes::new())`) and drains contiguous `BTreeMap<usize, PutPayload>` entries into `file.write(chunk).await` as they arrive!

### 4. Prefix-Safe Listing Over `find` and `ls`

- **Path translation**: Given `bucket` and `prefix: Option<&Path>`:
  - `None` or `Some(p)` where `p.as_ref().is_empty()` -> `target = bucket.clone()`, `key_prefix = ""`.
  - Non-empty `Some(p)` -> `target = format!("{bucket}/{}", p.as_ref())`, `key_prefix = format!("{}/", p.as_ref())`.
- **`list`**: Stream lazily via `futures_util::stream::once` calling `fs.find(&target, FindOptions { withdirs: false, ..Default::default() })`. Keep only `entry.is_file()` whose bucket-stripped key starts with `key_prefix` (when non-empty). Convert each `Entry` + `ObjectStat` to `ObjectMeta` via `Path::parse(key)`. If `prefix` is non-empty and `find` returns `ErrorKind::NotFound`, yield empty stream; if `prefix` is root and bucket is missing, propagate `Error::NotFound`.
- **`list_with_offset`**: Wrap `self.list(prefix)` with `.try_filter(move |meta| ready(meta.location > offset))`.
- **`list_with_delimiter`**: Call `fs.ls(&target, ListOptions { refresh: true, versions: false })` (bypassing `DirCache` so external writes and pre-completion multipart checks are always fresh). If `prefix` is non-empty and `ls` returns `ErrorKind::NotFound`, return `Ok(ListResult::default())`. Partition entries whose bucket-stripped key starts with `key_prefix`: `EntryKind::File` -> `ListResult::objects`, `EntryKind::Directory` -> `ListResult::common_prefixes` (`Path::parse(key)`).

### 5. Security, Input Validation, and Verification Plan

- **Authentication & Secrets**: Relies exclusively on `gcs-rust-fs` / `google-cloud-storage` Application Default Credentials (ADC) over TLS/gRPC; zero credentials, tokens, or fallback secrets are stored in code or logged in `Debug`/`Display` impls.
- **Path & Bucket Boundary Validation**:
  - `GcsRustFsBuilder::build` rejects empty bucket names or bucket names containing `/` or `#` with `object_store::Error::Generic` (`InvalidConfig`).
  - `object_store::path::Path` already enforces no leading/trailing `/`, no empty `//` segments, no `.` or `..` traversal segments, and no ASCII control characters.
  - Because `gcs-rust-fs` interprets a trailing `#<i64>` suffix as an object generation, our path mapper explicitly checks unversioned object keys for a trailing `#<i64>` suffix and returns `Error::NotSupported` rather than allowing unintended generation pinning.
- **Testing & Localhost Binding**: Offline tests run purely in-memory (`CachedFs<InMemoryFs>`) with no network listener; any optional local endpoint tests bind strictly to `127.0.0.1` (never `0.0.0.0`).

#### Verification Plan
- **Functional Read/Write/List Suite**: Run `cargo test --features gcs-rust-fs,integration` executing offline `put_get_delete_list`, `list_uses_directories_correctly`, `list_with_delimiter`, `stream_get`, conditional `put_opts(..., false)`, and `PageCache` hit-rate assertions, plus env-gated live GCS integration tests (`GCS_RUST_FS_TEST_BUCKET` / `GOOGLE_BUCKET`).
- **Automated Security Check & Audit**: Verify dependency/code safety (confirming SecureCoder scanner status if active and auditing all newly created Rust files for input validation, path boundary safety, error handling, and absence of secrets).

## Risks / Trade-offs

- **[`list` buffers `Vec<Entry>` in memory on huge buckets in `0.1.3`]** → Documented on `GcsRustFs`; bounded-memory `list_page` is already specified in [`gcs-rust-fs-object-store-primitives`](file:///usr/local/google/home/princer/code/arrow-rs-object-store/openspec/changes/gcs-rust-fs-object-store-primitives) for Phase 2.
- **[`put_opts` requires 2 RTTs (`pipe_file` + `info`) to populate `PutResult`]** → Acceptable for functional Read/Write/List workloads today; drops to 1 RTT once `gcs-rust-fs` exposes `put_bytes -> Result<ObjectStat>`.
- **[`rustc >= 1.91` required when `--features gcs-rust-fs` is enabled]** → Kept strictly opt-in behind `feature = "gcs-rust-fs"` so default `object_store` builds preserve `rust-version = "1.85"`.

## Migration Plan

1. Add optional `gcs-rust-fs` dependency and feature in [Cargo.toml](file:///usr/local/google/home/princer/code/arrow-rs-object-store/Cargo.toml) without changing default features.
2. Implement `src/gcs_rust_fs/{mod,builder,upload}.rs` and export behind `#[cfg(all(feature = "gcs-rust-fs", not(target_arch = "wasm32")))]`.
3. Verify offline and live Read/Write/List tests, `cargo check` (default features), and `cargo doc --features gcs-rust-fs --no-deps`. Rollback is trivial (disable feature or revert module).
