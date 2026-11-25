# Tasks

## 1. Cargo Feature Setup, Store Core, and Builder

- [x] 1.1 Add optional `gcs-rust-fs = { version = "0.1.3", optional = true }` and feature `gcs-rust-fs = ["dep:gcs-rust-fs", "tokio"]` to `Cargo.toml`, and declare `#[cfg(all(feature = "gcs-rust-fs", not(target_arch = "wasm32")))] pub mod gcs_rust_fs;` in `src/lib.rs` with a minimal module skeleton, verifying both `cargo check` (default features) and `cargo check --features gcs-rust-fs` succeed
- [x] 1.2 Implement `GcsRustFs`, `GcsRustFsBuilder` (bucket validation, `Transport`, `endpoint`, `project`, `from_env`, default Mode 2 `CachedFs` `PageCache`, and custom `with_filesystem` injection), path/metadata/error conversion helpers (including unversioned `#<i64>` suffix rejection and directory-to-`NotFound` mapping), and module rustdoc, verifying unit tests for builder/path/error mapping and `cargo doc --features gcs-rust-fs --no-deps` pass with zero warnings

## 2. Read, Write, and List ObjectStore Implementation

- [x] 2.1 Implement `ObjectStore` Read operations (`get_opts`, `head`, `get_ranges`) over `FileSystem::open` (`ReadaheadFile`, `GetRange::as_range` strict bounds, `GetOptions::version`, and `ReadPattern::Random` concurrent `read_range`) together with an in-memory `gcs_rust_fs::FileSystem` test double (`MockMemoryFs`), verifying `cargo test --features gcs-rust-fs` passes read unit tests covering full/bounded/offset/suffix/versioned reads, out-of-bounds `InvalidRange`, directory `NotFound`, and `CachedFs` `PageCache` hits
- [x] 2.2 Implement `ObjectStore` Write, Multipart, Delete, Copy, and Rename operations (`put_opts`, ordered `GcsRustFsMultipartUpload` over `open(Write)`, `delete_stream`, `copy_opts`, `rename_opts`), verifying `cargo test --features gcs-rust-fs` passes write unit tests covering `PutMode::{Overwrite, Create, Update}`, concurrent out-of-order `put_part`, pre-completion `NotFound` isolation, `abort()`, `delete_stream`, `copy`, and `rename`
- [x] 2.3 Implement `ObjectStore` List operations (`list`, `list_with_offset`, `list_with_delimiter`) over `find` and `ls` with strict directory-prefix child filtering and non-root `NotFound` empty-result handling, verifying `cargo test --features gcs-rust-fs` passes list unit tests covering root listing, file-path listing (`[]`), sibling-prefix exclusion, exclusive `offset` ordering, and `/` delimiter `common_prefixes`

## 3. End-to-End Read/Write/List Integration Suite & Security Verification

- [x] 3.1 Add integration test suite runners exercising `src/integration.rs` (`put_get_delete_list`, `list_uses_directories_correctly`, `list_with_delimiter`, `stream_get`, `put_opts(..., false)`, `put_get_attributes`) against both offline `CachedFs<MockMemoryFs>` and env-gated live `GcsRustFsBuilder::from_env()` (`GCS_RUST_FS_TEST_BUCKET` / `GOOGLE_BUCKET`), and audit all newly added files for path/bucket validation and absence of hardcoded secrets, verifying `cargo test --features gcs-rust-fs,integration` and `cargo test` (default features) pass cleanly
