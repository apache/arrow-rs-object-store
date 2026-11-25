# Spec Delta

## Purpose

Defines observable `ObjectStore` write, concurrent multipart upload, deletion, copy, and rename semantics for the `object_store::gcs_rust_fs` adapter over `gcs-rust-fs` `0.1.3`.

## ADDED Requirements

### Requirement: Single-shot object writes with PutMode enforcement
`GcsRustFs` SHALL implement `ObjectStore::put_opts` for `PutMode::Overwrite` and `PutMode::Create` using `gcs-rust-fs`, returning `PutResult` populated with the written object's `e_tag` and generation `version`, and returning `Error::NotImplemented` for `PutMode::Update` on `0.1.3`.

#### Scenario: Create mode rejects overwriting an existing object
- **WHEN** an object already exists at `path` and a caller invokes `put_opts` with `PutMode::Create`
- **THEN** the write fails with `object_store::Error::AlreadyExists` and leaves the existing object intact

#### Scenario: Empty and non-empty overwrites round-trip bytes and metadata
- **WHEN** a caller invokes `put` with empty or non-empty `PutPayload`
- **THEN** `head` and `get` return the exact payload length and bytes along with `e_tag` and `version`

### Requirement: Concurrent ordered multipart uploads with pre-completion isolation
`GcsRustFs` SHALL implement `ObjectStore::put_multipart_opts` returning a `MultipartUpload` that accepts concurrent `put_part` futures, reorders out-of-order part completions into contiguous part-index order before writing to the underlying `gcs-rust-fs` stream, keeps the destination object invisible until `complete()`, and discards staged data on `abort()`.

#### Scenario: Concurrent multipart parts remain invisible until complete and assemble in order
- **WHEN** a caller initiates `put_multipart`, polls multiple `put_part` futures concurrently, checks `head` and `list` before `complete()`, and then awaits `complete()`
- **THEN** `head` returns `Error::NotFound` and `list` omits the path prior to `complete()`, and after `complete()` `get` returns the exact concatenation of parts in `put_part` call order

#### Scenario: Aborted multipart upload publishes no object
- **WHEN** a caller initiates `put_multipart`, uploads zero or more parts, and calls `abort()`
- **THEN** `get` and `head` on the destination path return `Error::NotFound`

### Requirement: Streamed deletion and server-side copy and rename
`GcsRustFs` SHALL implement `delete_stream`, `copy_opts` (`CopyMode::Overwrite`), and `rename_opts` (`RenameTargetMode::Overwrite`) by delegating to `rm_file`, `copy_file`, and `move_file` on `gcs-rust-fs`, returning `Error::NotImplemented` for create-only copy/rename modes unsupported by `0.1.3`.

#### Scenario: Copy and rename update object locations
- **WHEN** a caller copies `src` to `dst` and renames `dst` to `dst2`
- **THEN** `dst2` and `src` remain readable with the original payload while `head(dst)` returns `Error::NotFound`
