# Spec Delta

## Purpose

Defines observable requirements for single-round-trip byte uploads returning object metadata, generation-matched compare-and-swap writes, and concurrent in-memory multipart uploads across Cloud Storage bucket types in `gcs-rust-fs`.

## ADDED Requirements

### Requirement: Single-shot byte upload returning object metadata
The filesystem SHALL provide a `put_bytes` operation that atomically writes an in-memory `Bytes` payload to `path` with `WriteOptions` and returns the published object's `ObjectStat` (`etag`, `generation`, and `size`) without issuing a follow-up metadata lookup on flat or hierarchical buckets.

#### Scenario: Successful put_bytes returns published ObjectStat
- **WHEN** a caller uploads a `Bytes` payload via `put_bytes` with `WriteMode::Overwrite`
- **THEN** the filesystem publishes the object in one upload operation, invalidates affected `CachedFs` directory cache entries, and returns `ObjectStat` containing a non-empty `etag` and positive `generation`

### Requirement: Generation-matched compare-and-swap write mode
The filesystem SHALL support `WriteMode::IfGenerationMatch(i64)` on write operations, enforcing server-side generation matching so writes succeed only when the remote object's current generation equals the expected generation.

#### Scenario: Matching generation commits new version
- **WHEN** a caller writes to an existing object with `WriteMode::IfGenerationMatch(g)` where `g` matches the object's current generation
- **THEN** the write succeeds atomically and returns the new `ObjectStat` with an updated generation

#### Scenario: Mismatched generation fails with PreconditionFailed
- **WHEN** a caller writes to an object with `WriteMode::IfGenerationMatch(g)` where the object has been modified or deleted so its generation does not equal `g`
- **THEN** the write fails with `ErrorKind::PreconditionFailed` without overwriting the remote object

#### Scenario: Create-only write on existing object fails with AlreadyExists
- **WHEN** a caller writes to an existing object with `WriteMode::Create`
- **THEN** the write fails with `ErrorKind::AlreadyExists`

### Requirement: Concurrent in-memory multipart uploads on flat and hierarchical buckets
On flat and hierarchical buckets, the filesystem SHALL provide `open_multipart` returning a `MultipartWriter` whose `write_part(part_idx, data)` accepts concurrent out-of-order part uploads and whose `complete()` atomically composes all parts into the destination object.

#### Scenario: Concurrent out-of-order parts assemble in index order
- **WHEN** a caller uploads parts `0..N` concurrently in arbitrary completion order via `MultipartWriter::write_part` and invokes `complete()`
- **THEN** `complete()` publishes a single destination object whose byte content is the concatenation `part_0 || part_1 || ... || part_{N-1}`, deletes temporary part objects, and returns the destination `ObjectStat`

#### Scenario: Multipart uploads exceeding 32 parts use hierarchical composition
- **WHEN** a caller uploads between `33` and `1,024` parts via `MultipartWriter` on a flat or hierarchical bucket and invokes `complete()`
- **THEN** `complete()` reduces intermediate groups of up to `32` temporary objects via server-side composition, applies destination `WriteOptions` preconditions and metadata on the final compose, and cleans up all intermediate objects

#### Scenario: Aborted or dropped multipart upload cleans up temporary parts
- **WHEN** a caller invokes `MultipartWriter::abort()` or drops an uncompleted `MultipartWriter` after uploading one or more parts
- **THEN** the destination object is not created or modified and staged temporary part objects are scheduled for parallel deletion

### Requirement: Ordered multipart fallback on zonal buckets
On zonal Rapid Storage buckets where server-side composition is unavailable, `open_multipart` SHALL accept concurrent `write_part` calls, buffer out-of-order arrivals in memory, and append contiguous parts sequentially to an appendable object stream.

#### Scenario: Out-of-order parts on zonal bucket stream in contiguous order
- **WHEN** a caller invokes `open_multipart` on a `BucketKind::Zonal` bucket, calls `write_part(1, b)` before `write_part(0, a)`, and then calls `complete()`
- **THEN** the writer holds part `1` until part `0` arrives, appends `a` followed by `b` in order, and returns the resulting `ObjectStat` on `complete()`
