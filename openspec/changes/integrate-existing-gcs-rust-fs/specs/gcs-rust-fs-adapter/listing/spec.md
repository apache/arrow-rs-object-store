# Spec Delta

## Purpose

Defines observable `ObjectStore` recursive prefix listing, exclusive offset listing, and `/` delimiter listing semantics for `object_store::gcs_rust_fs` over `gcs-rust-fs` `0.1.3`.

## ADDED Requirements

### Requirement: Strict directory-prefix recursive listing
`GcsRustFs` SHALL implement `ObjectStore::list` over `gcs-rust-fs` `find` (`withdirs = false`) such that `None` or root `/` lists all objects in the bucket and `Some(prefix)` yields only objects strictly beneath `"{prefix}/"`, never echoing an object whose key equals `prefix` itself.

#### Scenario: Listing an existing file path or missing prefix returns empty stream
- **WHEN** `foo/x.json` and `foo.bar/y.json` exist and a caller invokes `list(Some("foo/x.json"))` or `list(Some("missing"))`
- **THEN** `list` yields zero items without returning an error

#### Scenario: Listing a directory prefix excludes sibling prefixes sharing string prefix
- **WHEN** `foo/x.json` and `foo.bar/y.json` exist and a caller invokes `list(Some("foo"))`
- **THEN** `list` yields only `foo/x.json` and excludes `foo.bar/y.json`

### Requirement: Exclusive offset filtering in list_with_offset
`GcsRustFs` SHALL implement `ObjectStore::list_with_offset` by yielding only objects matching `prefix` whose `ObjectMeta::location` is strictly greater than `offset` (`location > offset`).

#### Scenario: Keys less than or equal to offset are excluded
- **WHEN** a bucket contains keys before, equal to, and after `offset` under `prefix`
- **THEN** `list_with_offset(prefix, offset)` returns only keys matching `prefix` with `location > offset`

### Requirement: Delimiter-grouped shallow listing
`GcsRustFs` SHALL implement `ObjectStore::list_with_delimiter` over `gcs-rust-fs` `ls` (`refresh = true`), returning direct child files in `ListResult::objects`, immediate child directories in `ListResult::common_prefixes`, and an empty `ListResult` when a non-root prefix does not exist in an existing bucket.

#### Scenario: Delimited listing separates direct objects and immediate child prefixes
- **WHEN** `mydb/wb/foo.json`, `mydb/wb/000/000/000.segment`, and `mydb/wb/001/001/000.segment` exist and a caller invokes `list_with_delimiter(Some("mydb/wb"))`
- **THEN** `ListResult::objects` contains only `mydb/wb/foo.json` and `ListResult::common_prefixes` contains `mydb/wb/000` and `mydb/wb/001`

#### Scenario: Delimited listing on non-existent prefix or file path returns empty result
- **WHEN** a caller invokes `list_with_delimiter` on a non-existent prefix `"not_there"` or an existing file path `"foo/x.json"` inside an existing bucket
- **THEN** the call succeeds with empty `objects` and empty `common_prefixes`
