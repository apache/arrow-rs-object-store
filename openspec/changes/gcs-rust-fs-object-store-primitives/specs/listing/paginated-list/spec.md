# Spec Delta

## Purpose

Defines observable requirements for bounded-memory single-page object and prefix listing with continuation tokens, delimiter grouping, and server-side exclusive offset filtering in `gcs-rust-fs`.

## ADDED Requirements

### Requirement: Single-page bounded-memory object listing
The filesystem SHALL provide a `list_page` operation that fetches at most one service page of entries and common directory prefixes under a bucket path without materializing remaining pages in memory, returning `next_page_token` when further results exist.

#### Scenario: Multi-page prefix streams page by page
- **WHEN** a caller invokes `list_page` with `page_size = Some(100)` on a prefix containing `250` objects and iterates using each returned `next_page_token`
- **THEN** each call returns at most `100` entries per page with `Some(token)` until the final page returns `next_page_token = None`, and total yielded entries equal `250`

#### Scenario: Empty or non-existent prefix in existing bucket returns empty page
- **WHEN** a caller invokes `list_page` on a prefix that matches zero objects inside an existing bucket
- **THEN** the operation returns `Ok(ListPage { entries: [], prefixes: [], next_page_token: None })` rather than `ErrorKind::NotFound`

#### Scenario: Non-existent bucket fails with NotFound
- **WHEN** a caller invokes `list_page` targeting a bucket that does not exist
- **THEN** the operation returns an error with `ErrorKind::NotFound`

### Requirement: Delimiter-grouped non-recursive page listing
When `ListPageOptions::delimiter` is `true`, `list_page` SHALL group keys beneath immediate `/` child segments into `ListPage::prefixes` (including native folders on hierarchical and zonal buckets) and return only direct child objects in `ListPage::entries`.

#### Scenario: Delimited page separates immediate files and child prefixes
- **WHEN** a bucket contains `dir/a.parquet`, `dir/sub1/b.parquet`, and `dir/sub2/c.parquet` and a caller invokes `list_page("bucket/dir", ListPageOptions { delimiter: true, .. })`
- **THEN** `ListPage::entries` contains `bucket/dir/a.parquet` and `ListPage::prefixes` contains `bucket/dir/sub1/` and `bucket/dir/sub2/`

### Requirement: Exclusive server-side start_after offset pushdown
When `ListPageOptions::start_after` is provided, `list_page` SHALL push the lower-bound filter to Cloud Storage so only keys strictly greater than `start_after` in lexicographic UTF-8 byte order are returned.

#### Scenario: Exact start_after key is excluded from results
- **WHEN** a bucket contains `a/1.parquet`, `a/2.parquet`, and `a/3.parquet` and a caller invokes `list_page("bucket/a", ListPageOptions { start_after: Some("a/2.parquet".into()), .. })`
- **THEN** the returned `ListPage::entries` contains only `bucket/a/3.parquet` and excludes both `a/1.parquet` and `a/2.parquet`
