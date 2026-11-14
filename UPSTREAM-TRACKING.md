# Upstream Tracking

GitHub Issues and Discussions are currently disabled for this fork. Until they
are enabled, this file is the canonical reviewed snapshot for the upstream
watchlist. When issue tracking is enabled, create an umbrella issue and move
live status there while retaining this file as an offline snapshot.

Last reviewed: 2026-11-14

Upstream: `apache/arrow-rs-object-store`

Fork: `servicecute/arrow-rs-object-store`

## High-priority compatibility watchlist

| Source | Area | Fork action |
| --- | --- | --- |
| [#878](https://github.com/apache/arrow-rs-object-store/issues/878) | Custom HTTP connectors during URL resolution | Re-check hook propagation and connector wrapping. |
| [#840](https://github.com/apache/arrow-rs-object-store/issues/840) | Custom retry policy | Re-check `RetryError` and metadata behavior. |
| [#901](https://github.com/apache/arrow-rs-object-store/issues/901) | Throttling error variants | Align provider error classification and metadata. |
| [#903](https://github.com/apache/arrow-rs-object-store/pull/903) | Conditional puts with custom headers | Review request/header API overlap. |
| [#899](https://github.com/apache/arrow-rs-object-store/pull/899) | Multipart put modes | Review retry and completion error paths. |
| [#847](https://github.com/apache/arrow-rs-object-store/pull/847) | `quick-xml` security/dependency update | Backport promptly when merged and validated. |

## Security and reliability watchlist

| Source | Area | Fork action |
| --- | --- | --- |
| [#877](https://github.com/apache/arrow-rs-object-store/issues/877) | Unsigned S3 list headers | Review signing and default-header behavior. |
| [#783](https://github.com/apache/arrow-rs-object-store/issues/783) | GCS OAuth and default headers | Check token-fetch isolation. |
| [#874](https://github.com/apache/arrow-rs-object-store/issues/874) | GCS upload CRC32C | Review upload integrity behavior. |
| [#875](https://github.com/apache/arrow-rs-object-store/issues/875) | Azure upload `Content-MD5` | Review upload integrity behavior. |
| [#889](https://github.com/apache/arrow-rs-object-store/issues/889) | Local staging-file cleanup | Review filesystem failure cleanup. |
| [#888](https://github.com/apache/arrow-rs-object-store/issues/888) | Local fsync durability | Review durability guarantees and tests. |
| [#885](https://github.com/apache/arrow-rs-object-store/issues/885) | Azure multipart retry | Review lost-success-response handling. |
| [#902](https://github.com/apache/arrow-rs-object-store/issues/902) | Short range response bodies | Review range validation and error mapping. |

## Downstream feature context

- Fork change: opt-in bounded HTTP error metadata, currently in PR [#1](https://github.com/servicecute/arrow-rs-object-store/pull/1).
- Related upstream proposal: [apache/arrow-rs-object-store#904](https://github.com/apache/arrow-rs-object-store/pull/904).
- Re-check this list before each upstream rebase and release.
