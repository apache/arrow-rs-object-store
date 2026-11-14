# Upstream Sync

This repository is a downstream fork of
`apache/arrow-rs-object-store`. Upstream synchronization must preserve the
fork's consumer-facing contracts while keeping downstream changes focused and
easy to rebase.

## Source of truth

- The Apache repository is the source of truth for upstream implementation,
  release, and security changes.
- GitHub Issues and pull requests are the source of truth for the live
  watchlist and work status.
- [`UPSTREAM-TRACKING.md`](../../UPSTREAM-TRACKING.md) is a reviewable snapshot
  of the current watchlist, not a replacement for GitHub status.

## Sync workflow

1. Fetch `upstream` and inspect the release notes, open issues, and open pull
   requests since the last reviewed revision.
2. Classify each change as required security fix, compatibility fix, provider
   behavior, API change, dependency/toolchain change, or unrelated upstream
   work.
3. Review changes touching HTTP connectors, retries, signing, credentials,
   response parsing, multipart operations, filesystem durability, or public
   traits before rebasing the fork.
4. Rebase or selectively backport upstream changes while preserving the
   downstream patch set. Do not merge unrelated upstream work into a focused
   downstream feature branch.
5. Resolve conflicts in favor of the upstream contract unless the fork has a
   documented consumer requirement. Record intentional divergence in the
   tracking snapshot and the relevant code documentation.
6. Run formatting, warnings-denied Clippy, the applicable feature-matrix
   checks, tests, doctests, and security/dependency audits.
7. Update the snapshot revision and consumer pins after the sync is reviewed.
   If an upstream-sync umbrella issue exists, update it as well; while issue
   tracking is disabled, use `UPSTREAM-TRACKING.md` as the canonical tracker.

## Review gates

- Treat public trait changes, feature changes, retry semantics, and error
  representation changes as API-risk changes.
- Re-check downstream hooks and wrappers whenever upstream changes connector
  construction or request execution.
- Re-check provider-specific adapters whenever upstream changes signing,
  headers, status mapping, checksums, or credential refresh behavior.
- Do not silently drop a downstream patch during conflict resolution.
- A human maintainer must approve the resulting diff before merging it into
  the fork's `main` branch.
