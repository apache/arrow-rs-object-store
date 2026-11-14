# AGENTS.md (Kiro Steering)

This repository uses Kiro steering documents as the source of truth for agent
context. It is the servicecute downstream fork of
`apache/arrow-rs-object-store`.

## Steering Index (with inclusion)

1. `.kiro/steering/ai-assisted-development.md` — `inclusion: always` — Human
   accountability, AI-assisted development, upstream contribution, security,
   and downstream fork maintenance policy.
2. `.kiro/steering/upstream-sync.md` — `inclusion: always` — Upstream review,
   rebase, backport, conflict-resolution, and verification rules.
3. `.kiro/steering/pr-review.md` — `inclusion: manual` — Pull-request scope,
   public-API, integration, security, documentation, and verification review
   gates.

## Verification

- Rust formatting: `cargo fmt --all -- --check`.
- Rust linting: run warnings-denied Clippy for the affected targets.
- Rust tests: run the relevant unit, integration, and documentation tests.
- Preserve Apache license headers and existing public API conventions.
- Keep downstream changes focused, document intentional divergence, and pin
  consumer dependency and fork revisions.
- Review `UPSTREAM-TRACKING.md` and the linked GitHub watchlist before rebasing
  or releasing the fork.
- Read `.kiro/steering/pr-review.md` before opening or merging a pull request.
