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
   public-API, integration, security, documentation, and Rust-specific
   verification review gates supplementing the global PR workflow.

## Global Steering Index (with inclusion)

The shared Kiro steering catalog is pinned in `.kiro/global-steering/`.

1. `.kiro/global-steering/remote-steering-bootstrap.md` — `inclusion: always`.
2. `.kiro/global-steering/agent-conduct.md` — `inclusion: always`.
3. `.kiro/global-steering/code-style.md` — `inclusion: always`.
4. `.kiro/global-steering/documentations.md` — `inclusion: always`.
5. `.kiro/global-steering/file-naming-convention.md` — `inclusion: always`.
6. `.kiro/global-steering/git-branch-workflow.md` — `inclusion: always`.
7. `.kiro/global-steering/plain-language-summaries.md` — `inclusion: always`.
8. `.kiro/global-steering/post-implementation-validation.md` — `inclusion: always`.
9. `.kiro/global-steering/pull-request-workflow.md` — `inclusion: always` —
   Shared PR lifecycle, public-API review, bot feedback, and completion gates.
10. `.kiro/global-steering/rust-code-search-strategy.md` — `inclusion: conditional` —
    patterns: `**/*.rs`, `**/Cargo.toml`.
11. `.kiro/global-steering/semantic-naming-and-api-contracts.md` — `inclusion: always`.
12. `.kiro/global-steering/task-completion-rules.md` — `inclusion: always`.
13. `.kiro/global-steering/time-estimation.md` — `inclusion: always`.
14. `.kiro/global-steering/worktree-agent-workflow.md` — `inclusion: manual`.

Other shared steering documents remain available in the pinned submodule and
should be indexed here when their technology or workflow applies.

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
