# Pull Request Review

Use this checklist before opening or merging a pull request in the downstream
fork. CodeRabbit or another automated reviewer may supplement this process,
but cannot replace human review.

## Scope and intent

- Confirm the change has a clear consumer or maintenance rationale.
- Keep the patch focused and identify unrelated changes before review.
- State whether the change is additive, behavior-changing, deprecated, or
  breaking.
- For upstream-derived changes, record the source commit, issue, or pull
  request and preserve intentional downstream divergence.

## Public API and integration

- Check that every advertised public type, method, trait, and re-export is
  reachable through the intended public path.
- Test the public builder or store-construction path, not only crate-private
  helpers.
- Check default behavior as well as explicitly enabled behavior.
- Review feature combinations, custom connectors, provider builders, and WASM
  or platform-specific paths affected by the change.
- Check semver impact, trait bounds, visibility, derives, and rustdoc links.

## Correctness and security

- Compare documentation guarantees with the actual ownership and lifetime of
  data in errors, responses, logs, and debug output.
- Review byte limits, UTF-8 boundaries, untrusted input, credentials, signed
  URLs, provider response bodies, and request identifiers.
- Add regression tests for boundary values, multibyte input, failure paths,
  retries, and absence of opt-in behavior where relevant.
- Verify that errors do not expose sensitive URLs, headers, bodies, or
  credentials beyond the documented contract.

## Verification and handoff

- Run `cargo fmt --all -- --check`.
- Run warnings-denied Clippy for the affected targets.
- Run relevant unit, integration, feature-matrix, and doctests; run the full
  suite for public API or retry/transport changes.
- Review the final diff from the target branch and inspect generated or
  documentation changes.
- Ensure the PR description contains rationale, user-facing changes,
  compatibility status, linked issue status, and honest verification results.
- Record unresolved review findings explicitly; do not merge with an
  unreviewed assumption.
