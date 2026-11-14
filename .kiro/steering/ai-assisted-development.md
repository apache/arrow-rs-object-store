# AI-Assisted Development

This repository is the servicecute downstream fork of
`apache/arrow-rs-object-store`. AI assistance is a development tool, not an
owner or maintainer.

## Human accountability

- Every change must have a human owner who understands the implementation,
  its security implications, and its maintenance cost.
- The owner must review the complete diff, run the relevant checks, and be able
  to explain the public API and behavioral changes without relying on the AI
  tool.
- AI-generated code must not be merged merely because it compiles or passes a
  narrow test.

## Changes in this fork

- Keep the downstream patch set small, focused, and easy to rebase onto the
  Apache upstream repository.
- Prefer upstream-compatible APIs and behavior. Do not introduce
  project-specific requirements into the generic object-store contract without
  a documented reason.
- Add regression tests for every behavioral change and run formatting, lint,
  unit, integration, and documentation checks appropriate to the scope.
- Do not include credentials, signed URLs, private provider responses, or
  customer data in prompts, source files, tests, logs, or error metadata.
- Record intentional divergence from upstream in the relevant documentation
  and keep dependency versions and fork revisions pinned for consumers.

## Upstream contributions

- Do not submit unattended or deceptively AI-authored pull requests to Apache
  Arrow or other upstream projects.
- Before proposing an upstream change, a human maintainer must rewrite or
  validate the description in their own words, understand every changed line,
  and commit to responding to review and maintaining the change.
- Upstream pull requests should be narrowly scoped, reference the motivating
  consumer or issue, describe verification honestly, and disclose AI
  assistance when required by the upstream project's policy.
- If upstream declines a change because of contribution policy or scope, keep
  it as a clearly documented downstream patch rather than repeatedly
  resubmitting automated variants.

## Security and release gates

- Treat provider authentication, request signing, retries, error handling, and
  data deletion as security-sensitive code.
- A human must review changes affecting those areas before release.
- Release or publish a fork revision only after the full applicable test suite,
  dependency audit, and diff review pass.
- Never use AI assistance to bypass repository permissions, upstream review,
  licensing requirements, or required security checks.
