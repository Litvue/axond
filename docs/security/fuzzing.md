# Parser fuzzing (retired)

The Rust fuzz workspace, replay lane and scheduled fuzz workflow were removed
with the Rust gateway under [ADR 0069](../adr/0069-retire-the-rust-gateway.md).
[ADR 0052](../adr/0052-fuzzing-the-untrusted-input-parsers.md) records the historical
decision. The old targets and seeds remain in git history and v0.7.x tags.

There is no current coverage-guided fuzzing lane. TypeScript parser regression
tests and property tests run in the workspace suite, including arbitrary SSE
chunk splitting. They are not a replacement claim for the removed harness.
Selecting TS fuzz equivalents remains an open question.

Parser changes still need regression tests for hostile input, bounded resource
use and secret-free errors. Security findings follow [SECURITY.md](../../SECURITY.md).
