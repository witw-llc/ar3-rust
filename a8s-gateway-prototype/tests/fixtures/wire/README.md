# Synthetic A8S wire fixtures

These fixtures are design/test inputs, not messages to publish. All identities and URLs are synthetic. The example.invalid attachment URL deliberately cannot fetch a real object. No AWS credentials, real bucket names, or external writes are required.

manifest.json records expected S3 mailbox/key selection from the pinned Python source, plus the attachment payload hash. simple and conflicting_duplicate intentionally share an ID but differ in content. The latter must not overwrite or silently deduplicate the first.

The fixtures cover ordinary mail, namespace mail, attachment wire shape, two receipt stages, and an identity/content conflict. They are a starting corpus, not full conformance coverage.

All fixture IDs encode 2026-10-02T12:00:00Z with different synthetic randomness. JSON object key order/whitespace need not be identical across Python and Rust, but the canonical field values and the decoded content must match. Establish one canonical request/envelope digest function before implementing conflict detection; never mistake harmless JSON whitespace for a changed message.
