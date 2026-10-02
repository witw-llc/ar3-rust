# a8s gateway prototype (Rust)

Status: a runnable offline prototype, written in one morning on 2026-10-02.
It is reference code. It is not maintained, not deployed anywhere and not
release-ready: read [KNOWN-DEFECTS.md](KNOWN-DEFECTS.md) before you reuse any
part of it. The package version `0.0.0` is a placeholder.

It is an HTTP gateway for the [a8s](https://github.com/witw-llc/ar3) message
network: an agent with one token and an HTTP client sends and receives a8s
mail with no local install. This prototype speaks the a8s wire itself, in
Rust, over a SQLite custody store, with local filesystem doubles in place of
the remote and the storage service. Authentication is synthetic and for
loopback only.

AR3's own hosted server takes another shape: a thin front on an ordinary a8s
node, so that the Python router stays the only implementation of the wire.
This code is kept as the starting point for a later Rust port. The API design
it implements a subset of is in [docs/api-design.md](docs/api-design.md).

Local state uses prototype schema 2. Schema 1 state is rejected without
migration or deletion.

## Implemented subset

| Boundary | Present behavior |
|---|---|
| API | Axum/Tokio HTTP on a numeric loopback address; non-loopback binding fails |
| Identity | Fixed synthetic analyst, builder, and builder-reader profiles; tenant/seat come from the authenticated profile |
| Send | Immutable logical intent, server-generated ULID/date, durable acceptance, principal-scoped idempotency; duplicate attachment IDs/names rejected |
| Publication | Explicit selection of one operation/message; final bytes freeze before provider side effects; ambiguous outcomes stay blocked and retry the same key/bytes |
| Remote | `Remote` and `ObjectClient` traits; S3-shaped adapter exercised with local filesystem objects |
| Poll | Configured builder/team or analyst mailboxes; delimiter `/`, bounded pages, persistent continuation cursors, wrap for late arrivals |
| Custody | Message, consumed digest, per-consumer delivery, attachment bytes and stable receipt outbox commit in one transaction |
| Handling | Read does not handle; registered consumer claims use a generation, secret proof and recovery epoch; explicit handled/blocked/retry dispositions |
| Receipts | A8S version-2 custody/deferred receipts have stable persisted IDs; control evidence is not an agent task; no wire-level handled receipt |
| Attachments | Durable bounded staging, exact registered fake bearer references, message/owner authorization; no attachment LIST/delete or arbitrary HTTP GET |
| MQTT | Session/adapter contract double tests callback consumption and connection-generation ACK fencing; no broker client |
| Recovery | SQLite backup API; each restore creates a fresh epoch and hold; marker mismatch pauses handling/publication; evidence and incomplete attachments remain unclaimable |

Remote senders are `network_asserted`, not authenticated principals or human
approvals. Message content cannot alter grants or invoke tools. Independently
registered consumers get separate copies; sharing a consumer means competing
claims. This prototype fixes two consumers rather than exposing registration.

## Local commands

Run from this directory with Rust installed. The state and object paths are
explicit; neither `A8S_HOME` nor existing AR3 configuration is read.

```bash
cargo build --release --locked
mkdir -m 700 /tmp/ar3-local-demo
target/release/ar3-server-prototype init \
  /tmp/ar3-local-demo/state.sqlite 00000000000000000000000000
target/release/ar3-server-prototype serve \
  /tmp/ar3-local-demo/state.sqlite /tmp/ar3-local-demo/objects 127.0.0.1:8080
```

The zero cutoff is for the empty synthetic double only. It is not a recovery
policy for an existing network. Existing state/object directories must already
be private; the program refuses permissive directories rather than changing
shared filesystem permissions.

Synthetic authorization headers are `Bearer analyst-test`, `Bearer builder-test`
and `Bearer builder-reader-test`. They are public test identities, not production
credentials. Do not expose this listener through a proxy or tunnel.

```bash
curl --fail http://127.0.0.1:8080/v1/messages \
  -H 'Authorization: Bearer analyst-test' \
  -H 'Idempotency-Key: demo-send-1' -H 'Content-Type: application/json' \
  --data '{"recipient":"builder","content":"Synthetic message","attachment_ids":[]}'
```

The 202 response names the stable operation and message. Accepted means durable
outbound intent, not published, delivered, or handled.

## HTTP routes

Authentication is required except `GET /health/live`. Responses use no-store.
POST requests require `Idempotency-Key`. JSON DTOs reject unknown writable fields,
including request-supplied sender, tenant, bucket, paths, and provider endpoints.

| Route | Purpose |
|---|---|
| GET `/health/live`, `/health/ready` | Minimal liveness; authenticated store/recovery diagnostics |
| GET `/v1/identity`, `/v1/capabilities` | Synthetic profile and actual enabled features/limits |
| POST `/v1/messages` | `{recipient, content, attachment_ids}` acceptance |
| GET `/v1/operations/{id}` | Owner-scoped state and safe blocked reason |
| POST `/v1/operations/{id}/publish` | Local-double control: explicitly publish one owned operation; body `{}` |
| GET `/v1/inboxes/{mailbox}/messages` | Up to 16 authorized delivery/envelope rows, including handled/blocked/deferred state |
| POST `/v1/inboxes/{mailbox}/poll` | Local-double control: fetch one bounded page per configured mailbox; body `{}` |
| POST `/v1/inboxes/{mailbox}/claims` | `{consumer_id, max_messages, wait_seconds: 0}`; at most 8 claims |
| POST `/v1/deliveries/{id}/disposition` | `{lease_generation, lease_proof, outcome, handling_reference}` |
| POST `/v1/attachments` | Raw bounded bytes with `X-Attachment-Filename`; durable registered ID |
| GET `/v1/attachments/{id}/content` | Exact owner-authorized staged bytes |
| GET `/v1/messages/{id}/files/{filename}` | Exact bytes already fetched for an authorized consumer inbox |

Publish and poll are explicit prototype controls, not production API additions.
Publish retries preserve the selected operation's identity. Poll caches its page
result by operation key; a new poll uses a new key. Claims and dispositions cache
stable results, while old lease generations/epochs remain rejected.
Inbound file routes decode the filename once as UTF-8 before validation; spaces
and Unicode work, while escaped separators, traversal and double encoding fail.

There is no HTTP administration, shell, generic filesystem, arbitrary URL proxy,
attachment enumeration, storage deletion, or task execution endpoint. Normal
JSON mutation and data routes do not log bodies, URLs, proofs or authorization.

## Wire and storage rules

Normal wire fields are `id`, `date`, `from`, `to`, `content`, `files`.
Attachment entries retain `filename`, `storage`, and bounded opaque extensions.
Namespace `team:builder` routes under `a8s-test-mail/team/<ULID>.json`; normal
builder mail uses `a8s-test-mail/builder/<ULID>.json`. Receipt keys derive from the
original sender. IDs are strict uppercase standard ULIDs: lowercase, forbidden
Crockford characters and first digits above 7 are quarantined, not remapped.
Legacy error-only/file-without-storage entries are outside this parser subset.
Duplicate wire filenames and canonical JSON that expands past the body limit
are quarantined; polling continues to following keys. If generated outbound
wire exceeds the limit after resolving attachment references, its durable
operation stays blocked with `wire_body_too_large` and no remote publication.

The configured mail/storage prefixes are distinct constants. No request can
choose them. The local fake storage origin is `objects.example.invalid`;
references are nonresolving capabilities checked against an exact local
registry. Only exact registered capability equality authorizes retrieval;
forged same-prefix URLs, S3 URIs, userinfo, foreign/private hosts, encoded path
escapes and absent capability proofs fail. No credentialed fallback exists.
Fake references do not model AWS signing, URL expiry or real provider IAM.

Consumed custody survives while a deferred attachment prevents claiming. The
prototype does not yet retry deferred transfers or implement the final
attachment-failure transition. It leaves those messages visible and unhandled.

## Bounds and failure behavior

- 256 KiB JSON; 128 KiB content; 16 files; 1 MiB attachment bytes
- 32 request permits, held through response body consumption/cancellation
- One serialized blocking service/SQLite owner; exclusive process locking,
  DELETE journaling, FULL synchronous commits, 100 ms database busy timeout
- 8,192 database pages (32 MiB with the default 4 KiB page size); 5,000 counted
  records across messages, operations, staged attachments, diagnostics and
  deliveries; admission reserves 64 records plus each accepted delivery's
  remaining five-attempt claim budget
- Disposition idempotency lives in the existing delivery row (at most 32 cached
  completions across reviewed repairs); issued leases complete at the record
  cap without allocating a record; empty claims cannot consume reserved budget
- Expired final attempts become durable dead letters even if admission rejects
  caching the resulting empty claim: the request returns `quota`, its key is
  unaccepted, and terminal maintenance commits without allocating a record
- No history truncation, remote delete/reaper, automatic outbound drain, or
  retry loop; failure preserves pending/blocked work
- 16 objects per page, at most two configured mailbox pages per explicit poll;
  listing keeps only a bounded candidate page; failed exact GETs revisit on wrap
- 60-second claim; two-second expiry grace; retry waits five seconds;
  five unsuccessful claims leave a durable dead letter
- Request body read timeout 10 seconds; JSON parser nesting limit applies

Attachment bodies are bounded buffers/SQLite BLOBs, not the proposed 50 MiB
streaming implementation. Local directory enumeration time, idle TCP/header
resource limits, malicious aggregate load, disk exhaustion and production
fairness remain unmeasured. These bounds are not a validated 256 MiB load ceiling.
Record reservations do not reserve physical disk space: a full volume or failed
fsync can still prevent completion and must preserve the current lease for retry.

## Recovery commands

Stop the prototype before invoking commands that open its exclusive store.

```text
backup DB NEW_SNAPSHOT
restore SNAPSHOT NEW_DB
status DB
cutover DB
reviewed-requeue DB MESSAGE_ID
outbox DB
publish DB OBJECTS MESSAGE_ID
ingest DB OBJECTS ENVELOPE_JSON
```

Restore always holds, including a valid snapshot. Saved proofs are invalidated;
every restore gets a random new epoch. Cutover advances the join point, leaves
eligible old unhandled deliveries uncertain and old outbound blocked, and
preserves handled records, receipt evidence and deferred attachment state.
Selected reviewed IDs rederive eligibility from the stored envelope and exact
attachment custody before requeue; a receipt or missing file cannot become a
claimable task. The CLI is an
operator-only local recovery surface; there is no HTTP equivalent.

A separate fsynced epoch/sequence marker detects a database rollback when the
marker survives independently. Marker-write failure pauses the current process.
If an external rollback replaces both database and marker, the database cannot
detect its lost future; operator intervention is required. A supported restore
never bypasses the hold. Snapshot files include database rows and staged BLOBs;
local provider-double objects are separate test state. Encrypted off-host backup,
full export/import, schema migration and production reconciliation are absent.

## Verification

```bash
cargo fmt --check
cargo clippy --locked --offline --all-targets -- -D warnings
AR3_CONFORMANCE_DIR=/tmp/ar3-conformance cargo test --locked --offline
python3 tests/python_conformance.py /tmp/ar3-conformance
python3 tests/loopback_smoke.py target/release/ar3-server-prototype
cargo audit --file Cargo.lock
```

The two Python scripts check the prototype against the Python a8s receiver.
They need a checkout of the AR3 suite: set `AR3_REFERENCE_ROOT` to its path.
The Python receiver runs only inside temporary a8s state, with exact fake
attachment retrieval and a disabled HTTP fallback; no agent is invoked. The
check runs in one direction only (Python accepts what the Rust test wrote);
see KNOWN-DEFECTS.md.

Measured on one Apple-silicon machine with cargo 1.99: release build 25 s,
binary about 3.5 MB, 37 tests passing, about 8.4 MiB resident when idle and
9.3 MB after 200 accepted sends. This is not a load benchmark.

## Not implemented

Live AWS/S3 transport and attachment implementations, least-privilege IAM,
credential provisioning, HTTPS, production token/OIDC validation and revocation,
API grant administration, browser/MCP compatibility, a real MQTT client/broker,
lease extension, inbox/event pagination, deferred attachment retries and expiry,
streaming 50 MiB files, full observability, sustained load and real disk/kill testing and
operational restore review are not implemented or proven. Domain identifiers
remain named strings rather than the proposed identity newtypes. Dependency
license metadata is inventoried, not a complete SBOM/legal compliance review.

K7E, R4T execution, MCP and an actual disconnected-agent wake bridge are future
work. No capability here wakes an idle cloud agent. No live credentials, existing
A8S history, real network/storage state, server settings or persistent access
configuration are used by this prototype.
