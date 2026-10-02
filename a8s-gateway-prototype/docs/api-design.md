# Gateway API design and a8s compatibility contract

Status: a design, written on 2026-10-02. The prototype in this directory implements a subset of it. No endpoint is deployed. This API wraps existing a8s wire data; API v1 is not a new a8s envelope version.

## Boundary rules

- HTTPS API base: /v1. Authentication is required except bounded liveness information
- Tenant, network grants, acting sender, and allowed inboxes come from the authenticated principal
- JSON input rejects unknown writable fields and invalid types; no silently coerced from, tenant, provider URL, bucket, object key, or shell command
- Inputs and outputs are UTF-8; message content is preserved byte-for-byte after JSON decoding, including newlines, dollar signs, backticks, and backslashes
- Server-created outbound messages receive a ULID and UTC date once, within the durable operation acceptance transaction
- Idempotency-Key is required for mutation requests. Scope is tenant + principal + operation kind + key. Same key and same canonical request returns the same operation; same key with different request returns 409
- Reusing an idempotency key cannot bypass current token/tenant validation. Revoked principals cannot read old responses
- User content, URLs with query strings, tokens, and lease secrets are excluded from access logs. Responses containing mail or attachment capabilities use Cache-Control: no-store
- A stable opaque event cursor is a local durable sequence, not a remote S3 continuation token, timestamp, or ULID

## Proposed endpoints

| Method and path | Required capability | Meaning |
|---|---|---|
| GET /health/live | None, minimal result | Process responds; reveals no tenant/provider config |
| GET /health/ready | Operator or restricted ingress | Storage/schema/recovery status; provider degradation separately described |
| GET /v1/capabilities | Authenticated | Enabled service features, limits, protocols, provider readiness, wake modes |
| GET /v1/identity | Authenticated | Current principal, tenant, seat and effective capabilities |
| POST /v1/messages | send on selected seat/destination | Durably accept an outbound operation |
| GET /v1/operations/{operation_id} | Owner or explicit observer grant | Distinguish accepted, publishing, published, blocked, uncertain, rejected |
| GET /v1/messages/{message_id} | Sender or inbox grant | Read one permitted canonical message and scoped delivery status |
| GET /v1/inboxes/{mailbox}/messages | inbox read | Bounded read-only view; never a claim or acknowledgement |
| POST /v1/inboxes/{mailbox}/claims | claim registered consumer | Acquire up to the bounded limit, optionally wait up to 25 seconds |
| POST /v1/deliveries/{delivery_id}/lease | Claim holder | Extend current fenced lease within maximum duration |
| POST /v1/deliveries/{delivery_id}/disposition | Claim holder | Record handled, blocked, or retry; requires current lease generation |
| POST /v1/attachments | Attachment create | Stream one bounded upload and return its registered ID |
| GET /v1/attachments/{attachment_id} | Owner or linked-message grant | Metadata for one known object |
| GET /v1/attachments/{attachment_id}/content | Owner or linked-message grant | Stream exact authorized bytes, or an explicitly requested short-lived capability |
| GET /v1/events | Scoped event read | Page committed event references after a durable cursor |

No raw S3 proxy, arbitrary URL downloader, filesystem path reader, remote storage listing/deletion, shell execution, credential echo, dynamic provider registration, or automatic task execution endpoint is in the first version.

## Send operation

Example request, all identifiers synthetic:

```json
{
  "recipient": "team:builder",
  "content": "Please review the attached result.\nKeep the result under this message ID.",
  "attachment_ids": ["att_example_001"]
}
```

The principal is assigned sender analyst. The server checks that this sender may contact team:builder and may use att_example_001. The client does not provide from, tenant, date, wire files URLs, or remote list.

Response after durable acceptance, HTTP 202:

```json
{
  "operation_id": "op_example_001",
  "message_id": "<server-assigned-ULID>",
  "state": "accepted",
  "status_url": "/v1/operations/op_example_001"
}
```

accepted means queued. The operation status includes per-remote publish states and observed receipt states; it must not collapse provider acceptance into recipient delivery. Missing credentials or recovery_hold may be returned before acceptance as a rejection, or after durable acceptance as blocked with an actionable reason. There is no successful sent response for a message stored nowhere.

An upload completes local durable staging before its ID becomes attachable. Its exact remote reference must be usable before the message is published. No client-provided path is opened, even if it resembles a familiar sandbox or server path.

Acceptance stores the immutable logical request and attachment IDs, not a premature wire envelope with nonexistent URLs. The worker finalizes the exact wire envelope once references are ready, before any publish attempt, and freezes its bytes/digest. Pre-finalization URL refresh leaves the logical request unchanged; post-finalization refresh may not overwrite that ULID, even after a timeout whose publish outcome is unknown.

## Inbox, consumer, lease, and disposition

Reading a message is safe to repeat. Claims are operations on a pre-registered consumer within a mailbox. Registering a second consumer changes who receives independent copies and is not implicit in a connection or request.

Example claim body:

```json
{
  "consumer_id": "consumer_example_001",
  "max_messages": 10,
  "wait_seconds": 20
}
```

A returned delivery contains the envelope, delivery ID, current lease generation, opaque lease proof, expiry, and attempt count. Lease proof is secret-bearing request data and is never a URL parameter or log field. The database generation is authoritative; client clocks cannot extend a lease or settle a stale one.

Example disposition body:

```json
{
  "lease_generation": 3,
  "lease_proof": "<opaque-current-lease-proof>",
  "outcome": "handled",
  "handling_reference": "result_example_001"
}
```

- handled: the client completed its disposition and durably retained the result or follow-on operation references
- blocked: the client/server stores a durable pending item with a specific blocker, intended action, and authority still required; this remains visibly open
- retry: releases the current attempt into bounded backoff, preserving history

The server cannot prove a remote client's unreported side effects. A handling_reference is evidence supplied by that client, not proof of exactly-once completion. An action with an uncertain external result remains uncertain until reconciled. Do not acknowledge handled merely because the model received the message.

An idempotent repeat of a successfully committed disposition returns that disposition. A conflicting stale or wrong-owner disposition returns 409 or 403 without altering state. Concurrent lease acquisition, expiry, extension, and completion must be tested with controlled races.

## Existing A8S envelope profile

Normal envelope:

```json
{
  "id": "<ULID>",
  "date": "2026-10-02T12:00:00Z",
  "from": "analyst",
  "to": "team:builder",
  "content": "Byte-exact UTF-8 text",
  "files": [
    {
      "filename": "result.txt",
      "storage": ["<authorized-exact-object-URL>"]
    }
  ]
}
```

Field names are id, date, from, to, content, files, not body, timestamp, attachments, or a newly invented version. The API may use clearer field names, but the adapter translates them once at the wire boundary. Do not stamp a top-level v:1 onto every message and call it compatible.

Existing extensions may be retained as opaque bounded data for round-trip fidelity; reserved control fields must be parsed and validated before any special behavior. Unknown control variants are quarantined or ignored according to a documented compatibility policy, never interpreted as an ordinary authorized task or accepted as a valid receipt.

Simple mailbox names use upstream canonical_name: strip, lowercase, validate [A-Za-z0-9][A-Za-z0-9_-]*. Namespace suffixes remain in the envelope. The server's allowed namespace binding rules must be explicit; validating only the prefix does not authorize arbitrary suffix routing.

Record one ULID parser policy explicitly. The inspected Python is_ulid accepts any 26 Crockford characters, including a first digit that exceeds the standard 128-bit range; a Rust ULID library may reject those. Server-generated IDs must be standard ULIDs. Add a malformed/legacy-input compatibility case and document strict rejection or quarantine instead of silently changing an incoming ID to make it parse.

Normal S3 key:

```text
<remote-prefix>/<canonical-first-recipient-segment>/<message-id>.json
```

For recipient team:builder, the mailbox segment is team, not team:builder. Do not URL-decode a caller's arbitrary object path and treat it as a mailbox name.

Receipts retain normal envelope fields and use the reserved destination __a8s_receipt__, from _a8s, empty content, and empty files. Their a8s_control value names type delivery_receipt, version 2, for_id, original sender, recipient array, stage, file array, and detail. The receipt envelope has its own new ULID. The S3 mailbox is derived from the original sender, not from __a8s_receipt__.

Permitted version-2 stages: inbox_write, attachment_fetched, attachment_failed, deferred, expired, no_local_recipient. no_local_recipient from one network node is not proof that no other node delivered the message. Delivery/control input from a shared network is network-asserted evidence, not authenticated authorization to act for the original sender.

No wire-level handled receipt is added in the first version. Do not mislabel inbox_write or broker acknowledgement as handled.

## S3 adapter operations

Remote adapter:

- publish receives already finalized envelope bytes and stable remote/key identity
- poll receives an explicit mailbox set and bounded cursor/page budget
- accepted incoming envelope commits custody and consumed state atomically
- a malformed object produces a bounded durable diagnostic without allowing the first page to starve later valid mail
- same ULID with different canonical envelope content is a conflict, not a benign duplicate
- no delete-on-read; initial gateway does not run a remote reaper unless separately enabled and authorized

Attachment adapter:

- stage_upload and publish_upload use a stable server-generated attachment operation ID and exact key
- retrieve_exact uses a registered ID and ownership/message grant
- inbound registration additionally requires the supplied valid bearer capability or an independent exact-object grant; an allowed prefix plus message text cannot authorize a credentialed GET, and expired URL signatures never trigger credential fallback
- presign_exact creates a scoped expiring bearer reference only when configured and authorized
- it has no list_prefix, delete_prefix, or generic proxy method

Mail and storage prefixes must be disjoint by path segment, including ancestor/descendant overlap. A string prefix comparison alone must not treat mail/a and mail/ab as the same path. Endpoint configuration is fixed by the operator and must be validated separately from a URL presented by an incoming message.

## Errors and state visibility

Use a consistent bounded error shape:

```json
{
  "error": {
    "code": "recovery_hold",
    "message": "Message handling is paused until recovery is reviewed.",
    "request_id": "req_example_001",
    "retryable": false
  }
}
```

Required distinctions: invalid_input (400), unauthenticated (401), forbidden (403), not_found (404), idempotency_conflict/stale_lease (409), body_too_large (413), quota/rate_limited (429), provider_unavailable/durable_storage_unavailable (503), and recovery_hold. A blocked durable operation may be an ordinary 200 status result rather than a transport error. Retry-After is included only where a retry could help. Never expose provider raw errors that may contain credentials or signed URLs.

## MCP projection

The remote MCP adapter calls the same application methods and receives the same principal context. It must not wrap shell commands, invent from arguments, or provide less strict access than REST. A tool call's JSON-RPC request ID is not the durable Idempotency-Key; tools that mutate need an explicit stable operation key.

Proposed tools: tell, inbox_list, inbox_claim, inbox_ack, operation_status, attachment_info. Map inbox_ack to the disposition contract, with precise outcome names. Binary transfer should use the service's authorized file route; do not base64 arbitrary-size files into a tool response. Tool descriptions state which actions mutate and what accepted means.

Capabilities expose the tested MCP revision and whether connected notifications or a separately verified host wake bridge are available. Notification delivery does not change inbox state. One protocol's optional resumability is not a substitute for the service's own event cursor.

## Conformance sources

The wire behaviour this design follows is the Python implementation in the
[AR3 suite](https://github.com/witw-llc/ar3) at `v0.1.99`: `apps/a8s/tell.py`
(envelope writing), `apps/a8s/core.py` (canonical naming),
`apps/a8s/mailbox.py` (attachment wire transformation),
`apps/a8s/transports/s3.py` (S3 key selection and polling),
`apps/a8s/delivery_receipt.py` (receipt builder and parser) and
`apps/a8s/mcp_server.py` (the local MCP tool).
