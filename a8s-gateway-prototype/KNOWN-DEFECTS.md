# Known defects

Found by a read of the source and by a run of the built binary on loopback,
on 2026-10-02. `repro.sh` reproduces defects 1, 2, 3 and 5:

```bash
cargo build --release --locked
bash repro.sh target/release/ar3-server-prototype /tmp/gw-repro
```

The script uses loopback port 18090 and the synthetic test identities.

## Defects. The first three stop the gateway in normal use.

1. **The byte cap is reached long before the record cap, and nothing frees
   it.** `src/store.rs` (the page cap, attachment staging, admission).
   Upload 32 of 1 MiB returns `503 durable_storage_unavailable`.
2. **A restore and cutover drops every message published before the
   cutover.** `cutover` sets the join point to now, and ingest files every
   older id as `before_cutover_uncertain_history`. After backup, restore,
   cutover and restart, a poll reports `objects:1, custody:0` and a claim
   returns no message. The Python node keeps its first join point across
   restarts and takes mail up to one hour older than it. Mail that arrives
   while a gateway is down must be delivered.
3. **Every poll and every claim adds a permanent operations row, and the
   record cap then refuses all new work.** A send returns 202; after 5000
   empty claims the same send returns `429 quota`. A client that polls each
   10 seconds reaches this in about 14 hours.
4. **No Host or Origin check on the listener** (`src/api.rs`). A page in a
   local browser can reach a loopback listener by DNS rebinding. Found by
   reading; not reproduced.
5. **A repeated disposition is not idempotent across a re-claim.** The same
   idempotency key returns `200` before a re-claim and `409 stale_lease`
   after it.
6. **An attachment upload to storage happens before the wire envelope is
   frozen, and also when the envelope is then refused.** The object stays in
   storage with no message.
7. **An inbound envelope that carries the id of an outbound message that is
   not final blocks the mailbox.**
8. **Lease proofs are stored in plain text** in the cached operation
   response. A reader of the database can complete any lease.
9. **A different object at the publish key blocks that message forever** as
   `provider_result_unknown`. A Python sender stamps a retry again and PUTs
   to the same key, so this case is normal on a real network.
10. **State directory permissions are not checked on open, and backup and
    restore do not set file modes.** Also: `restore SNAPSHOT name.sqlite`
    with a relative destination fails with `provider_unavailable`, because
    the parent of a bare file name is empty.

## Where the prototype differs from the Python wire

- A lowercase ULID is rejected, and so is a first character of 8 to Z.
  Python accepts lowercase and compares in upper case.
- A 256 KiB envelope cap, and an oversize object is fetched again on every
  poll. Python has no cap on receive.
- All six envelope fields are required. Python needs `id` and `to`.
- The file entry `{filename, error, detail}` is rejected. Python honours it.
- `to` is compared as raw text, and the cutoff compares the full ULID string
  with no grace period.

## The conformance check is one-way

`tests/python_conformance.py` proves that the Python receiver accepts what
the Rust test wrote. It does not prove the reverse, and it runs only when a
person sets an environment variable. Before a second implementation of a wire
is trusted, the first implementation must be the oracle in both directions:
it produces the inputs the second one consumes, and it consumes what the
second one produces, in the default test run, against one storage double
that enforces what the real service enforces.
