# ar3-rust

Rust reference code for [AR3](https://github.com/witw-llc/ar3), the suite
that lets independent command-line agents work together: a8s (the message
router), r4t (the roster) and k7e (the knowledge engine). AR3 itself is
written in Python. This repository holds Rust work on it.

## Contents

| Directory | What it is | Status |
|---|---|---|
| [`a8s-gateway-prototype/`](a8s-gateway-prototype/) | An HTTP gateway for the a8s message network, with a SQLite custody store and offline provider doubles | Archived prototype from 2026-10-02. Reference only; read its `KNOWN-DEFECTS.md` first |

## What the prototype is good for

- A measured baseline: a 3.5 MB binary that idles at about 8 MB.
- A worked API design for a hosted a8s gateway: durable sends, claims with a
  lease, explicit dispositions, attachment capabilities.
- Synthetic a8s wire fixtures.
- A list of what a second implementation of the a8s wire got wrong on its
  first day, with a script that reproduces four of the defects.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
