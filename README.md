# commonsdb-iroh-service

A single-node content registry served over the [iroh](https://iroh.computer)
peer-to-peer network. One small daemon (`registryd`) on one cheap server:

- accepts new declarations through an authenticated HTTP API,
- publishes them into a partitioned, content-addressed HAMT index,
- serves the whole dataset to **any** third-party reader node via a read
  ticket — readers sync directly over iroh, no gateway or cloud services
  involved.

The bundled `storectl` CLI is the reference reader: give it the ticket and
it can `get`, `watch`, `list`, and re-serve (`seed`) the registry from
anywhere.

## Quickstart: a registry in 5 minutes

On a fresh Debian/Ubuntu server (any small VM works — 2 vCPU / 4 GB is
plenty):

```bash
curl -fsSL https://raw.githubusercontent.com/commonsdb/commonsdb-iroh-service/main/deploy/install.sh | sudo bash
```

The installer prints a generated API token and starts the daemon. Submit a
record:

```bash
TOKEN=...   # from the installer output (or /etc/registryd/config.toml)
curl -X POST localhost:8080/v1/records \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"key":"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
       "value":{"title":"hello registry"}}'
```

Within the publish interval (30 s by default) the record is live on the
P2P network. From **any other machine**:

```bash
TICKET=$(curl -s your-server:8080/ticket)   # public by design
storectl --ticket "$TICKET" get bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi
```

That's the whole system.

## How it works

```
                    ┌────────────────────────────────────────────┐
 HTTP POST /records │                registryd                   │
 ──────────────────►│  axum write API ──► embedded queue (redb)  │
   (bearer token)   │                        │                   │
                    │              publisher loop (single task)  │
                    │                        │                   │
                    │      HAMT partitions + pointer doc         │
                    │   (iroh-docs + iroh-blobs, persistent)     │
                    └───────────────┬────────────────────────────┘
                                    │ iroh QUIC (UDP)
                     read ticket    ▼
              storectl / any reader node syncs doc + fetches blobs
```

Records are keyed by CIDv1 (or a legacy declaration-id format), sharded
into 256 partitions, and stored in per-partition hash array mapped tries
whose nodes are content-addressed iroh blobs. A tiny mutable "root pointer
document" (iroh-docs) maps each partition to its current root hash; the
read ticket grants read-only access to that document. Everything else is
immutable. See [docs/data-model.md](docs/data-model.md).

## Repository layout

| Crate | What it is |
|---|---|
| `crates/registry-core` | Pure data structures: keys, partitioning, HAMT engine, ISCC decoding. No I/O, fully unit-tested. |
| `crates/registry-node` | The iroh integration: node bootstrap, blob store, pointer document. |
| `crates/registryd` | The daemon: HTTP API, embedded queue/index (redb), publisher, `verify` audit. |
| `crates/storectl` | The distributable read-only client. |
| `crates/registry-viewer` | Micro web app: a local dashboard that reviews the registry through a reader node's eyes (partition browser, key lookup). |

Plus `deploy/` (systemd unit, installer, Dockerfile/compose),
`scripts/` (dump import, backups), and `docs/`.

## Documentation

- [docs/operator-guide.md](docs/operator-guide.md) — provision → running
  node, configuration reference, backups, verify, takedowns, monitoring.
- [docs/reader-guide.md](docs/reader-guide.md) — for third parties:
  ticket → `storectl`.
- [docs/api.md](docs/api.md) — the HTTP write API.
- [docs/testing-guide.md](docs/testing-guide.md) — manual verification
  of every stage by hand: origin checks, console reads over iroh,
  viewer internals, a one-machine sandbox of the full pipeline, and a
  troubleshooting matrix.
- [docs/reader-node-operations.md](docs/reader-node-operations.md) —
  **how to stop and restore a local reader node safely** (clean
  shutdown, crash recovery, resuming a value warm).
- [docs/nns-api.md](docs/nns-api.md) — the similarity-search HTTP API
  (exact ISCC Content-Code NNS) served by every viewer node, for
  external frontends.
- [docs/data-model.md](docs/data-model.md) — partitioning, HAMT wire
  format, publishing semantics.
- [docs/similarity-search.md](docs/similarity-search.md) — the optional
  ISCC similarity index.

## Building from source

```bash
cargo build --release          # registryd + storectl + registry-viewer in target/release/
cargo test --workspace         # unit tests (no network needed)
```

## Seeing a deployed registry from any machine

The viewer bootstraps itself from a running deployment — no config
files, no pre-shared ticket:

```bash
cargo build --release -p registry-viewer
./target/release/registry-viewer --ticket-url http://<origin-host>:8080/ticket
# open http://127.0.0.1:8090 — the dashboard shows sync progress live
```

It replicates the registry index over iroh in the background (parallel
partition walks, woken instantly by origin publishes) and keeps serving
its last good state while offline. See
[docs/reader-guide.md](docs/reader-guide.md) for details.

## Operational lessons baked in

A few behaviors exist because their absence hurt in a larger predecessor
deployment; they are worth knowing about:

- **Clean shutdown always.** SIGTERM drains the publisher and closes the
  blob store; an unclean kill costs a full-store consistency scan on the
  next start. The systemd unit allows 300 s for this.
- **A missing tree root is an error, never "empty".** Transient read
  failures must not silently rebuild a partition from scratch.
- **`registryd verify` is the only honest audit** — it walks the actual
  published trees and diffs them against the record index.
- **Local disk only** for the data directory; never a network filesystem.

## License

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
