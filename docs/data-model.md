# Data model and publishing semantics

This document is the wire-format contract. Everything here is stable:
readers built against it (including `storectl` releases in the wild) must
keep working, so changes to any encoding described below are breaking
changes to the whole network.

## Records

A record is a `(key, value)` pair:

- **key** — either a syntactically valid **CIDv1** string, or the legacy
  declaration-identifier format: exactly 57 characters over the RFC 4648
  base32 alphabet that big-integer-decode (base-x style, no padding, no
  multibase prefix) to 36 bytes starting `0x01 0x0c 0x12 0x20`. The key is
  operator-assigned and never derived from the value.
- **value** — a JSON object, at most `max_value_bytes` (64 KiB default)
  serialized. Stored verbatim as UTF-8 bytes; those bytes are the unit of
  content addressing.

**Immutability**: a key, once accepted, is permanently bound to the BLAKE3
hash of its value bytes. Resubmitting the identical value is an idempotent
no-op; submitting different bytes under the same key is a conflict (HTTP
409). There is no update or delete — only the denylist (see the operator
guide) can exclude a key from future published trees.

## Content addressing

Every stored object — record values and index nodes alike — is an
iroh-blobs blob addressed by its 32-byte BLAKE3 hash. The daemon computes
`blake3(value_bytes)` at the HTTP tier; iroh-blobs independently arrives
at the same hash on import, which is asserted in code.

## Partitioning

```
digest      = sha256(key_bytes)
partition   = digest[0] mod 256
descent[d]  = digest[1 + d]        # byte used at HAMT depth d
```

One sha256 digest per key covers both the partition assignment and the
entire trie descent path. 256 top-level partitions is a deployment-wide
constant baked into every reader.

## The HAMT

Each partition is an independent hash array mapped trie. A node blob is
self-describing via a one-byte tag:

- **Leaf** (`0x00`): tag byte followed by newline-delimited JSON, one
  `LeafEntry` per line:

  ```json
  {"key":"bafy...","hash":"<64 hex chars>","size":123,
   "added_at":"2026-01-01T00:00:00Z","content_code":1234567890}
  ```

  `content_code` (the record's 64-bit ISCC Content-Code, decimal) is
  optional and omitted when absent. Entries are sorted by key. A leaf
  holds at most `leaf_max_entries` (1024 default) entries; inserting past
  that splits it into an intermediate whose children redistribute the
  entries by their next descent byte.

- **Intermediate** (`0x01`): tag byte followed by exactly 256 × 32 bytes —
  child node hashes by slot, all-zeros meaning "no child".

Lookup cost is one blob fetch per level plus the leaf; at hundreds of
millions of records that is still only ~3–4 fetches.

Inserts are **path-copying**: only the nodes on the modified path are
rewritten, siblings are shared with the previous version. Re-inserting an
existing key with the same hash is a structural no-op (this is what makes
at-least-once queue redelivery safe). Superseded nodes become orphans and
are eventually removed by GC.

## The root pointer document

The only mutable structure: one iroh-docs document with one entry per
partition under the key `partition/<id>` (zero-padded to three digits,
e.g. `partition/042`). Each entry's content is the partition's current
32-byte root hash, stored as its own tiny blob. An optional
`iscc-index/root` entry points at the similarity index
(docs/similarity-search.md).

The **read ticket** grants read-only access to this document plus the
addresses of bootstrap peers. It is public by design — access control is
"can you reach a peer", and the data is meant to be read.

A reader resolving a key therefore: syncs the doc → reads
`partition/<id>` → fetches the root node blob → descends the trie →
fetches the value blob. All blob fetches fall back to the ticket's
provider nodes on a local cache miss.

**Hard rule learned in production**: an entry that exists but whose
content blob is unreadable is an *error*, never "no root". Treating a
transient read failure as an empty partition once caused silent
partition resets in a predecessor deployment.

## Existence and duplicate detection

The daemon tracks every accepted key in an embedded redb index:
`key → {content_hash, size, partition_id, status, created_at,
content_code?, published_at?}` with `status ∈ pending | published |
denylisted`. The same database holds the pending queue (sequence number →
key) the publisher drains.

## Publishing

The single publisher task turns pending queue rows into published trees.
One cycle:

1. read the oldest batch (up to `publish_max_pending`) from the queue,
2. group entries by partition,
3. per partition: `insert_batch` onto the current root, write the new
   root hash to the pointer document — **this write is the durability
   boundary** — then mark the records published and delete their queue
   rows,
4. denylisted keys are excluded from the tree and marked `denylisted`
   instead.

A failure anywhere leaves the queue rows in place; the next cycle replays
them and the idempotent HAMT insert converges. Records are never marked
published before their partition's new root is referenced from the
pointer document.

Cycles trigger on `publish_max_pending` pending records or
`publish_interval_secs` elapsed, whichever comes first; with a backlog
(bulk import) cycles run back-to-back.

There is exactly one writer by construction — one process, one publisher
task, partitions processed sequentially. If publishing is ever made
concurrent, rebuilds of the same partition MUST be serialized: two
concurrent rebuilds from the same old root lose the loser's records while
the index already says "published" (a real, observed failure mode — about
1% of records under load, invisible until audited).

## Garbage collection

Path-copying orphans superseded index nodes on every publish. iroh-blobs
GC runs on `gc_interval_secs` and sweeps every blob not in the protect
set, which registryd enumerates as: every value hash in the record index
(this covers pending records whose blobs no tree references yet), every
reachable node and value of every partition tree, the similarity index if
present, and the pointer document's own entry blobs. GC is skipped
entirely while a publish or submission is mid-flight, and an enumeration
error aborts the sweep — an incomplete protect set must never reach it.
