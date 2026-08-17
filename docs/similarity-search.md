# ISCC similarity search

The registry carries optional support for near-duplicate lookup over
[ISCC](https://iscc.codes) (ISO 24138) Content-Codes.

## What is always on

When a submitted value contains an `iscc` field (matched
case-insensitively), the daemon decodes its 64-bit Content-Code and
stores it in the record's index row **and** in the published leaf
entries' `content_code` field. This costs nothing and means a future
similarity index can be built without resyncing or re-reading any
values — and a reader resolving a known key gets the code for free.

Supported ISCC inputs: a bare 64-bit Content-Code unit, or the canonical
256-bit composite ISCC-CODE (the Content unit is extracted). Undecodable
or absent codes are simply not indexed; that is never an error.

## The index (not published by this daemon yet)

`registry-core` ships the full banded index implementation
(`similarity.rs`, `iscc_store.rs`, both unit-tested) and `storectl
similar` ships the query path: the 64-bit code is split into 8 bands of
8 bits; each band value maps to a bucket of candidate keys; a query
probes its bands, unions the candidates, and verifies exact Hamming
distance client-side using the inline codes. The index lives in the same
blob store under the pointer-document entry `iscc-index/root`.

`registryd` does not yet build that index after publishes — the daemon
keeps its single pass ISCC-free. Until it does, `storectl similar`
against this node reports that no index is published. Wiring it in is a
contained change to the publisher (collect the cycle's decoded members,
`iscc_store::insert_batch`, publish the new root) — the storage format,
query tooling, and GC protection for it already exist in this
repository.
