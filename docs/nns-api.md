# Similarity search (NNS) HTTP API

Any `registry-viewer` node doubles as a similarity-search service over
the registry's ISCC Content-Codes. The search is **exact** (a full
Hamming-distance scan of every locally replicated code — no
approximate-index misses), answers in ~15 ms for millions of records,
and needs no external services: everything is computed from the node's
own replica, so it keeps working when the origin is down.

An external frontend (e.g. a search site) can call it directly:
responses to `GET` endpoints carry `Access-Control-Allow-Origin: *`, so
browser apps on other domains work out of the box.

## Deploying an instance for external callers

```bash
registry-viewer --ticket-url http://<origin-host>:8080/ticket --bind 0.0.0.0:8090
```

The default index-only replica (~2 GB for 6.7 M records) is all the
search needs; `--warm-values` is not required. Put a TLS-terminating
proxy in front for public exposure — the viewer itself speaks plain
HTTP and has no authentication (its data is public by design, but the
`POST /api/sync*` maintenance endpoints are best not exposed; only
`GET` is CORS-enabled).

## Endpoint

```
GET /api/similar?iscc=<code>&max_distance=<n>&limit=<n>
```

| parameter | required | default | meaning |
|---|---|---|---|
| `iscc` | yes | — | ISCC to search near. Accepts a full composite code (`ISCC:KEC…`) or a bare Content-Code unit; case-insensitive; the `ISCC:` scheme prefix is optional. The 64-bit Content-Code component is extracted and used. |
| `max_distance` | no | `8` | maximum Hamming distance (differing bits) over the 64-bit code, clamped to `0..=16`. `0` = identical content code. |
| `limit` | no | `50` | maximum matches returned, clamped to `1..=500`. |

### Response — `200 OK`, `application/json`

```json
{
  "query_code": "0xbcc8e0988e9b2fca",
  "max_distance": 12,
  "scanned": 6667578,
  "with_content_code": 6667578,
  "elapsed_ms": 14,
  "matches": [
    {
      "key": "bbqjca223mtn2ccc6ddvosrugs4emklwuoyptzvqhfehi3s24ioizdvmx",
      "distance": 0,
      "partition": 167,
      "content_code": "0xbcc8e0988e9b2fca"
    }
  ]
}
```

| field | meaning |
|---|---|
| `query_code` | the 64-bit Content-Code decoded from the `iscc` parameter (hex) |
| `max_distance` | the effective radius after clamping |
| `scanned` / `with_content_code` | how many codes the search actually compared — **the coverage guarantee**. Equals the full registry when the node's index replica is in sync (compare with `total_records` from `/api/partitions`); lower means the replica is still catching up and results are complete only over what is local. |
| `elapsed_ms` | server-side search time |
| `matches` | ascending by `distance` (ties by partition/position), truncated to `limit`. At most 10 000 candidates are collected before ranking. |
| `matches[].key` | the record key — feed it to `GET /api/record/<key>` for the full record (declaration JSON, signatures), or to `GET /api/external-metadata/<key>` for the central registry's view |
| `matches[].distance` | differing bits between the query and this record's Content-Code (0–16) |
| `matches[].content_code` | the record's own 64-bit Content-Code (hex) |

### Errors

| status | body | cause |
|---|---|---|
| `400` | `{"error": "not a decodable ISCC …"}` | the `iscc` parameter is not a valid ISCC / Content-Code unit |
| `500` | `{"error": …}` | internal failure (should not happen; check the node's log) |

### Examples

```bash
# nearest neighbours within 12 bits
curl 'http://<node>:8090/api/similar?iscc=ISCC:KECXLCNDPDGLFHV6XTEOBGEOTMX4UZPF756V2P7R5CN3V67P2UHW6MY&max_distance=12&limit=20'

# exact content-code match only
curl 'http://<node>:8090/api/similar?iscc=ISCC:KEC…&max_distance=0'
```

From a browser app:

```js
const r = await fetch(`https://nns.example.org/api/similar?iscc=${encodeURIComponent(iscc)}&max_distance=10`);
const { matches, scanned } = await r.json();
```

## Semantics & limitations

- Distance is over the **64-bit ISCC Content-Code only** — perceptual
  near-duplicate detection by content component, the same metric the
  predecessor vector-database setup used. Meta/Data/Instance components
  do not participate.
- Results are as fresh as the node's replica: with the node in `auto`
  sync mode that is seconds behind the origin's publishes.
- The scan is exhaustive and exact; there is no recall/precision
  trade-off to tune. If the registry ever grows by orders of magnitude,
  the banded index in `registry-core::similarity` (already implemented
  and unit-tested) is the drop-in acceleration path.
