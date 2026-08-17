# HTTP write API

`registryd` listens on `bind_addr` (default `127.0.0.1:8080`). The API is
deliberately small: submit records, read one back, health, ticket.

For public exposure put a TLS reverse proxy (Caddy, nginx) in front; the
daemon itself speaks plain HTTP. Binding to localhost and tunneling is
fine for demos.

## Authentication

Write endpoints require `Authorization: Bearer <token>` where the token
is any entry of `api_tokens` in the daemon config. Read endpoints
(`GET /v1/records/{key}`, `/health`, `/ticket`) are open — the same data
is world-readable over iroh anyway.

Missing/unknown token → `401 {"error":"unauthorized", ...}`.

## POST /v1/records

Submit one record.

```bash
curl -X POST localhost:8080/v1/records \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"key":"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
       "value":{"title":"example","iscc":"ISCC:EAAQCAIBABYAEAQE"}}'
```

`value` is normally a JSON object. It may also be a **string containing
serialized JSON** — the string is stored byte-for-byte, which preserves
original content hashes when importing an existing dump.

Responses:

| Status | Body | Meaning |
|---|---|---|
| `202` | `{"key":..., "status":"queued", "partition":N}` | accepted, queued for the next publish cycle |
| `200` | same + `"duplicate":true` | identical resubmission; idempotent no-op |
| `400` | `{"error":"invalid_key"...}` | key is neither CIDv1 nor a valid declaration id |
| `400` | `{"error":"invalid_value"...}` | value is not a JSON object |
| `409` | `{"error":"conflict", "existing_hash":"b3-..."}` | key exists with different content (immutability) |
| `413` | `{"error":"value_too_large"...}` | serialized value exceeds `max_value_bytes` |

## POST /v1/records/batch

Up to `batch_max_records` (default 500) records, same semantics each:

```json
{"records": [{"key": "...", "value": {...}}, ...]}
```

Always answers `207 Multi-Status` with per-record results:

```json
{"results": [
  {"key": "...", "status": "queued", "partition": 12, "duplicate": false},
  {"key": "...", "status": "error", "error": "conflict", "existing_hash": "b3-..."}
]}
```

A whole-request failure (`400 batch_too_large`, `401`) has no `results`.

## GET /v1/records/{key}

Convenience read-through against the local index + blob store, so a demo
can verify a write without standing up a reader node. The canonical read
path is `storectl` over iroh.

```json
{"key": "...", "status": "published", "hash": "b3-...", "partition": 249,
 "size": 54, "created_at": "...", "published_at": "...",
 "value": {"title": "example"}}
```

`status` is `pending` until the next publish cycle, then `published`
(or `denylisted`). Unknown key → `404 {"status":"not_found"}`.

## GET /health

Open, JSON, cheap — point your uptime monitor here.

```json
{"status": "ok", "queue_depth": 0, "records_total": 4300000,
 "partitions": 256, "endpoint_id": "<node id>",
 "last_publish_at": "2026-01-01T00:00:00+00:00",
 "cycles_completed": 42, "records_published": 12345,
 "last_publish_error": null}
```

`last_publish_error` non-null or a persistently growing `queue_depth` is
what "unhealthy" looks like.

## GET /ticket

The read-only ticket for the root pointer document, as plain text. Public
by design — hand it to anyone who should read the registry
(docs/reader-guide.md).
