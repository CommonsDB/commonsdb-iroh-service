# Operator guide

From empty server to serving registry, and everything needed to keep it
healthy afterwards.

## 1. Provisioning

Any small Linux VM with a public address works. Reference sizing: 2 vCPU,
4 GB RAM, 40 GB local SSD comfortably holds several million records with
headroom. Requirements:

- **local disk** for the data directory — never NFS/SMB or another
  network filesystem; the blob store's consistency assumptions don't
  survive them,
- outbound UDP open (iroh always works via relays); for best performance
  also allow inbound UDP so peers can connect directly — iroh binds a
  dynamic port, so either allow inbound UDP generally or skip a host
  firewall on the node,
- one TCP port if the write API is exposed beyond localhost (see TLS
  below).

## 2. Install

```bash
curl -fsSL https://raw.githubusercontent.com/commonsdb/commonsdb-iroh-service/main/deploy/install.sh | sudo bash
```

This installs `/usr/local/bin/registryd`, creates the `registryd` system
user and `/var/lib/registryd`, writes `/etc/registryd/config.toml` with a
generated API token, and enables the systemd unit. Alternatively use
`deploy/docker-compose.yml`, or build from source and copy the pieces by
hand — the unit file is `deploy/registryd.service`.

First-start checklist:

```bash
systemctl status registryd
curl -s localhost:8080/health | jq
curl -s localhost:8080/ticket        # save this — it's what readers need
```

## 3. Configuration

`/etc/registryd/config.toml` (mode 640, it contains tokens). Every key is
optional; defaults shown. Environment variables override the file —
useful for containers.

| Key | Default | Env override | Notes |
|---|---|---|---|
| `bind_addr` | `127.0.0.1:8080` | `REGISTRYD_BIND_ADDR` | keep on loopback behind a proxy |
| `data_dir` | `/var/lib/registryd` | `REGISTRYD_DATA_DIR` | local disk only |
| `api_tokens` | `[]` | `REGISTRYD_API_TOKENS` (comma-sep) | must be non-empty to start |
| `max_value_bytes` | `65536` | `REGISTRYD_MAX_VALUE_BYTES` | |
| `batch_max_records` | `500` | `REGISTRYD_BATCH_MAX_RECORDS` | |
| `publish_max_pending` | `1000` | `REGISTRYD_PUBLISH_MAX_PENDING` | publish when this many pending |
| `publish_interval_secs` | `30` | `REGISTRYD_PUBLISH_INTERVAL_SECS` | …or this much time passed |
| `top_level_partitions` | `256` | `REGISTRYD_TOP_LEVEL_PARTITIONS` | wire format — do not change |
| `leaf_max_entries` | `1024` | `REGISTRYD_LEAF_MAX_ENTRIES` | wire format — do not change |
| `denylist_path` | none | `REGISTRYD_DENYLIST_PATH` | see Takedowns |
| `gc_interval_secs` | `3600` | `REGISTRYD_GC_INTERVAL_SECS` | 0 disables GC |

Logging: `LOG_LEVEL` (or `RUST_LOG`, which wins) controls verbosity; the
default filters iroh's routine P2P chatter down to errors.

### TLS

The daemon speaks plain HTTP. To accept writes from elsewhere, put Caddy
in front — with a domain pointed at the server this is the whole config
(`/etc/caddy/Caddyfile`):

```
registry.example.org {
    reverse_proxy 127.0.0.1:8080
}
```

Bare `bind_addr = "0.0.0.0:8080"` with the bearer token as the only
protection is acceptable for short-lived demos, not for anything else.

## 4. Data directory and keys

```
/var/lib/registryd/
├── blobs/            # iroh-blobs store (values + index nodes)
├── docs/             # iroh-docs store (root pointer document)
├── index.redb        # record index + pending queue
├── read-ticket.txt   # exported on every start; also GET /ticket
└── secrets/
    ├── node-secret-key    # node identity → EndpointId in every ticket
    ├── namespace-secret   # identity of the pointer document
    └── author-secret      # signs pointer document entries
```

The three key files are generated on first start and **must be preserved
and kept secret**: losing/regenerating them changes the node identity and
the document namespace, invalidating every distributed read ticket
(readers would need a new ticket). They are 32 raw bytes each; the
backup script includes them.

## 5. Lifecycle

`systemctl stop registryd` sends SIGTERM: the API stops, the publisher
drains what is already due, and the blob store is flushed and closed
deliberately. **Never SIGKILL a healthy daemon** — an unclean close costs
a full-store consistency scan on the next open (hours of silent startup
on a big store). The unit's `TimeoutStopSec=300` exists for exactly this;
a normal stop takes seconds.

Upgrades: stop, replace the binary, start. All on-disk formats are
versioned by their own stores.

## 6. Backups

```bash
scripts/backup.sh /var/backups/registryd            # local directory
scripts/backup.sh box:registryd-backups             # any rclone remote
```

Dated `tar.gz` of the whole data dir; `--keep N` prunes old ones
(default 7). The daemon can keep running — both stores commit through
fsync, so a live copy is crash-consistent (equivalent to a power-cut
snapshot). For byte-perfect backups stop the daemon first.

Restore: stop, untar over `data_dir`, start, then run `registryd verify`.

Cron example (nightly at 03:17):

```
17 3 * * * registryd /path/to/scripts/backup.sh box:registryd-backups
```

## 7. Verify

```bash
systemctl stop registryd
registryd verify            # audit: index vs actual tree contents
registryd verify --fix      # re-queue anything missing
systemctl start registryd
```

Walks every partition's published tree and diffs it against the record
index — the only audit that cannot be fooled by status flags. On a
single-writer node it should always report zero missing, which makes it a
strong health check after imports, restores, or crashes. Non-zero exit =
gaps found. It opens the same stores as the daemon, so the daemon must be
stopped; a full pass over millions of records takes a few minutes on
local NVMe.

## 8. Importing an existing dataset

Given an NDJSON dump (`{"key":...,"value":...}` per line, optionally
gzipped):

```bash
scripts/import-dump.sh dump.ndjson.gz --token $TOKEN            # everything
scripts/import-dump.sh dump.ndjson.gz --token $TOKEN --limit 100000   # demo subset
```

Runs against localhost by default; the publisher keeps up in the
background (watch `queue_depth` on `/health`). Order of magnitude: a few
hours for millions of records on local NVMe, minutes for a 100k demo
subset. Re-running the same dump is safe — duplicates are idempotent.
Afterwards, run `registryd verify` (section 7).

## 9. Takedowns

Create a denylist file, e.g. `/etc/registryd/denylist.txt`:

```
# one key per line, comments allowed
bafybeig...offending-key
```

Set `denylist_path = "/etc/registryd/denylist.txt"` and the publisher
excludes those keys from every future published tree (already-queued
copies are marked `denylisted` instead of published). The file is
re-read every publish cycle — no restart needed for additions.

Note: content published *before* a key was denylisted remains in old tree
versions and in caches of readers who already fetched it; the denylist
governs what the registry publishes going forward.

## 10. Monitoring

`GET /health` is cheap, unauthenticated JSON — point a free uptime
monitor at it. Alert on: HTTP down, `last_publish_error` non-null, or
`queue_depth` growing without bound. `journalctl -u registryd` carries
one line per publish cycle.

## 11. Troubleshooting

| Symptom | Likely cause |
|---|---|
| start hangs for a long time, no log output | previous unclean shutdown → consistency scan; let it finish, then find what killed the process |
| `no API tokens configured` at start | `api_tokens` empty — set it in config or env |
| publish cycle errors mentioning a missing node blob | the store lost data (disk?); restore from backup, run `verify --fix` |
| readers time out fetching | node unreachable over UDP — check the server firewall; readers fall back to relays, which are slower but must also not be blocked |
| record stays `pending` | publisher stalled — check `/health` `last_publish_error` and the journal |
