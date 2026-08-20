# Manual testing & debugging guide

A step-by-step walkthrough for verifying every stage of the system by
hand: the write daemon on its server, the iroh data path, the console
reader, and the web viewer. Each step names the implementation it
exercises, shows the exact command, the output to expect, and what a
failure at that point means.

Set these once per shell; every command below uses them:

```bash
export ORIGIN_HOST=3.76.25.244                       # current deployment
export ORIGIN_API=http://$ORIGIN_HOST:8080
export SSH_KEY=~/.ssh/cdb-registry-demo-key.pem
export SSH="ssh -i $SSH_KEY ec2-user@$ORIGIN_HOST"
```

The read ticket is intentionally never written into this repository —
fetch it fresh whenever you need one:

```bash
export TICKET=$(curl -s $ORIGIN_API/ticket)
```

---

## 1. The moving parts (what you are testing)

```
 writer (EC2)                                readers (anywhere)
┌───────────────────────────┐               ┌──────────────────────────┐
│ registryd                 │               │ storectl (console)       │
│  HTTP write API :8080     │               │ registry-viewer (web UI) │
│  redb record index        │    iroh       │                          │
│  publisher → HAMT blobs   │ ◄──────────►  │ local iroh blob store    │
│  pointer doc (iroh-docs)  │  docs+blobs   │ replicated pointer doc   │
│  iroh endpoint (UDP)      │               │                          │
└───────────────────────────┘               └──────────────────────────┘
```

- Records enter through the **write API**
  ([`crates/registryd/src/api.rs`](../crates/registryd/src/api.rs),
  routes at line ~46) into a redb index.
- The **publisher** batches queued records into per-partition HAMT
  trees (256 partitions; each node is an immutable iroh-blobs blob —
  wire format in [`crates/registry-core/src/hamt.rs`](../crates/registry-core/src/hamt.rs))
  and writes each partition's new root hash into the **pointer
  document** ([`crates/registry-node/src/pointer_doc.rs`](../crates/registry-node/src/pointer_doc.rs)),
  the only mutable structure in the system.
- Readers hold a **read ticket** (an `iroh-docs` `DocTicket`: namespace
  capability + bootstrap addresses). iroh-docs replicates the pointer
  document to them; iroh-blobs fetches tree nodes and record values on
  demand. **All registry data moves over iroh** — the only HTTP a
  reader ever performs is the optional one-line `GET /ticket`
  bootstrap.

---

## 2. Origin server checks (EC2)

### 2.1 Is the daemon process healthy?

```bash
$SSH systemctl status registryd --no-pager
$SSH journalctl -u registryd -n 50 --no-pager -o short-iso
```

Healthy startup ends with two lines (from
[`crates/registryd/src/main.rs`](../crates/registryd/src/main.rs) ~190):

```
read ticket exported (also served on GET /ticket) endpoint_id=…
write API listening addr=0.0.0.0:8080
```

**If the service is `active` but there is no "write API listening" line
after the last "Started registryd" line:** the process is inside the
iroh-blobs one-time consistency scan that follows any *unclean* store
close (SIGKILL, OOM-kill, power loss). Nothing is wrong — but it reads
the whole ~110 GB store before serving. Watch its progress by byte
count:

```bash
$SSH 'P=$(systemctl show -p MainPID --value registryd); sudo cat /proc/$P/io | grep read_bytes; ps -o etime= -p $P'
```

Divide `read_bytes` by elapsed time for the rate; the scan ends around
the store size (`sudo du -sh /var/lib/registryd/blobs`). **Never restart
the daemon during the scan** — that restarts it from zero. This is also
why the daemon must only ever be stopped with SIGTERM
(`sudo systemctl stop registryd`): it drains the publisher and closes
the store cleanly (`main.rs` ~230).

### 2.2 What does the daemon think it holds?

```bash
curl -s $ORIGIN_API/health | python3 -m json.tool
```

Handler: `health` in [`crates/registryd/src/api.rs`](../crates/registryd/src/api.rs) (~433).
Field meaning:

| field | meaning | healthy value today |
|---|---|---|
| `records_total` | rows in the redb record index (ground truth of ingested data) | 6,667,578 |
| `queue_depth` | records ingested but not yet in published trees | 0 when idle |
| `records_published` / `cycles_completed` | publisher progress counters since this process start | grows after writes |
| `last_publish_at` / `last_publish_error` | last publisher cycle result | error `null` |
| `endpoint_id` | the daemon's iroh identity — must match the ticket's provider | stable across restarts |

`records_total` vs what a reader sees is THE end-to-end check: a reader
that has fully synced must show the same number.

### 2.3 Read a record straight from the daemon (bypassing iroh)

```bash
curl -s $ORIGIN_API/v1/records/<key> | python3 -m json.tool
```

Handler: `get_record` in `api.rs` (~410). This answers from the
daemon's local index + blob store. Use it to decide **which side** of a
discrepancy is broken: if the daemon returns the record but a reader
cannot resolve it over iroh, the publish/sync path is at fault; if even
this fails, ingestion is.

### 2.4 iroh connectivity of the origin

```bash
# the daemon's UDP socket (the port is chosen at bind time)
$SSH 'sudo ss -ulnp | grep registryd'

# security group already allows UDP 0-65535 inbound; verify direct
# traffic actually arrives while a reader is fetching:
$SSH "sudo timeout 20 tcpdump -c 20 -n udp and host \$(echo \$SSH_CLIENT | cut -d' ' -f1)"
```

If readers can only reach the origin through an iroh relay
(`…relay.n0.iroh.link` in their logs, plus
`Dropping received relay packet: no available capacity` under load),
bulk sync throughput collapses. Direct UDP both ways is what makes the
batched sync fast.

### 2.5 Memory protection (why the daemon must not be OOM-killed)

The box has 2 GB RAM. An OOM SIGKILL is the worst failure available:
it forces the hours-long consistency scan of 2.1. Two guards exist:

- 4 GB swapfile (`swapon --show`);
- `MemoryHigh=1400M` on the unit (kernel reclaims/throttles the daemon
  instead of killing it): persisted as a drop-in in
  `/etc/systemd/system.control/registryd.service.d/`. Verify with
  `systemctl show registryd -p MemoryHigh`.

Check for past kills: `$SSH 'sudo dmesg -T | grep -i oom | tail'`.

On the client side, batched fetches are deliberately paced —
`PREFETCH_CONCURRENCY = 2` requests in flight, 32 blobs per request, in
[`crates/registry-node/src/blob_store.rs`](../crates/registry-node/src/blob_store.rs)
(`prefetch`). History: an unpaced client (512-blob batches × 6 walks)
spiked the daemon to 1.47 GB RSS and the kernel OOM-killed it.

### 2.6 Continuous ingestion (declarations → iroh)

New declarations flow in automatically: a systemd timer on the origin
host (`registry-updater.timer`, every 6 hours — the declaration source
cannot be listed incrementally, so each run streams a full listing and
filters by upload time against a stored watermark) enriches new source
objects exactly like the bulk export did and submits them to the write
API. Submission is idempotent, so overlapping windows are harmless.
Once the publisher's next cycle lands they reach every iroh reader.

```bash
$SSH systemctl list-timers registry-updater.timer --no-pager
$SSH journalctl -u registry-updater -n 30 --no-pager   # per-run summary lines
$SSH sudo cat /var/lib/registryd-updater/state.json    # watermark + last counts
```

End-to-end freshness check: the `data_updated_at` field of the viewer's
`/api/partitions` (and the "newest record was inserted" line in the UI)
must advance past the watermark after a publish cycle follows an
updater run that submitted records.

### 2.7 Server file layout

```
/var/lib/registryd/
  blobs/        iroh-blobs store (HAMT nodes + values + doc entry blobs)
  docs/         iroh-docs replica store (the pointer document)
  index.redb    record index (write-side ground truth)
  secrets/      node identity — endpoint_id stability across restarts
  read-ticket.txt   the ticket served on GET /ticket
/etc/registryd/config.toml   (root-readable; api tokens + tuning)
```

---

## 3. Reading through iroh from the console (`storectl`)

Build once: `cargo build --release` → binaries in `target/release/`.

Ticket resolution order for every subcommand (implementation:
[`crates/storectl/src/config.rs`](../crates/storectl/src/config.rs) `resolve`, ~56):
`--ticket` flag → `STORECTL_READ_TICKET` env → `read_ticket` in
`~/.config/storectl/config.toml` (macOS:
`~/Library/Application Support/storectl/config.toml`) → compiled-in
default (empty in this repo).

### 3.1 Resolve one record end-to-end over iroh

```bash
./target/release/storectl get <key> --ephemeral --ticket "$TICKET"
```

`--ephemeral` forces a throwaway in-memory node — this is the "prove a
total stranger can read the registry" test. What happens, in order
(`get` in [`crates/storectl/src/commands.rs`](../crates/storectl/src/commands.rs) ~34):

1. spawn an iroh node, import the ticket, start doc sync;
2. poll until the key's partition root appears in the replicated
   pointer doc (`wait_for_partition_root`, 20 s deadline) — **exercises
   iroh-docs sync**;
3. descend the partition's HAMT (`hamt::lookup`,
   [`crates/registry-core/src/hamt.rs`](../crates/registry-core/src/hamt.rs) ~125),
   fetching each node blob — **exercises iroh-blobs fetch-on-miss**
   ([`crates/registry-node/src/blob_store.rs`](../crates/registry-node/src/blob_store.rs) `get`);
4. fetch the value blob and print the JSON.

Validating a production record's enrichment while you are at it:

```bash
./target/release/storectl get <key> --ephemeral --ticket "$TICKET" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); \
     print('signature:', 'present' if d.get('signature') else 'MISSING'); \
     print('tsaSignature:', 'present' if d.get('tsaSignature') else 'MISSING'); \
     print('timestamp:', d.get('commonsDbRegistry',{}).get('timestamp'))"
```

Every record in the current dataset must have `signature`, and
`timestamp >= 2026-06-01`.

### 3.2 Enumerate a partition / count everything

```bash
# one partition, counts only
./target/release/storectl list --partition 167 --summary --ephemeral --ticket "$TICKET"

# the whole registry (256 partitions; prints per-partition counts)
./target/release/storectl list --summary --ephemeral --ticket "$TICKET"
```

The walk uses the shared breadth-first parallel walker
(`walk_entries_parallel`,
[`crates/registry-core/src/hamt.rs`](../crates/registry-core/src/hamt.rs)):
each tree level is first pulled in batched `GetMany` requests
(`prefetch` in `blob_store.rs` — one round-trip per ~32 blobs instead
of one per blob), then decoded concurrently. A partition that cannot be
fully fetched is *reported and skipped*, never fatal, and a partial
listing is returned (`WalkOutcome.complete == false`).

The full-registry summary total must equal `records_total` from § 2.2.

### 3.3 Watch publishes live

```bash
./target/release/storectl watch --ticket "$TICKET"
```

Subscribes to the pointer document's event stream
(`docs.import_and_subscribe`, `commands.rs` ~697). Submit a record on
the write side and you should see `InsertRemote`/`ContentReady` events
within a few seconds. This is the same signal the viewer uses to wake
its sync loop instantly (`doc_event_wake`,
[`crates/registry-viewer/src/main.rs`](../crates/registry-viewer/src/main.rs)).

### 3.4 Local reader state

```bash
./target/release/storectl status          # identity, synced roots, cache size
./target/release/storectl config          # where its ticket/storage came from
./target/release/storectl identity        # endpoint id (safe while a seed runs)
```

---

## 4. The web viewer

### 4.1 Fresh machine, one command

```bash
cargo build --release -p registry-viewer
./target/release/registry-viewer --ticket-url $ORIGIN_API/ticket
# open http://127.0.0.1:8090
```

`--ticket-url` performs the single bootstrap HTTP GET (`fetch_ticket`,
[`crates/registry-viewer/src/main.rs`](../crates/registry-viewer/src/main.rs))
and everything afterwards is iroh. A stored ticket also works
(`--ticket`, env, config file) but a freshly served one always carries
the origin's *current* direct addresses.

To simulate a genuinely new machine on a machine that has run one
before, point `--storage-dir` at an empty directory:

```bash
./target/release/registry-viewer --ticket-url $ORIGIN_API/ticket \
  --bind 127.0.0.1:8093 --storage-dir /tmp/viewer-fresh-test
```

Expected: the dashboard is up within ~2 s; "roots synced" reaches 256
within seconds (pointer doc replication); the record counter climbs as
partitions replicate; the state banner flips to green "in sync with the
origin" when `behind` reaches 0.

### 4.2 What the sync loop does (what you are watching)

Implementation: `sync_loop` in `crates/registry-viewer/src/main.rs`.

1. **Restore**: previous per-partition state is loaded from
   `viewer-state.json` and everything locally walkable is served
   immediately (a restart never blanks the UI; this is the node's own
   cache, not a backup channel — a fresh directory starts empty).
2. **Pass**: one streamed query snapshots all 256 roots from the
   replicated doc (`PointerDoc::partition_roots`,
   [`crates/registry-node/src/pointer_doc.rs`](../crates/registry-node/src/pointer_doc.rs));
   partitions whose root moved are re-walked, `PARTITION_CONCURRENCY`
   (4) at a time, local store first, network for the remainder.
3. **Wake**: any pointer-doc event triggers the next pass immediately
   (`doc_event_wake`); otherwise passes idle at 15 s. Failed partitions
   keep serving their last good listing and are retried each pass
   (`mark_stale`).

### 4.3 Poke the viewer's API directly

```bash
V=http://127.0.0.1:8090
curl -s $V/api/status      | python3 -m json.tool   # identity, ticket source, sync stats
curl -s $V/api/partitions  | python3 -m json.tool | head -40
curl -s "$V/api/partition/167?offset=0&limit=3" | python3 -m json.tool
curl -s $V/api/record/<key> | python3 -m json.tool
```

Field semantics (handlers in `crates/registry-viewer/src/main.rs`):

- `/api/status` — `roots_synced` = partitions present in the local doc
  replica; `sync.behind` = partitions whose published root ≠ last
  walked root; `syncing_now` = walks in flight. Served from memory,
  never touches the network.
- `/api/partitions` — per-partition `records`, `synced_at`,
  `stale_error` (last refresh failure, last good data still served),
  `behind` flag; `pending` = published but never walked yet.
- `GET /api/similar?iscc=<code>&max_distance=<0..16>&limit=<n>` — exact
  Hamming nearest-neighbor search over ISCC Content-Codes, computed
  entirely from the local replica (works offline): the query ISCC's
  64-bit Content-Code is compared against every synced record's inline
  code. `scanned`/`with_content_code` in the response tell you the
  coverage the search actually had.
- `POST /api/sync` / `POST /api/sync/<partition>` — queue an explicit
  (forced) sync of everything / one partition; the response confirms the
  queue. In `--sync-mode manual` this is the only way index blobs are
  fetched; in auto mode it doubles as cache-bypassing verification.
- `/api/record/<key>` — `found` refers to the **index entry**.
  `value` is the JSON if the value blob could be fetched; otherwise
  `value_error` says exactly why (values are fetched on demand from
  the origin unless the viewer runs `--warm-values`; with the origin
  offline, index lookups still work but values are unavailable —
  that is a designed property, not data loss).

### 4.4 The macOS LaunchAgent (this machine's permanent viewer)

```bash
launchctl print gui/$(id -u)/com.commonsdb.registry-viewer | head -30   # running?
tail -f ~/Library/Logs/registry-viewer.log                              # live log
# clean restart (SIGTERM → graceful close of the local store):
launchctl bootout gui/$(id -u)/com.commonsdb.registry-viewer
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.commonsdb.registry-viewer.plist
# deploy a rebuilt binary between those two commands:
cp target/release/registry-viewer ~/.local/bin/registry-viewer
```

Its store lives in `~/Library/Application Support/storectl/` (identity,
`blobs/`, `docs/`, `viewer-state.json`).

---

## 5. Local sandbox: the full pipeline on one machine

Runs the complete write→publish→iroh→read cycle in ~1 minute, no
network dependencies. This is the fastest way to isolate "is it the
code or is it the deployment".

```bash
# 1. a scratch daemon (new terminal; Ctrl-C stops it cleanly)
REGISTRYD_BIND_ADDR=127.0.0.1:18080 \
REGISTRYD_DATA_DIR=/tmp/registryd-sandbox \
REGISTRYD_API_TOKENS=testtoken \
REGISTRYD_PUBLISH_INTERVAL_SECS=5 \
REGISTRYD_PUBLISH_MAX_PENDING=50000 \
./target/release/registryd run

# 2. feed it 30k generated records (valid CIDv1 keys)
python3 scripts/gen-test-records.py 30000 http://127.0.0.1:18080 testtoken

# 3. wait for queue_depth to hit 0
watch -n2 'curl -s http://127.0.0.1:18080/health | python3 -m json.tool'

# 4. a fresh reader against it — full sync should take ~20-30 s
./target/release/registry-viewer --ticket-url http://127.0.0.1:18080/ticket \
  --bind 127.0.0.1:8092 --storage-dir /tmp/viewer-sandbox
# open http://127.0.0.1:8092 — watch it climb to 30,000 / green

# 5. console read of a sandbox record (key printed by the generator)
./target/release/storectl get <key> --ephemeral \
  --ticket "$(curl -s http://127.0.0.1:18080/ticket)"

# 6. live-update check: submit 500 more, viewer should show 30,500
#    within ~15 s (5 s publish cycle + doc event + walk)
python3 scripts/gen-test-records.py 500 http://127.0.0.1:18080 testtoken --offset 900000
```

All `REGISTRYD_*` environment overrides:
[`crates/registryd/src/config.rs`](../crates/registryd/src/config.rs) (~99).

---

## 6. Troubleshooting matrix

| symptom | first check | usual cause / fix |
|---|---|---|
| `curl $ORIGIN_API/health` → connection refused | § 2.1 journal + scan progress | daemon mid-consistency-scan after an unclean stop — wait it out, never restart |
| viewer "roots synced 0", banner stuck on "waiting for the pointer document" | `storectl watch` from the same machine; `providers` in `/api/status` | dead/stale ticket (re-fetch from `/ticket`), origin offline, or UDP egress blocked |
| partitions synced but record `value unavailable` | `/api/record` → `value_error` text | values are on-demand; origin offline or blob GC'd — § 4.3; run `--warm-values` for a full local replica |
| sync crawls (records/s ~ tens) | reader log for `relay` WARNs; § 2.4 tcpdump | relay-bound path (no direct UDP) or unbatched fetching (prefetch broken) |
| partitions flip to red `stale_error: index incomplete` | can `storectl list --partition N` walk it? § 2.3 daemon-side read | origin briefly down mid-walk (self-heals next pass) or genuinely missing blobs on the origin — then `registryd verify` |
| daemon OOM-killed (journal: `oom-kill`) | `dmesg` § 2.5; who was reading heavily? | MemoryHigh drop-in must exist; reader pacing (`PREFETCH_CONCURRENCY`) must be in place |
| reader and daemon record counts differ after full sync | § 3.2 summary vs § 2.2 `records_total` | publisher gap → `registryd verify [--fix]` (stop the daemon first — docs/operator-guide.md) |
| viewer UI frozen / API slow | `curl $V/api/status` (must be instant) | handlers are all served from memory now; if slow, the process is wedged — check its log, restart cleanly |
| reader dies with cascading `poisoned storage should not be used` panics | first panic in its log (`bao_file.rs`) | a blob write was cancelled mid-transfer — batch fetches must be driven to completion, deadlines checked only between requests (`walk_entries_parallel` in `crates/registry-core/src/hamt.rs`); wipe the reader's storage dir and restart, the origin is unaffected |

---

## 7. Invariants worth spot-checking after any change

1. **Same bytes everywhere**: `hash` shown by `/api/record` equals the
   BLAKE3 of the value bytes (`content_hash`,
   [`crates/registry-core/src/record.rs`](../crates/registry-core/src/record.rs) ~54) —
   iroh verifies this on transfer, so a mismatch means local
   corruption.
2. **Deterministic trees**: re-publishing the same records yields the
   same partition roots (unit-proven for the bulk path; compare roots
   across `/api/partitions` and `storectl status`).
3. **Counts line up**: daemon `records_total` = full `storectl list
   --summary` total = viewer `total_records` at `behind == 0`.
4. **A stranger can read**: `storectl get --ephemeral` with only the
   `/ticket` output works from any network that allows outbound UDP.
