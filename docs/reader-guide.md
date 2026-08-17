# Reader guide (storectl)

Everything a third party needs to read the registry: the **read ticket**
(a long base32 string) and the `storectl` binary. The ticket is public;
ask the operator or fetch it from the node's `GET /ticket` endpoint.

## Install

Download a release binary, or build from source:

```bash
cargo build --release -p storectl   # target/release/storectl
```

## Configure the ticket

Precedence, highest first:

1. `--ticket <string>` flag
2. `STORECTL_READ_TICKET` environment variable
3. `read_ticket` in the config file — `~/.config/storectl/config.toml` on
   Linux, `~/Library/Application Support/storectl/config.toml` on macOS:

   ```toml
   read_ticket = "<the long base32 ticket string>"
   # storage_dir = "/custom/cache/location"   # optional
   ```

4. a ticket compiled into the binary at release time (a release pipeline
   may overwrite `crates/storectl/default_ticket.txt` before building;
   this repository's own builds keep it empty)

`storectl config` prints what was resolved and from where.

## Commands

```bash
storectl get <key>                # resolve and print one record's JSON
storectl get <key> --out f.json   # …to a file
storectl get <key> --ephemeral    # throwaway identity, no local cache
storectl watch                    # follow live pointer-document updates
storectl list --summary           # per-partition record counts
storectl list --partition 42      # enumerate one partition's records
storectl similar <ISCC>           # similarity search (when an index is published)
storectl seed                     # run forever, re-serving all partitions
storectl status                   # local identity, cache size
storectl identity                 # print this node's EndpointId
```

First reads on a fresh machine take seconds (pointer document sync +
tree descent over the network); subsequent reads hit the local cache.
The cache lives under the platform data dir (`storectl status` shows it)
and can be deleted freely.

## Running a seed node

`storectl seed` turns any machine into an always-on replica: it syncs
every partition and serves blobs back to the network, taking read load
off the origin and keeping the dataset available if the origin is down.
Point a systemd unit at it and forget it. Seeding requires the same
network posture as the origin node: outbound UDP, ideally unrestricted
inbound UDP.

## Visual review: registry-viewer

For a point-and-click look at the same data, the workspace ships a micro
web app:

```bash
cargo run -p registry-viewer -- --ticket "$TICKET"
# then open http://127.0.0.1:8090
```

On a brand-new machine there is an even shorter path: fetch the ticket
straight from a running `registryd`'s public `/ticket` endpoint. This is
also the most reliable way to join — a ticket stored weeks ago carries
the origin's direct addresses from back then, while a freshly served one
is always current:

```bash
cargo build --release -p registry-viewer
./target/release/registry-viewer --ticket-url http://<origin-host>:8080/ticket
# then open http://127.0.0.1:8090
```

(`VIEWER_TICKET_URL` works too.) Two sync modes exist:
`--sync-mode auto` (default) keeps every partition's index replicated
continuously; `--sync-mode manual` replicates only the tiny pointer
document — the dashboard then shows exactly which partitions are
published and how far behind the local copy is, and index data is
fetched only when requested: click a pending partition, use "Sync all
now", or call `POST /api/sync` / `POST /api/sync/<partition>`. Manual
mode keeps a node near-zero-footprint until you ask for data.

In auto mode the viewer joins the network exactly
like `storectl` (same ticket resolution, no privileged access), starts a
background sync that replicates every partition's index locally —
changed partitions are walked in parallel, and a publish at the origin
wakes the loop immediately via the pointer-document event stream — and
serves a local dashboard: live sync progress (records replicated,
partitions in flight/queued, per-partition freshness), paged partition
listings, and record lookup with the full JSON value. Partitions the
origin has republished show amber until re-walked; failures show red
while the last good listing keeps being served. Useful for demos and
for eyeballing a freshly imported dataset. `--ephemeral` skips the
local cache; `--bind` moves the port; `--warm-values` additionally
replicates every record's value bytes (full seeder behavior). It keeps
its cache separate from storectl's, so both can run on one machine.

## Verifying independently

A reader never has to trust a gateway: every index node and value is
content-addressed, so the bytes fetched either hash to what the tree
references or they are rejected by the transport. `storectl list
--summary` against your own seed is an independent count of what the
registry actually publishes.
