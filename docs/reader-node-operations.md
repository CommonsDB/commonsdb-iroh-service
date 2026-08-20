# Reader node operations: stop, restore, survive crashes

How to safely stop and restore a local reader node (`registry-viewer` —
one process containing both the dashboard and the embedded iroh node),
and what actually happens in each case.

> **⚠️ The one rule**
>
> **Never force-kill the process** — no `kill -9`, no Activity Monitor
> "Force Quit", no `pkill -9`. The node must close its blob store
> cleanly. An unclean close never loses data, but the next start will
> verify the **entire** store before serving — seconds for a small
> index-only cache, **tens of minutes for a full value replica**.
> A clean stop makes the next start instant.

---

## Where the node's data lives

| path (macOS default) | contents |
|---|---|
| `~/Library/Application Support/storectl/blobs/` | every replicated blob: index nodes + (with `--warm-values`) record values |
| `…/storectl/docs/` | the replicated pointer document |
| `…/storectl/identity` | the node's key — keeps the same network identity across restarts (matters when this node is listed as a provider in a shared ticket) |
| `…/storectl/viewer-state.json` | last-known per-partition summary (instant UI after restart) |

Leave this directory alone while the node is stopped. Nothing in it
expires or degrades.

---

## Stop safely

**Foreground run (any OS):** `Ctrl-C` — that is SIGTERM handling; it
finishes in-flight requests and closes the store cleanly.

**macOS LaunchAgent:**

```bash
launchctl bootout gui/$(id -u)/com.commonsdb.registry-viewer
```

This delivers SIGTERM and *also disables the auto-restart* (`KeepAlive`),
so the node stays down until you bring it back deliberately.

While stopped: the dashboard is down, syncing pauses, and other readers
cannot fetch blobs from this machine. The registry itself is unaffected —
the node simply accumulates lag and catches up later.

---

## Restore

**Foreground:** run the same command as before (same `--storage-dir`).

**macOS LaunchAgent:**

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.commonsdb.registry-viewer.plist
```

### Decoding launchctl's errors

| message | actual meaning | what to do |
|---|---|---|
| `Bootstrap failed: 5: Input/output error` right after a bootout | launchd race — the old instance is still unloading | wait 2–3 s, run bootstrap again |
| `Bootstrap failed: 5` repeatedly | usually the service is **already loaded**, or another viewer process (e.g. one started manually with `nohup`) already holds the port | `launchctl print gui/$(id -u)/com.commonsdb.registry-viewer` to see if it's loaded; `lsof -nP -iTCP:8090 -sTCP:LISTEN` to see who owns the port — stop that process (plain `kill -TERM <pid>`, wait for it to exit) and bootstrap again |
| `Boot-out failed: 3: No such process` | the service is **not loaded** — it is already stopped as far as launchd is concerned | nothing to do; if a viewer process is still running it was started outside launchd — stop it with `kill -TERM <pid>` |

One instance per storage directory, always: two viewers pointed at the
same `--storage-dir` cannot coexist (the second fails on the store lock
or the port). If launchctl behaves erratically across several attempts
(persistent I/O errors, phantom states), that is launchd itself wedged —
a reboot clears it, and `RunAtLoad` brings the viewer back
automatically.

What happens on start, in order:

1. dashboard serving within ~2 s;
2. last-known state shown immediately (from `viewer-state.json`);
3. local store re-walked — seconds to a couple of minutes, no network;
4. node reconnects with its **same identity** and doc sync resumes;
5. partitions that changed while it was down sync automatically
   (`auto` mode) or on request (`manual` mode);
6. first similarity query rebuilds its in-memory index (~300 ms),
   subsequent ones are ~15 ms.

> **⚠️ After every restart of a warming node**
>
> The value-replica warm ("fetch every record's value") is *requested*
> in memory, so a restart pauses it. Nothing is lost or re-downloaded —
> press **"Force full re-sync"** in the UI (or
> `curl -X POST http://127.0.0.1:8090/api/sync`) and it resumes exactly
> where it stopped (already-present blobs are skipped).
>
> Stopping *during* an active warm is safe: on SIGTERM the sync loop
> drains its in-flight transfers (a few seconds) before the store
> closes, so the next start is still instant.

---

## Self-healing and the optional watchdog

A reader never needs routine restarts. Transfers that stop making
progress (a peer that is up but not answering) are detected in ~45 s
and abandoned by closing their connection — the sync loop then retries
on a fresh one (`run_transfer_with_stall_guard` in
`crates/registry-node/src/blob_store.rs`). The origin restarting, its
IP changing, or the network dropping are all recovered automatically;
the ticket's identity anchor plus the relay make old tickets keep
working after address changes.

For unattended reader machines, `scripts/viewer-watchdog.sh` adds a
belt-and-suspenders layer: run it every 5 minutes (launchd
`StartInterval=300` / cron) and it restarts the viewer only on the two
wedge signatures (API dead long past startup, or a frozen sync pass
with work pending). It respects manual stops and startup verification
scans.

## If it was killed uncleanly anyway

Crash, power loss, force-kill — data is still safe. The next start runs
the store verification scan **before** the API comes up; the log stays
quiet while it runs.

> **⚠️ Do not restart a node mid-scan** — the scan starts over from
> zero. Let it finish; the node then serves normally.

Watch a scan's progress by bytes read (compare against the store's size):

```bash
P=$(pgrep -x registry-viewer)   # or the registryd PID on a server
cat /proc/$P/io | grep read_bytes           # Linux
# macOS has no /proc — watch disk read activity of the PID instead:
top -pid $P -stats pid,command,disks
```

The same rules apply, with bigger stakes, to the origin daemon
(`registryd`): stop it only via `sudo systemctl stop registryd`
(SIGTERM, clean close) — see docs/testing-guide.md, "Origin server
checks", for its scan-progress commands.
