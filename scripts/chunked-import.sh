#!/usr/bin/env bash
# Import a large NDJSON dump in chunks, pausing between chunks so the
# daemon's blob-store GC can reclaim path-copying orphans. A continuous
# import starves GC (it skips runs while writes are in flight) and the
# orphans can outgrow the disk; chunking is the operational fix.
#
# Usage:
#   scripts/chunked-import.sh dump.ndjson[.gz] --token TOKEN
#     [--url http://127.0.0.1:8080] [--chunk 250000] [--start-at N]
set -euo pipefail

URL="http://127.0.0.1:8080"
TOKEN="${REGISTRYD_TOKEN:-}"
CHUNK=250000
START=0
DUMP=""

while [ $# -gt 0 ]; do
  case "$1" in
    --url) URL="$2"; shift 2 ;;
    --token) TOKEN="$2"; shift 2 ;;
    --chunk) CHUNK="$2"; shift 2 ;;
    --start-at) START="$2"; shift 2 ;;
    -*) echo "unknown option: $1" >&2; exit 2 ;;
    *) DUMP="$1"; shift ;;
  esac
done
[ -n "$DUMP" ] && [ -f "$DUMP" ] || { echo "usage: $0 dump.ndjson[.gz] --token T" >&2; exit 2; }
[ -n "$TOKEN" ] || { echo "no token" >&2; exit 2; }

here=$(cd "$(dirname "$0")" && pwd)

count_lines() {
  case "$DUMP" in
    *.gz) gunzip -c "$DUMP" | wc -l ;;
    *) wc -l < "$DUMP" ;;
  esac
}

queue_depth() {
  curl -s -m 10 "$URL/health" | sed -n 's/.*"queue_depth":\([0-9]*\).*/\1/p'
}

TOTAL=$(count_lines)
echo "chunked import: $TOTAL records, chunk=$CHUNK, starting at $START"

offset=$START
while [ "$offset" -lt "$TOTAL" ]; do
  echo "=== chunk at offset $offset ($(date -u))"
  bash "$here/import-dump.sh" "$DUMP" --url "$URL" --token "$TOKEN" \
    --skip "$offset" --limit "$CHUNK"

  # Let the publisher drain fully…
  while true; do
    depth=$(queue_depth)
    [ -n "$depth" ] || depth=999999
    [ "$depth" -eq 0 ] && break
    echo "  waiting for publisher: queue_depth=$depth"
    sleep 30
  done

  # …then stand idle long enough for at least one GC cycle to run and
  # sweep this chunk's orphans (GC only runs while nothing is writing).
  before=$(sudo journalctl -u registryd --no-pager 2>/dev/null | grep -c "protect set collected" || true)
  echo "  idle for GC (had $before passes)..."
  for _ in $(seq 1 40); do
    sleep 30
    now=$(sudo journalctl -u registryd --no-pager 2>/dev/null | grep -c "protect set collected" || true)
    [ "$now" -gt "$before" ] && { echo "  gc pass observed"; break; }
  done

  offset=$((offset + CHUNK))
done
echo "chunked import complete at $(date -u)"
