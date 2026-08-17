#!/usr/bin/env bash
# Stream an NDJSON dump into a running registryd's batch write API.
#
# Each input line must be a JSON object: {"key": "...", "value": {...}}
# (value may also be a string containing serialized JSON — it is passed
# through verbatim). Gzipped dumps are detected by the .gz suffix.
#
# Usage:
#   scripts/import-dump.sh dump.ndjson[.gz] [options]
#
# Options:
#   --url URL          registryd base URL (default http://127.0.0.1:8080)
#   --token TOKEN      bearer token (default: $REGISTRYD_TOKEN)
#   --batch-size N     records per request (default 500, the API limit)
#   --limit N          stop after N records (demo subsets)
#   --skip N           skip the first N records (chunked/resumed imports)
#
# The publisher keeps up in the background; watch `GET /health` for
# queue_depth to reach 0 after the import finishes.
set -euo pipefail

URL="http://127.0.0.1:8080"
TOKEN="${REGISTRYD_TOKEN:-}"
BATCH_SIZE=500
LIMIT=0
SKIP=0
DUMP=""

while [ $# -gt 0 ]; do
  case "$1" in
    --url) URL="$2"; shift 2 ;;
    --token) TOKEN="$2"; shift 2 ;;
    --batch-size) BATCH_SIZE="$2"; shift 2 ;;
    --limit) LIMIT="$2"; shift 2 ;;
    --skip) SKIP="$2"; shift 2 ;;
    -h|--help) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) echo "unknown option: $1" >&2; exit 2 ;;
    *) DUMP="$1"; shift ;;
  esac
done

[ -n "$DUMP" ] || { echo "usage: $0 dump.ndjson[.gz] [--url U] [--token T] [--batch-size N] [--limit N]" >&2; exit 2; }
[ -f "$DUMP" ] || { echo "no such file: $DUMP" >&2; exit 2; }
[ -n "$TOKEN" ] || { echo "no token: pass --token or set REGISTRYD_TOKEN" >&2; exit 2; }

reader() {
  case "$DUMP" in
    *.gz) gunzip -c "$DUMP" ;;
    *) cat "$DUMP" ;;
  esac
}

limiter() {
  if [ "$LIMIT" -gt 0 ]; then head -n "$LIMIT"; else cat; fi
}

skipper() {
  if [ "$SKIP" -gt 0 ]; then tail -n +"$((SKIP + 1))"; else cat; fi
}

sent=0
errors=0
batch_file=$(mktemp)
trap 'rm -f "$batch_file"' EXIT

flush() {
  local count="$1"
  [ "$count" -gt 0 ] || return 0
  {
    printf '{"records":['
    paste -sd, - < "$batch_file"
    printf ']}'
  } > "$batch_file.body"
  # Batches are idempotent (duplicates are no-ops), so a stalled or failed
  # request is safely retried. --max-time turns a half-dead connection
  # into a retry instead of an indefinite hang.
  local response http_code attempt
  for attempt in 1 2 3; do
    if response=$(curl -sS --max-time 300 -w '\n%{http_code}' -X POST "$URL/v1/records/batch" \
      -H 'content-type: application/json' \
      -H "Authorization: Bearer $TOKEN" \
      --data-binary @"$batch_file.body"); then
      http_code=${response##*$'\n'}
    else
      http_code=000
    fi
    [ "$http_code" = "207" ] && break
    echo "batch attempt $attempt failed (HTTP $http_code); retrying" >&2
    sleep $((attempt * 5))
  done
  if [ "$http_code" != "207" ]; then
    echo "batch failed after retries with HTTP $http_code: ${response%$'\n'*}" >&2
    exit 1
  fi
  if command -v jq >/dev/null 2>&1; then
    local batch_errors
    batch_errors=$(printf '%s' "${response%$'\n'*}" | jq '[.results[] | select(.status == "error")] | length')
    errors=$((errors + batch_errors))
  fi
  sent=$((sent + count))
  rm -f "$batch_file.body"
  : > "$batch_file"
  echo "imported $sent records (errors so far: $errors)"
}

count=0
while IFS= read -r line; do
  [ -n "$line" ] || continue
  printf '%s\n' "$line" >> "$batch_file"
  count=$((count + 1))
  if [ "$count" -ge "$BATCH_SIZE" ]; then
    flush "$count"
    count=0
  fi
done < <(reader | skipper | limiter)
flush "$count"

echo "done: $sent records submitted, $errors rejected"
if [ "$errors" -gt 0 ]; then
  echo "note: rejected records are reported per batch above (conflicts, invalid keys)" >&2
fi
