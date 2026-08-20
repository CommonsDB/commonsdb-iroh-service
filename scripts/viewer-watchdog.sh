#!/bin/sh
# Restart a wedged registry-viewer — and nothing else.
#
# Designed to be safe to run every few minutes from launchd/cron:
#   * respects manual stops (service unloaded -> does nothing);
#   * leaves startup verification scans alone (a fresh process gets a
#     grace period before an unreachable API counts as wedged);
#   * only acts on the two stuck signatures:
#       1. API unreachable although the process has run long past startup;
#       2. sync loop frozen: last pass older than STALE_MIN while work
#          is pending.
#
# macOS/launchd version. Usage: viewer-watchdog.sh [port]
# Install (every 5 min): a launchd plist with StartInterval=300 running
# this script — see docs/reader-node-operations.md.

PORT="${1:-8090}"
LABEL="com.commonsdb.registry-viewer"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
STARTUP_GRACE_MIN=45
STALE_MIN=30

# Manual stop? Then it is supposed to be down.
launchctl print "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || exit 0

PID=$(pgrep -x registry-viewer | head -1)
[ -z "$PID" ] && exit 0  # launchd's KeepAlive handles plain crashes

elapsed_min() {
    # etime formats: MM:SS, HH:MM:SS, D-HH:MM:SS
    ps -o etime= -p "$1" | tr -d ' ' | awk -F'[-:]' \
        '{ if (NF==4) print $1*1440+$2*60+$3; else if (NF==3) print $1*60+$2; else print $1 }'
}

STATUS=$(curl -s -m 5 "http://127.0.0.1:$PORT/api/partitions" 2>/dev/null)
if [ -z "$STATUS" ]; then
    # API down. Startup scan? Give a fresh process its grace period.
    [ "$(elapsed_min "$PID")" -lt "$STARTUP_GRACE_MIN" ] && exit 0
    REASON="api unreachable after ${STARTUP_GRACE_MIN}m of runtime"
else
    REASON=$(printf '%s' "$STATUS" | python3 -c "
import json, sys, datetime
d = json.load(sys.stdin)
work = d['behind'] + d['syncing'] + len(d.get('pending') or [])
last = d.get('last_pass_at')
if not work or not last:
    sys.exit(0)  # idle or not yet measurable: healthy
age = (datetime.datetime.now(datetime.timezone.utc)
       - datetime.datetime.fromisoformat(last.replace('Z', '+00:00'))).total_seconds() / 60
if age > $STALE_MIN:
    print(f'sync loop frozen: last pass {age:.0f}m ago with {work} partitions pending')
" 2>/dev/null)
    [ -z "$REASON" ] && exit 0
fi

logger -t viewer-watchdog "restarting registry-viewer: $REASON"
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null
sleep 20
n=0
while [ $n -lt 6 ]; do
    launchctl bootstrap "gui/$(id -u)" "$PLIST" 2>/dev/null && break
    n=$((n + 1)); sleep 5
done
