#!/usr/bin/env bash
# Nightly-friendly backup of a registryd data directory.
#
# Produces a dated tar.gz of the whole data dir (blob store, docs store,
# record index, keys, ticket) and either keeps it in a local directory or
# hands it to rclone when the target contains a colon (remote:path).
# Restore = stop registryd, untar over the data dir, start.
#
# The daemon keeps running during the backup: both redb and iroh-blobs
# write via fsynced transactions, so a live copy is crash-consistent —
# equivalent to a power-cut snapshot, which both stores recover from.
# For a byte-perfect backup, stop the daemon first (systemctl stop
# registryd), at the cost of a short read-serving gap.
#
# Usage:
#   scripts/backup.sh [--data-dir DIR] [--keep N] TARGET
#   TARGET: a local directory, or an rclone remote like "box:registryd-backups"
set -euo pipefail

DATA_DIR="/var/lib/registryd"
KEEP=7
TARGET=""

while [ $# -gt 0 ]; do
  case "$1" in
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    --keep) KEEP="$2"; shift 2 ;;
    -h|--help) sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) echo "unknown option: $1" >&2; exit 2 ;;
    *) TARGET="$1"; shift ;;
  esac
done

[ -n "$TARGET" ] || { echo "usage: $0 [--data-dir DIR] [--keep N] TARGET" >&2; exit 2; }
[ -d "$DATA_DIR" ] || { echo "no such data dir: $DATA_DIR" >&2; exit 2; }

# Print all but the newest $KEEP names from stdin (portable: GNU head's
# negative -n is not available on BSD/macOS).
prune_candidates() {
  sort | awk -v keep="$KEEP" '{lines[NR]=$0} END {for (i = 1; i <= NR - keep; i++) print lines[i]}'
}

STAMP=$(date -u +%Y%m%d-%H%M%S)
NAME="registryd-backup-$STAMP.tar.gz"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# Ask the kernel to flush filesystem buffers before copying.
sync

tar -czf "$WORK/$NAME" -C "$(dirname "$DATA_DIR")" "$(basename "$DATA_DIR")"
SIZE=$(du -h "$WORK/$NAME" | cut -f1)

case "$TARGET" in
  *:*)
    command -v rclone >/dev/null 2>&1 || { echo "rclone not installed" >&2; exit 1; }
    rclone copy "$WORK/$NAME" "$TARGET"
    echo "uploaded $NAME ($SIZE) to $TARGET"
    # Prune old remote backups beyond --keep.
    rclone lsf "$TARGET" --files-only | grep '^registryd-backup-.*\.tar\.gz$' | prune_candidates | \
      while IFS= read -r old; do rclone deletefile "$TARGET/$old" && echo "pruned $old"; done
    ;;
  *)
    mkdir -p "$TARGET"
    mv "$WORK/$NAME" "$TARGET/"
    echo "wrote $TARGET/$NAME ($SIZE)"
    ls -1 "$TARGET" | grep '^registryd-backup-.*\.tar\.gz$' | prune_candidates | \
      while IFS= read -r old; do rm -f "$TARGET/$old" && echo "pruned $old"; done
    ;;
esac
