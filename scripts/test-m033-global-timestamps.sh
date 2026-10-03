#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  race)
    go test -race ./hat/hatReplication -run 'GlobalTimestampOracle(FileStore|SnapshotBinary)' -count=1
    ;;
  '')
    go test ./hat/hatReplication -run 'GlobalTimestampOracle(FileStore|SnapshotBinary)' -count=1
    ;;
  *)
    printf 'usage: %s [race]\n' "$0" >&2
    exit 2
    ;;
esac
