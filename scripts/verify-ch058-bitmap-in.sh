#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
cache="${TMPDIR:-/tmp}/hatrie-cache-round61-ch058-${mode}-gocache"
rm -rf "$cache"
trap 'rm -rf "$cache"' EXIT

case "$mode" in
  test)
    GOCACHE="$cache" go test ./hat/hatCache ./hat/hatSql -count=1
    ;;
  all)
    GOCACHE="$cache" go test ./... -count=1
    ;;
  race)
    GOCACHE="$cache" go test -race ./hat/hatCache ./hat/hatSql -count=1
    ;;
  race-focused)
    GOCACHE="$cache" go test -race ./hat/hatCache -run 'TestSQLBitmapIndexBatchINResolver' -count=1
    ;;
  vet)
    GOCACHE="$cache" go vet ./hat/hatCache ./hat/hatSql
    ;;
  *)
    printf 'usage: %s test|all|race|vet\n' "$0" >&2
    exit 2
    ;;
esac
