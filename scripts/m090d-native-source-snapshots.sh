#!/usr/bin/env bash
set -euo pipefail

gocache=$(mktemp -d /tmp/hatrie-m090d-go-cache.XXXXXX)
trap 'rm -rf -- "$gocache"' EXIT

case "${1:-}" in
  full)
    GOCACHE="$gocache" go test ./...
    ;;
  test)
    GOCACHE="$gocache" go test ./hat/hatSql ./hat/hatCache -run 'M090d|TestExecuteSQLQueryUsesOneSnapshotForRepeatedSources' -count=1
    ;;
  race)
    GOCACHE="$gocache" go test -race ./hat/hatSql ./hat/hatCache -run 'M090d|TestExecuteSQLQueryUsesOneSnapshotForRepeatedSources' -count=1
    ;;
  vet)
    GOCACHE="$gocache" go vet ./hat/hatSql ./hat/hatCache
    ;;
  benchmark)
    GOCACHE="$gocache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkM090dRepeatedSourceJoin$' -benchmem -count=5
    ;;
  *)
    printf 'usage: %s full|test|race|vet|benchmark\n' "$0" >&2
    exit 2
    ;;
esac
