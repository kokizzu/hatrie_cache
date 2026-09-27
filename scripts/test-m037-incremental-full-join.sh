#!/usr/bin/env bash
set -euo pipefail

case "${1:-test}" in
  format)
    gofmt -w hat/hatSql/m037_incremental_full_join.go hat/hatSql/m037_incremental_full_join_test.go
    ;;
  test)
    go test ./hat/hatSql -run '^TestM037IncrementalFullJoin'
    ;;
  benchmark)
    go test ./hat/hatSql -run '^$' -bench '^BenchmarkM037(RebuildFullOuterJoin|IncrementalFullJoin)$' -benchmem -count=5
    ;;
  package)
    go test ./hat/hatSql
    ;;
  race)
    go test -race ./hat/hatSql -run '^TestM037IncrementalFullJoin'
    ;;
  vet)
    go vet ./hat/hatSql
    ;;
  *)
    printf 'Usage: %s {format|test|benchmark|package|race|vet}\n' "$0" >&2
    exit 2
    ;;
esac
