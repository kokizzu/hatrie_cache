#!/usr/bin/env bash
set -euo pipefail

mode=${1:?expected format, test, benchmark, package, race, or vet}
cache_dir=$(mktemp -d /tmp/hatrie-cache-ch052-test.XXXXXX)
cleanup() {
  rm -rf "$cache_dir"
}
trap cleanup EXIT

case "$mode" in
  format)
    gofmt -w hat/hatSql/ch052_semi_anti_join_test.go hat/hatSql/semi_join.go
    ;;
  test)
    GOCACHE="$cache_dir" go test ./hat/hatSql -run '^TestCH052' -count=1
    ;;
  benchmark)
    GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH052NaiveSemiJoin$|^BenchmarkCH052IndexedSemiJoin$' -benchmem -count=5
    ;;
  package)
    GOCACHE="$cache_dir" go test ./hat/hatSql -count=1
    ;;
  race)
    GOCACHE="$cache_dir" go test -race ./hat/hatSql -run '^TestCH052' -count=1
    ;;
  vet)
    GOCACHE="$cache_dir" go vet ./hat/hatSql
    ;;
  *)
    printf 'unsupported mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
