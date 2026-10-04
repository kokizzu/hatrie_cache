#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

case "${1:-}" in
  format)
    gofmt -w hat/hatSort/*.go
    ;;
  test)
    go test ./hat/hatSort
    ;;
  race)
    go test -race ./hat/hatSort
    ;;
  vet)
    go vet ./hat/hatSort
    ;;
  benchmark)
    go test ./hat/hatSort -run '^$' -bench 'CHG02' -benchmem -count="${BENCH_COUNT:-5}"
    ;;
  *)
    printf 'usage: %s {format|test|race|vet|benchmark}\n' "$0" >&2
    exit 2
    ;;
esac
