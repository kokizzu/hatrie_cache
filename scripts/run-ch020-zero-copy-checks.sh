#!/usr/bin/env bash
set -euo pipefail

source_files=(
  ./hat/hatStorage/remote_part_cache.go
  ./hat/hatStorage/remote_part.go
  ./hat/hatStorage/remote_part_checksum.go
  ./hat/hatStorage/ch020_zero_copy_test.go
  ./hat/hatStorage/ch020_zero_copy_benchmark_test.go
)

case "${1:-test}" in
  format)
    gofmt -w "${source_files[@]}"
    ;;
  test)
    go test "${source_files[@]}"
    ;;
  race)
    go test -race "${source_files[@]}"
    ;;
  vet)
    go vet "${source_files[@]}"
    ;;
  benchmark)
    go test "${source_files[@]}" -run '^$' -bench '^BenchmarkCH020RemotePartCache' -benchmem -count=5
    ;;
  package)
    go test ./hat/hatStorage
    ;;
  *)
    printf 'usage: %s {format|test|race|vet|benchmark|package}\n' "$0" >&2
    exit 2
    ;;
esac
