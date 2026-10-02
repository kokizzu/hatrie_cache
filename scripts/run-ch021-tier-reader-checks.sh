#!/usr/bin/env bash
set -euo pipefail

source_files=(
  ./hat/hatStorage/disk_placement.go
  ./hat/hatStorage/storage_tier.go
  ./hat/hatStorage/ch015_storage_tier_move.go
  ./hat/hatStorage/storage_tier_reader.go
  ./hat/hatStorage/ch021_tier_reader_test.go
  ./hat/hatStorage/ch021_tier_reader_benchmark_test.go
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
    go test "${source_files[@]}" -run '^$' -bench '^BenchmarkCH021TieredStorageReader' -benchmem -count=5
    ;;
  package)
    go test ./hat/hatStorage
    ;;
  *)
    printf 'usage: %s {format|test|race|vet|benchmark|package}\n' "$0" >&2
    exit 2
    ;;
esac
