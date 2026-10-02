#!/usr/bin/env bash
set -euo pipefail

source_files=(
  ./hat/hatStorage/remote_part_cache.go
  ./hat/hatStorage/remote_part.go
  ./hat/hatStorage/remote_part_checksum.go
  ./hat/hatStorage/ch019_remote_part_checksum_test.go
  ./hat/hatStorage/ch019_remote_part_checksum_benchmark_test.go
)

case "${1:-test}" in
  format)
    gofmt -w "${source_files[@]}"
    ;;
  test)
    go test "${source_files[@]}"
    ;;
  package)
    go test ./hat/hatStorage
    ;;
  race)
    go test -race "${source_files[@]}"
    ;;
  vet)
    go vet "${source_files[@]}"
    ;;
  benchmark)
    go test "${source_files[@]}" -run '^$' -bench '^BenchmarkCH019RemotePartCache' -benchmem -count=5
    ;;
  *)
    printf 'usage: %s {format|test|package|race|vet|benchmark}\n' "$0" >&2
    exit 2
    ;;
esac
