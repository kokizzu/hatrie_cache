#!/usr/bin/env bash
set -euo pipefail

source_files=(
  ./hat/hatStorage/remote_part.go
  ./hat/hatStorage/remote_part_attachment.go
  ./hat/hatStorage/ch024_detach_test.go
  ./hat/hatStorage/ch024_detach_benchmark_test.go
)

case "${1:-}" in
  format)
    gofmt -w "${source_files[@]}"
    ;;
  test)
    go test "${source_files[@]}" -run 'TestCH024' -count=1
    ;;
  race)
    go test -race "${source_files[@]}" -run 'TestCH024' -count=1
    ;;
  vet)
    go vet "${source_files[@]}"
    ;;
  benchmark)
    go test "${source_files[@]}" -run '^$' -bench 'BenchmarkCH024' -benchmem -count=5
    ;;
  package)
    go test ./hat/hatStorage
    ;;
  *)
    printf 'usage: %s {format|test|race|vet|benchmark|package}\n' "$0" >&2
    exit 2
    ;;
esac
