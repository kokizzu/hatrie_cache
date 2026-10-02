#!/usr/bin/env bash
set -euo pipefail

source_files=(
  ./hat/hatStorage/compaction_merge_selector.go
  ./hat/hatStorage/ch026_merge_selector_test.go
  ./hat/hatStorage/ch026_merge_selector_benchmark_test.go
)

case "${1:-}" in
  format)
    gofmt -w "${source_files[@]}"
    ;;
  test)
    go test "${source_files[@]}" -run 'TestCH026' -count=1
    ;;
  race)
    go test -race "${source_files[@]}" -run 'TestCH026' -count=1
    ;;
  vet)
    go vet "${source_files[@]}"
    ;;
  benchmark)
    go test "${source_files[@]}" -run '^$' -bench 'BenchmarkCH026' -benchmem -count=5
    ;;
  package)
    go test ./hat/hatStorage
    ;;
  *)
    printf 'usage: %s {format|test|race|vet|benchmark|package}\n' "$0" >&2
    exit 2
    ;;
esac
