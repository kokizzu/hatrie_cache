#!/usr/bin/env bash
set -euo pipefail

mode=${1:-test}
test_files=(
  hat/hatDataStructure/functional_index.go
  hat/hatDataStructure/hash_index.go
  hat/hatDataStructure/hash_index_test.go
  hat/hatDataStructure/hash_index_benchmark_test.go
  hat/hatDataStructure/hash_index_adaptive_test.go
)

case "$mode" in
  test)
    go test "${test_files[@]}" -run '^TestHashIndex' -count=1
    ;;
  benchmark)
    go test "${test_files[@]}" -run '^$' -bench '^BenchmarkHashIndexAdaptive' -benchmem -count=5
    ;;
  race)
    go test -race "${test_files[@]}" -run '^TestHashIndex' -count=1
    ;;
  format)
    gofmt -w "${test_files[@]}"
    ;;
  full)
    go test ./hat/hatDataStructure -count=1
    ;;
  *)
    printf 'usage: %s {test|benchmark|race|format|full}\n' "$0" >&2
    exit 2
    ;;
esac
