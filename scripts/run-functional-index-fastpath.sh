#!/usr/bin/env bash
set -euo pipefail

mode=${1:-test}
test_files=(
  hat/hatDataStructure/functional_index.go
  hat/hatDataStructure/functional_index_test.go
  hat/hatDataStructure/functional_index_benchmark_test.go
)

case "$mode" in
  test)
    go test "${test_files[@]}" -run '^TestFunctionalIndexPostingLifecycleKeepsEntriesInSync$' -count=1
    ;;
  benchmark)
    go test "${test_files[@]}" -run '^$' -bench '^BenchmarkFunctionalIndexLookupInto$' -benchmem -count=5
    ;;
  full)
    go test ./hat/hatDataStructure -count=1
    ;;
  race)
    go test -race "${test_files[@]}" -run '^TestFunctionalIndex' -count=1
    ;;
  format)
    gofmt -w "${test_files[@]}" hat/hatDataStructure/functional_index.go
    ;;
  *)
    printf 'usage: %s {test|benchmark|full|race}\n' "$0" >&2
    exit 2
    ;;
esac
