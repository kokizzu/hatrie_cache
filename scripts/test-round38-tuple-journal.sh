#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  baseline)
    go test ./hat/hatDataStructure -count=1
    ;;
  benchmark-baseline)
    go test ./hat/hatDataStructure -run '^$' -bench 'BenchmarkTupleFieldUpdate/apply-fixed-width' -benchmem -count=5
    ;;
  feature)
    go test ./hat/hatDataStructure -run 'TestTU19' -count=1
    ;;
  benchmark-feature)
    go test ./hat/hatDataStructure -run '^$' -bench 'BenchmarkTU19TupleFieldOperationJournal' -benchmem -benchtime=10x -count=5
    ;;
  *)
    printf 'usage: %s baseline|benchmark-baseline|feature\n' "$0" >&2
    exit 2
    ;;
esac
