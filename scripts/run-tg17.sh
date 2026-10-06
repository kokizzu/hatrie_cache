#!/usr/bin/env bash
set -euo pipefail

mode=${1:-}
case "$mode" in
  format)
    gofmt -w hat/hatDataStructure/versioned_tuple_space.go hat/hatDataStructure/versioned_tuple_space_test.go hat/hatDataStructure/versioned_tuple_space_benchmark_test.go
    ;;
  test)
    go test ./hat/hatDataStructure -count=1
    ;;
  race)
    go test -race ./hat/hatDataStructure -count=1
    ;;
  vet)
    go vet ./hat/hatDataStructure
    ;;
  benchmark)
    go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTT017VersionedTupleSpaceUpgrade$' -benchmem -count=5
    ;;
  status)
    git status --short
    ;;
  stage)
    git add Makefile README.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md \
      TT017_VERSIONED_TUPLE_SPACE_UPGRADES.md \
      hat/hatDataStructure/versioned_tuple_migration.go \
      hat/hatDataStructure/versioned_tuple_space.go \
      hat/hatDataStructure/versioned_tuple_space_test.go \
      hat/hatDataStructure/versioned_tuple_space_benchmark_test.go \
      scripts/run-tg17.sh
    git diff --cached --check
    ;;
  commit)
    git diff --cached --check
    git commit -m 'feat: add online versioned tuple-space upgrades [skip ci]'
    ;;
  push)
    git push -u origin HEAD
    ;;
  *)
    printf 'usage: %s {format|test|race|vet|benchmark|status|stage|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
