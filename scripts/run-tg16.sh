#!/usr/bin/env bash
set -euo pipefail

mode=${1:-}
case "$mode" in
  format)
    gofmt -w hat/hatDataStructure/versioned_tuple_migration.go hat/hatDataStructure/versioned_tuple_migration_test.go hat/hatDataStructure/versioned_tuple_migration_benchmark_test.go
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
    go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTR016VersionedTupleMigration$' -benchmem -count=5
    ;;
  status)
    git status --short
    git diff --stat
    git diff --check
    git diff --cached --stat
    git diff --cached --check
    ;;
  stage)
    git add Makefile scripts/run-tg16.sh hat/hatDataStructure/versioned_tuple_migration.go hat/hatDataStructure/versioned_tuple_migration_test.go hat/hatDataStructure/versioned_tuple_migration_benchmark_test.go TT016_VERSIONED_TUPLE_MIGRATIONS.md README.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
    ;;
  commit)
    git commit -m 'feat: add versioned tuple migration manager [skip ci]'
    ;;
  push)
    git push -u origin HEAD
    ;;
  *)
    printf 'usage: %s {format|test|race|vet|benchmark|status|stage|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
