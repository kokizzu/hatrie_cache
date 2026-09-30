#!/usr/bin/env bash
set -euo pipefail

test_pattern='TestRowIteratorNextIntoReusesAndClearsMap'
benchmark_pattern='^BenchmarkRowIterator(Next|NextInto)$'

case "${1:-}" in
  format)
    gofmt -w hat/hatSql/client.go hat/hatSql/client_reuse_test.go
    ;;
  test)
    go test ./hat/hatSql -run "$test_pattern" -count=1
    ;;
  race)
    go test -race ./hat/hatSql -run "$test_pattern" -count=1
    ;;
  bench)
    go test ./hat/hatSql -run '^$' -bench "$benchmark_pattern" -benchmem -count=5
    ;;
  vet)
    go vet ./hat/hatSql
    ;;
  package)
    go test ./hat/hatSql -count=1
    ;;
  status)
    git diff --check
    git status --short
    git diff --stat -- \
      hat/hatSql/client.go \
      hat/hatSql/client_reuse_test.go \
      scripts/c191-reusable-query-iterator.sh \
      ROW_ITERATOR_NEXT_INTO.md \
      INSPIRATION.md \
      BENCHMARK.md \
      Makefile
    ;;
  commit)
    git add \
      hat/hatSql/client.go \
      hat/hatSql/client_reuse_test.go \
      scripts/c191-reusable-query-iterator.sh \
      ROW_ITERATOR_NEXT_INTO.md \
      INSPIRATION.md \
      BENCHMARK.md \
      Makefile
    git commit -m 'feat(sql): reuse NDJSON iterator rows [skip ci]'
    ;;
  push)
    git push origin HEAD
    ;;
  *)
    printf 'usage: %s format|test|race|bench|vet|package|status|commit|push\n' "$0" >&2
    exit 2
    ;;
esac
