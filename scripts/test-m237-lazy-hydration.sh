#!/usr/bin/env bash
set -euo pipefail

case "${1:-test}" in
  test)
    go test ./hat/hatSql -run '^TestM237GetOrHydrate' -count=1
    ;;
  package)
    go test ./hat/hatSql -count=1
    ;;
  race)
    go test -race ./hat/hatSql -run '^TestM237GetOrHydrate' -count=1
    ;;
  vet)
    go vet ./hat/hatSql
    ;;
  benchmark)
    go test ./hat/hatSql -run '^$' -bench '^BenchmarkM237' -benchmem -count=5 -benchtime=100ms
    ;;
  docs)
    test -s M237_LAZY_HYDRATION.md
    rg -q 'M237|GetOrHydrate|606.3' M237_LAZY_HYDRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md INSPIRATION_ROUND2.md
    ;;
  format)
    gofmt -w hat/hatSql/m237_lazy_hydration_test.go hat/hatSql/materialized.go hat/hatSql/materialized_hydration.go
    ;;
  *)
    echo "usage: $0 {test|package|race|vet|benchmark|docs|format}" >&2
    exit 2
    ;;
esac
