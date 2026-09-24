#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
cache="${TMPDIR:-/tmp}/hatrie-cache-c237-gocache"
rm -rf "$cache"
mkdir -p "$cache"
trap 'rm -rf "$cache"' EXIT

case "$mode" in
  test)
    GOCACHE="$cache" go test ./hat/hatSql -run '^TestC237ProjectionExplainReportsSelectionAndEstimatedIO$' -count=1
    ;;
  benchmark)
    GOCACHE="$cache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkC237ProjectionExplain$' -benchmem -count=5
    ;;
  format)
    gofmt -w hat/hatSql/model.go hat/hatSql/materialized.go hat/hatSql/query.go hat/hatSql/result_cache.go hat/hatSql/explain_dataflow.go hat/hatSql/c237_projection_explain_test.go hat/hatSql/c237_projection_explain_benchmark_test.go
    ;;
  test-package)
    GOCACHE="$cache" go test ./hat/hatSql
    ;;
  race)
    GOCACHE="$cache" go test -race ./hat/hatSql -run '^TestC237ProjectionExplainReportsSelectionAndEstimatedIO$' -count=1
    ;;
  vet)
    GOCACHE="$cache" go vet ./hat/hatSql
    ;;
  docs)
    test -f C237_PROJECTION_EXPLAIN.md
    rg -n 'C237_PROJECTION_EXPLAIN.md|c237-projection-selection-explain|C237' README.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION_ROUND2.md BENCHMARK.md C237_PROJECTION_EXPLAIN.md
    ;;
  status)
    git diff --check
    git status --short
    git diff --stat
    ;;
  commit)
    git diff --check
    git add Makefile README.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION_ROUND2.md BENCHMARK.md C237_PROJECTION_EXPLAIN.md hat/hatSql/model.go hat/hatSql/materialized.go hat/hatSql/query.go hat/hatSql/result_cache.go hat/hatSql/explain_dataflow.go hat/hatSql/c237_projection_explain_test.go hat/hatSql/c237_projection_explain_benchmark_test.go scripts/run-c237.sh
    git commit -m 'hatSql: explain projection selection'
    ;;
  push)
    git push origin HEAD:master
    ;;
  *)
    printf 'unsupported C237 mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
