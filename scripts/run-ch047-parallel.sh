#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
  format)
    gofmt -w hat/hatSql/ch047_parallel_test.go hat/hatSql/ch047_parallel_baseline_benchmark_test.go hat/hatSql/sql_keyset_token.go
    ;;
  test)
    go test ./hat/hatSql -run '^TestCH047' -count=1
    ;;
  baseline)
    go test -tags ch047baseline ./hat/hatSql -run '^$' -bench '^BenchmarkCH047BaselineJSONEachRow$' -benchmem -count=5
    ;;
  benchmark)
    go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH047JSONEachRow' -benchmem -count=5
    ;;
  race)
    go test -race ./hat/hatSql -run '^TestCH047' -count=1
    ;;
  vet)
    go vet ./hat/hatSql
    ;;
  package)
    go test ./hat/hatSql -count=1
    ;;
  check)
    git diff --check
    ;;
  status)
    git status --short
    ;;
  stage)
    git add Makefile ENGINE_IDEAS.md INSPIRATION_BACKLOG.md BENCHMARK.md ADOPTED_QUERY_ENGINE_IDEAS.md CH047_PARALLEL_FORMAT_PARSING.md \
      hat/hatSql/ch047_parallel_test.go hat/hatSql/ch047_parallel_baseline_benchmark_test.go hat/hatSql/sql_keyset_token.go \
      scripts/select-next-inspiration.sh scripts/run-ch047-parallel.sh
    ;;
  commit)
    git commit -m 'document parallel NDJSON parsing and harden keyset tokens [skip ci]'
    ;;
  push)
    git push origin HEAD:master
    ;;
  docs)
    rg -n 'CH-047|parallel JSONEachRow|ParallelJSONEachRow' ENGINE_IDEAS.md INSPIRATION_BACKLOG.md ADOPTED_QUERY_ENGINE_IDEAS.md CH047_PARALLEL_FORMAT_PARSING.md BENCHMARK.md
    ;;
  *)
    printf 'unknown mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
