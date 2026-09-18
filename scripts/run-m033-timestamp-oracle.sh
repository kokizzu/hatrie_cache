#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"

case "$mode" in
  format)
    gofmt -w \
      hat/hatSql/m033_timestamp_oracle_test.go \
      hat/hatSql/m033_timestamp_oracle_baseline_benchmark_test.go \
      hat/hatSql/m033_timestamp_oracle_benchmark_test.go \
      hat/hatSql/m033_timestamp_oracle.go
    ;;
  test)
    go test ./hat/hatSql -run 'TestM033' -count=1
    ;;
  baseline)
    go test -tags m033baseline ./hat/hatSql -run '^$' -bench '^BenchmarkM033TimestampOracleBaseline$' -benchmem -count=5
    ;;
  benchmark)
    go test ./hat/hatSql -run '^$' -bench '^BenchmarkM033TimestampOracle' -benchmem -count=5
    ;;
  race)
    go test -race ./hat/hatSql -run 'TestM033' -count=1
    ;;
  vet)
    go vet ./hat/hatSql
    ;;
  package)
    go test ./hat/hatSql -count=1
    ;;
  status)
    git status --short
    git diff --stat
    git diff --check
    git diff --cached --stat
    git diff --cached --check
    ;;
  stage)
    git add \
      Makefile \
      README.md \
      BENCHMARK.md \
      CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
      INSPIRATION_BACKLOG.md \
      M033_LOGICAL_TIMESTAMP_ORACLE.md \
      ADOPTED_QUERY_ENGINE_IDEAS.md \
      hat/hatSql/m033_timestamp_oracle.go \
      hat/hatSql/m033_timestamp_oracle_test.go \
      hat/hatSql/m033_timestamp_oracle_baseline_benchmark_test.go \
      hat/hatSql/m033_timestamp_oracle_benchmark_test.go \
      scripts/run-m033-timestamp-oracle.sh
    ;;
  commit)
    git commit -m 'add batched logical timestamp oracle [skip ci]'
    ;;
  push)
    git push origin HEAD:master
    ;;
  *)
    printf 'unknown mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
