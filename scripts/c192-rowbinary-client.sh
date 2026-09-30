#!/usr/bin/env bash
set -euo pipefail

test_pattern='TestConnQueryRowBinary'
benchmark_pattern='^BenchmarkSQLClient(NDJSONRows|RowBinaryRows)$'

case "${1:-}" in
  format)
    gofmt -w hat/hatSql/client.go hat/hatSql/client_row_binary_test.go hat/hatCache/sql_query.go
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
  compile)
    go test ./hat/hatCache -run '^$'
    ;;
  package)
    go test ./hat/hatSql -count=1
    ;;
  status)
    git diff --check
    git status --short
    git diff --stat -- \
      hat/hatSql/client.go \
      hat/hatSql/client_row_binary_test.go \
      hat/hatCache/sql_query.go \
      scripts/c192-rowbinary-client.sh \
      SQL_ROWBINARY_CLIENT.md \
      INSPIRATION.md \
      BENCHMARK.md \
      Makefile
    ;;
  commit)
    git add \
      hat/hatSql/client.go \
      hat/hatSql/client_row_binary_test.go \
      hat/hatCache/sql_query.go \
      scripts/c192-rowbinary-client.sh \
      SQL_ROWBINARY_CLIENT.md \
      INSPIRATION.md \
      BENCHMARK.md \
      Makefile
    git commit -m 'feat(sql): add RowBinary client stream [skip ci]'
    ;;
  push)
    git push origin HEAD
    ;;
  *)
    printf 'usage: %s format|test|race|bench|vet|compile|package|status|commit|push\n' "$0" >&2
    exit 2
    ;;
esac
