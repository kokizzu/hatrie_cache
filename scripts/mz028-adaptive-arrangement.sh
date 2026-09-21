#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  test)
    go test ./hat/hatSql -run '^TestMZ028' -count=1
    ;;
  race)
    go test -race ./hat/hatSql -run '^TestMZ028' -count=1
    ;;
  aggregate)
    go test ./hat/hatSql -run 'TestMZ028|TestTypedTableAggregate' -count=1
    ;;
  package)
    go test ./hat/hatSql -count=1
    ;;
  benchmark)
    go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableAggregateDictionaryEncoding/legacy/rows_existing_state$' -benchmem -count=5
    ;;
  format)
    gofmt -w hat/hatSql/mz028_adaptive_arrangement_test.go hat/hatSql/typed_table.go
    ;;
  *)
    printf '%s\n' 'usage: scripts/mz028-adaptive-arrangement.sh {test|race|aggregate|package|benchmark|format}' >&2
    exit 2
    ;;
esac
