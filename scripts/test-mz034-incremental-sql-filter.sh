#!/usr/bin/env bash
set -euo pipefail

mode=${1:-test}
package=./hat/hatSql
test_pattern='MZ034|SQLIncrementalFilter'
baseline_pattern='^BenchmarkMZ034RebuildSQLFilter$'
benchmark_pattern='^BenchmarkMZ034(RebuildSQLFilter|IncrementalSQLFilter)$'

case "$mode" in
  baseline)
    go test "$package" -run '^$' -bench "$baseline_pattern" -benchmem -count=5
    ;;
  format)
    gofmt -w hat/hatSql/mz034_sql_incremental_filter.go hat/hatSql/mz034_sql_incremental_filter_test.go
    ;;
  test)
    go test "$package" -run "$test_pattern" -count=1
    ;;
  benchmark)
    go test "$package" -run '^$' -bench "$benchmark_pattern" -benchmem -count=5
    ;;
  package)
    go test "$package" -count=1
    ;;
  race)
    go test -race "$package" -run "$test_pattern" -count=1
    ;;
  vet)
    go vet "$package"
    ;;
  *)
    printf 'usage: %s {baseline|format|test|benchmark|package|race|vet}\n' "$0" >&2
    exit 2
    ;;
esac
