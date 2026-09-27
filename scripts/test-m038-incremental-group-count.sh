#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
package_path="./hat/hatSql"
test_pattern='M038|IncrementalGroupCount'
baseline_pattern='^BenchmarkM038RebuildGroupCount$'
benchmark_pattern='^BenchmarkM038(RebuildGroupCount|IncrementalGroupCount)$'

case "$mode" in
  baseline)
    go test "$package_path" -run '^$' -bench "$baseline_pattern" -benchmem -count=5
    ;;
  format)
    gofmt -w hat/hatSql/m038_incremental_group_count.go hat/hatSql/m038_incremental_group_count_test.go
    ;;
  test)
    go test "$package_path" -run "$test_pattern" -count=1
    ;;
  benchmark)
    go test "$package_path" -run '^$' -bench "$benchmark_pattern" -benchmem -count=5
    ;;
  package)
    go test "$package_path" -count=1
    ;;
  race)
    go test -race "$package_path" -run "$test_pattern" -count=1
    ;;
  vet)
    go vet "$package_path"
    ;;
  *)
    printf 'usage: %s {baseline|format|test|benchmark|package|race|vet}\n' "$0" >&2
    exit 2
    ;;
esac
