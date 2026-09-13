#!/usr/bin/env bash
set -euo pipefail

mode=${1:-unit}
case "$mode" in
red)
	go test ./hat/hatSql -run '^TestSQLMaxIntermediateRows' -count=1
	;;
unit)
	go test ./hat/hatSql -run '^TestSQLMaxIntermediateRows' -count=1
	;;
package)
	go test ./hat/hatSql -count=1
	;;
race)
	go test -race ./hat/hatSql -run '^TestSQLMaxIntermediateRows' -count=1
	;;
vet)
	go vet ./hat/hatSql
	;;
bench)
    go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLIntermediateRows(Baseline|Guarded)$' -benchmem -count=5
	;;
format)
    gofmt -w hat/hatSql/intermediate_rows_test.go hat/hatSql/intermediate_rows_baseline_benchmark_test.go hat/hatSql/intermediate_rows_guard_benchmark_test.go hat/hatSql/query.go hat/hatSql/sql_result_cache.go hat/hatSql/governance.go
	;;
*)
	printf 'unknown c232 test mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
