#!/usr/bin/env bash
set -euo pipefail

mode=${1:-test}
package=./hat/hatSql
tests='TestMZ034SQLIncrementalProjection'
benchmarks='BenchmarkMZ034(Rebuild|Incremental)SQLProjection'

case "$mode" in
baseline)
		go test "$package" -run '^$' -bench '^BenchmarkMZ034RebuildSQLProjection$' -benchmem -count=5
		;;
format)
		gofmt -w hat/hatSql/mz034_sql_incremental_projection.go hat/hatSql/mz034_sql_incremental_projection_test.go hat/hatSql/mz034_sql_incremental_projection_baseline_test.go
		;;
	test)
		go test -tags mz034_projection "$package" -run "$tests" -count=1
		;;
benchmark)
		go test "$package" -run '^$' -bench "$benchmarks" -benchmem -count=5
		;;
package)
		go test "$package" -count=1
		;;
	race)
		go test -race -tags mz034_projection "$package" -run "$tests" -count=1
		;;
vet)
		go vet "$package"
		;;
*)
		echo "usage: $0 {baseline|format|test|benchmark|package|race|vet}" >&2
		exit 2
		;;
esac
