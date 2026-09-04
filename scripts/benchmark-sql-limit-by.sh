#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
case ${LIMIT_BY_BENCH_MODE:-baseline} in
all)
	bench='^BenchmarkSQLLimitBy/(baseline|limit_by)$'
	;;
candidate)
	bench='^BenchmarkSQLLimitBy/limit_by$'
	;;
baseline)
	bench='^BenchmarkSQLLimitBy/baseline$'
	;;
*)
	bench=${LIMIT_BY_BENCH:-'^BenchmarkSQLLimitBy/baseline$'}
	;;
esac
benchtime=${LIMIT_BY_BENCHTIME:-300ms}
go test ./hat/hatSql -run '^$' -bench "$bench" -benchmem -benchtime "$benchtime" -count=5
