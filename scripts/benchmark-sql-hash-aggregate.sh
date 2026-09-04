#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
mode=${HASH_AGGREGATE_BENCH_MODE:-baseline}
case "$mode" in
all)
	bench='^BenchmarkSQLHashGroupAggregate/(baseline|hash)$'
	;;
hash)
	bench='^BenchmarkSQLHashGroupAggregate/hash$'
	;;
baseline)
	bench='^BenchmarkSQLHashGroupAggregate/baseline$'
	;;
*)
	echo "unknown HASH_AGGREGATE_BENCH_MODE: $mode" >&2
	exit 2
	;;
esac
go test ./hat/hatSql -run '^$' -bench "$bench" -benchmem -count=5
