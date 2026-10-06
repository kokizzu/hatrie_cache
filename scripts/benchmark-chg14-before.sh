#!/usr/bin/env bash
set -euo pipefail

count=${BENCH_COUNT:-5}
red_test=hat/hatSql/chg14_plan_cache_metrics_test.go
disabled_test=${red_test}.disabled
mv "$red_test" "$disabled_test"
restore_red_test() {
	mv "$disabled_test" "$red_test"
}
trap restore_red_test EXIT
go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLPreparedQueryCache(Hit|NormalizedAliasHit|VersionedHit)$' -benchmem -count="$count"
