#!/usr/bin/env bash
set -euo pipefail

action=${1:?action required}
export GOCACHE=${GOCACHE:-"$PWD/.gocache"}

case "$action" in
format)
	gofmt -w \
		hat/hatSql/c213_compiled_plan_cache.go \
		hat/hatSql/compiled_negative_cache_benchmark_test.go \
		hat/hatSql/compiled_negative_cache_test.go
	;;
test)
	go test ./hat/hatSql -run 'TestSQLCompiledQueryCacheNegative'
	;;
full)
	go test ./hat/hatSql
	;;
race)
	go test -race ./hat/hatSql
	;;
vet)
	go vet ./hat/hatSql
	;;
benchmark)
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLCompiledQueryCacheRepeated' -benchmem -count=5
	;;
*)
	echo "unknown action: $action" >&2
	exit 2
	;;
esac
