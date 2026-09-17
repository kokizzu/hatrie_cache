#!/bin/sh
set -eu

mode=${1:-test}
case "$mode" in
test)
	go test ./hat/hatCache -run 'TestSQLJSONFieldIndexStringLookup'
	;;
benchmark-legacy)
	go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLJSONFieldIndexStringLookupLegacy$' -benchmem -count=5
	;;
benchmark)
	go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLJSONFieldIndexStringLookup' -benchmem -count=5
	;;
race)
	go test -race ./hat/hatCache -run 'TestSQLJSONFieldIndexStringLookup'
	;;
vet)
	go vet ./hat/hatCache
	;;
format)
	formatted=$(gofmt -d hat/hatCache/sql_query.go hat/hatCache/sql_borrowed_index.go hat/hatCache/tt023_string_index_test.go hat/hatCache/tt023_string_index_benchmark_test.go)
	if [ -n "$formatted" ]; then
		printf '%s\n' "$formatted"
		exit 1
	fi
	;;
package)
	go test ./hat/hatCache
	;;
*)
	printf 'unknown TT-023 test mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
