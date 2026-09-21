#!/usr/bin/env bash
set -euo pipefail

mode=${1:?mode is required}

case "$mode" in
format)
	gofmt -w \
		hat/hatSql/mz009_validity_index.go \
		hat/hatSql/mz009_validity_index_benchmark_test.go \
		hat/hatSql/mz009_validity_index_test.go
	;;
test)
	go test ./hat/hatSql -run '^(TestMZ009|TestSQLTemporalValidity)' -count=1
	;;
benchmark)
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ009ValidityIndex' -benchmem -count=5
	;;
race)
	go test -race ./hat/hatSql -run '^(TestMZ009|TestSQLTemporalValidity)' -count=1
	;;
vet)
	go vet ./hat/hatSql
	;;
verify)
	bash "$0" test
	bash "$0" race
	bash "$0" vet
	;;
*)
	echo "unknown mode: $mode" >&2
	exit 2
	;;
esac
