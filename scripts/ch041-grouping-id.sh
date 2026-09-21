#!/usr/bin/env bash
set -euo pipefail

mode=${1:?mode is required}

case "$mode" in
format)
	gofmt -w \
		hat/hatSql/ch041_grouping_id_benchmark_test.go \
		hat/hatSql/grouping_identifier_test.go \
		hat/hatSql/grouping_sets.go \
		hat/hatSql/query.go
	;;
test)
	go test ./hat/hatSql -run '^TestSQLGrouping' -count=1
	;;
benchmark)
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH041GroupingID' -benchmem -count=5
	;;
race)
	go test -race ./hat/hatSql -run '^TestSQLGrouping' -count=1
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
