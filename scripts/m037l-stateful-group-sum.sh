#!/usr/bin/env bash
set -euo pipefail

mode=${1:-check}
package=./hat/hatSql
cache_dir=$(mktemp -d /tmp/hatrie-cache-m037l-gocache.XXXXXX)
parked_dir=
parked_test=
restore_baseline_test() {
	if [[ -n "$parked_test" && -f "$parked_test" ]]; then
		mv "$parked_test" hat/hatSql/m037l_stateful_group_sum_test.go
	fi
	if [[ -n "$parked_dir" ]]; then
		rm -rf "$parked_dir"
	fi
}
cleanup() {
	restore_baseline_test
	rm -rf "$cache_dir"
}
trap cleanup EXIT
export GOCACHE="$cache_dir"

case "$mode" in
baseline)
	parked_dir=$(mktemp -d /tmp/hatrie-cache-m037l-baseline.XXXXXX)
	parked_test="$parked_dir/m037l_stateful_group_sum_test.go"
	mv hat/hatSql/m037l_stateful_group_sum_test.go "$parked_test"
	go test "$package" -run '^$' -bench '^BenchmarkM037LSumFullHistoryRebuild$' -benchmem -count=5 -benchtime=300ms
	;;
format)
	gofmt -w hat/hatSql/m037l_stateful_group_sum.go hat/hatSql/m037l_stateful_group_sum_benchmark_test.go hat/hatSql/m037l_stateful_group_sum_test.go
	;;
test)
	go test "$package" -run '^(TestM037L|ExampleNewIncrementalGroupSumInt64)' -count=1
	;;
benchmark)
	go test "$package" -run '^$' -bench '^BenchmarkM037L' -benchmem -count=5 -benchtime=300ms | tee M037L_BENCHMARK_RAW.txt
	;;
race)
	go test -race "$package" -run '^(TestM037L|ExampleNewIncrementalGroupSumInt64)' -count=1
	;;
vet)
	go vet "$package"
	;;
package-test)
	go test "$package"
	;;
check)
	gofmt -d hat/hatSql/m037l_stateful_group_sum.go hat/hatSql/m037l_stateful_group_sum_benchmark_test.go hat/hatSql/m037l_stateful_group_sum_test.go
	go test "$package" -run '^(TestM037L|ExampleNewIncrementalGroupSumInt64)' -count=1
	go test -race "$package" -run '^(TestM037L|ExampleNewIncrementalGroupSumInt64)' -count=1
	go vet "$package"
	git diff --check
	;;
status)
	git status --short
	;;
commit)
	git add hat/hatSql/m037l_stateful_group_sum.go hat/hatSql/m037l_stateful_group_sum_test.go hat/hatSql/m037l_stateful_group_sum_benchmark_test.go M037L_STATEFUL_GROUP_SUM.md M037L_BENCHMARK_RAW.txt BENCHMARK.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md scripts/m037l-stateful-group-sum.sh Makefile
	git commit -m 'feat(hatSql): add stateful differential group sum [skip ci]'
	;;
push)
	git push origin HEAD
	;;
*)
	printf 'usage: %s {baseline|format|test|benchmark|race|vet|package-test|check|status|commit|push}\n' "$0" >&2
	exit 2
	;;
esac
