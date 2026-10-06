#!/usr/bin/env bash
set -euo pipefail

test_pattern='^TestCHU05ExternalPartitionedRunningWindowsUseBoundedStreamingState$'

case "${1:-test}" in
red)
	set +e
	go test ./hat/hatSql -run "$test_pattern" -count=1
	status=$?
	set -e
	if [[ "$status" -eq 0 ]]; then
		printf '%s\n' 'expected the new partitioned-window test to fail before implementation' >&2
		exit 1
	fi
	printf 'red test failed as expected with status %s\n' "$status"
	;;
  format)
    gofmt -w hat/hatSql/query.go hat/hatSql/chu05_external_partitioned_window_test.go hat/hatSql/chu05_external_partitioned_window_benchmark_test.go
	;;
test)
	go test ./hat/hatSql -run "$test_pattern" -count=1
	;;
  package)
    go test ./hat/hatSql -count=1
    ;;
  race)
    go test -race ./hat/hatSql -run "$test_pattern" -count=1
    ;;
  race-package)
    go test -race ./hat/hatSql -count=1
    ;;
  vet)
    go vet ./hat/hatSql
    ;;
baseline)
	GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHU05ExternalPartitionedWindowMaterialized$' -benchmem -benchtime=200ms -count=5
	;;
benchmark)
	GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHU05ExternalPartitionedWindow' -benchmem -benchtime=200ms -count=5
	;;
*)
	printf 'usage: %s red|format|test|package|baseline|benchmark\n' "$0" >&2
	exit 2
	;;
esac
