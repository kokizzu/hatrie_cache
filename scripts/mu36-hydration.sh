#!/usr/bin/env bash
set -euo pipefail

mode="${1:?mode is required}"
package="./hat/hatSql"

run_with_private_cache() {
	local cache_dir
	local status
	cache_dir="$(mktemp -d /tmp/hatrie-mu36-gocache.XXXXXX)"
	set +e
	GOCACHE="$cache_dir" "$@"
	status=$?
	set -e
	rm -rf "$cache_dir"
	return "$status"
}

case "$mode" in
red)
	set +e
	go test -tags mu36hydration "$package" -run '^TestTypedTable.*HydrationAdmission' -count=1
	status=$?
	set -e
	if [[ "$status" -eq 0 ]]; then
		echo "expected the pre-implementation hydration admission test to fail"
		exit 1
	fi
	;;
baseline)
	GOMAXPROCS=1 run_with_private_cache go test "$package" -run '^$' -bench '^BenchmarkMU036HydrateNoAdmission$' -benchmem -benchtime=200ms -count=5
	;;
format)
	gofmt -w hat/hatSql/typed_table_arrangement_hydration_admission.go hat/hatSql/typed_table_arrangements.go hat/hatSql/typed_table_join_arrangements.go hat/hatSql/mu036_hydration_admission_test.go hat/hatSql/mu036_hydration_benchmark_test.go
	;;
test)
	run_with_private_cache go test "$package" -run '^TestTypedTable.*HydrationAdmission' -count=1
	;;
package)
	run_with_private_cache go test "$package" -count=1
	;;
benchmark)
	GOMAXPROCS=1 run_with_private_cache go test "$package" -run '^$' -bench '^BenchmarkMU036' -benchmem -benchtime=200ms -count=5
	;;
race)
	run_with_private_cache go test -race "$package" -run '^TestTypedTable.*HydrationAdmission' -count=1
	;;
race-package)
	run_with_private_cache go test -race "$package" -count=1
	;;
vet)
	run_with_private_cache go vet "$package"
	;;
repo)
	run_with_private_cache go test ./... -count=1
	;;
*)
	echo "unknown mode: $mode" >&2
	exit 2
	;;
esac
