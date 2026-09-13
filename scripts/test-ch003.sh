#!/usr/bin/env bash
set -euo pipefail

mode=${1:-package}
case "$mode" in
red)
	go test ./hat/hatSql -run 'TestCH003'
	;;
baseline)
	temporary_directory=$(mktemp -d)
	trap 'rm -rf "$temporary_directory"' EXIT
	git archive "${CH003_BASELINE_REVISION:-c059a6cb}" -o "$temporary_directory/source.tar"
	tar -xf "$temporary_directory/source.tar" -C "$temporary_directory"
	cp hat/hatSql/ch003_resource_profile_baseline_benchmark_test.go "$temporary_directory/hat/hatSql/"
	cd "$temporary_directory"
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH003Baseline' -benchmem -count=5
	;;
package)
	go test ./hat/hatSql
	;;
race)
	go test -race ./hat/hatSql
	;;
vet)
	go vet ./hat/hatSql
	;;
format)
	gofmt -w hat/hatSql/governance.go hat/hatSql/ch003_resource_profile_test.go hat/hatSql/ch003_resource_profile_baseline_benchmark_test.go hat/hatSql/ch003_resource_profile_benchmark_test.go
	;;
check)
	git diff --check
	git diff --cached --check
	;;
benchmark)
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH003' -benchmem -count=5
	;;
*)
	echo "usage: $0 {red|baseline|package|race|vet|format|check|benchmark}" >&2
	exit 2
	;;
esac
