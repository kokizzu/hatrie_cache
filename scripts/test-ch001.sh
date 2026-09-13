#!/usr/bin/env bash
set -euo pipefail

mode=${1:-package}
case "$mode" in
red)
	go test ./hat/hatSql -run 'TestSQLNamedSettingsProfiles'
	;;
baseline)
	temporary_directory=$(mktemp -d)
	trap 'rm -rf "$temporary_directory"' EXIT
	git archive "${CH001_BASELINE_REVISION:-fd07242ca9ca1cc32be7e111b53bb06b2a4e14db}" -o "$temporary_directory/source.tar"
	tar -xf "$temporary_directory/source.tar" -C "$temporary_directory"
	cp hat/hatSql/ch001_named_settings_baseline_benchmark_test.go "$temporary_directory/hat/hatSql/"
	cd "$temporary_directory"
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH001Baseline' -benchmem -count=5
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
	gofmt -w hat/hatSql/named_settings.go hat/hatSql/ch001_named_settings_profiles_test.go hat/hatSql/ch001_named_settings_baseline_benchmark_test.go
	;;
check)
	git diff --check
	git diff --cached --check
	;;
benchmark)
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH001' -benchmem -count=5
	;;
*)
	echo "usage: $0 {red|baseline|package|race|vet|format|check|benchmark}" >&2
	exit 2
	;;
esac
