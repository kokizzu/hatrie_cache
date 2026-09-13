#!/usr/bin/env bash
set -euo pipefail

mode="${1:-package}"
package_path="./hat/hatSql"

case "$mode" in
red)
	go test "$package_path" -run '^TestTypedTableStorageEvents$' -count=1
	;;
baseline)
	temporary_directory="$(mktemp -d)"
	trap 'rm -rf "$temporary_directory"' EXIT
	git archive "${CH005_BASELINE_REVISION:-3c92480c}" -o "$temporary_directory/source.tar"
	mkdir -p "$temporary_directory/source"
	tar -xf "$temporary_directory/source.tar" -C "$temporary_directory/source"
	mkdir -p "$temporary_directory/source/hat/hatSql"
	cp hat/hatSql/ch005_storage_events_baseline_benchmark_test.go "$temporary_directory/source/hat/hatSql/"
	go test -C "$temporary_directory/source" -run '^$' -bench '^BenchmarkCH005BaselineTypedTablePatchDeleteUpsert$' -benchmem -benchtime=100000x -count=5 "$package_path"
	;;
package)
	go test "$package_path" -count=1
	;;
race)
	go test -race "$package_path" -count=1
	;;
vet)
	go vet "$package_path"
	;;
format)
	gofmt -w \
		hat/hatSql/typed_table.go \
		hat/hatSql/typed_table_patch_parts.go \
		hat/hatSql/typed_table_storage_events.go \
		hat/hatSql/ch005_storage_events_test.go \
		hat/hatSql/ch005_storage_events_baseline_benchmark_test.go \
		hat/hatSql/ch005_storage_events_benchmark_test.go
	;;
check)
	git diff --check
	;;
benchmark)
	go test "$package_path" -run '^$' -bench '^BenchmarkCH005' -benchmem -benchtime=100000x -count=5
	;;
*)
	printf 'usage: %s [red|baseline|package|race|vet|format|check|benchmark]\n' "$0" >&2
	exit 2
	;;
esac
