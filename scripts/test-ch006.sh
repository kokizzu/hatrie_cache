#!/usr/bin/env bash
set -euo pipefail

mode="${1:-package}"
package_path="./hat/hatSql"

case "$mode" in
red)
	go test "$package_path" -run '^TestTypedTableSparsePrimaryMarkCache' -count=1
	;;
baseline)
	temporary_directory="$(mktemp -d)"
	trap 'rm -rf "$temporary_directory"' EXIT
	git archive "${CH006_BASELINE_REVISION:-3c9ff7a1}" -o "$temporary_directory/source.tar"
	mkdir -p "$temporary_directory/source"
	tar -xf "$temporary_directory/source.tar" -C "$temporary_directory/source"
	mkdir -p "$temporary_directory/source/hat/hatSql"
	cp hat/hatSql/ch006_sparse_mark_cache_baseline_benchmark_test.go "$temporary_directory/source/hat/hatSql/"
	go test -C "$temporary_directory/source" -run '^$' -bench '^BenchmarkCH006BaselineSparsePrimaryMark' -benchmem -benchtime=20x -count=5 "$package_path"
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
		hat/hatSql/typed_table_sparse_mark_cache.go \
		hat/hatSql/ch006_sparse_mark_cache_test.go \
		hat/hatSql/ch006_sparse_mark_cache_baseline_benchmark_test.go \
		hat/hatSql/ch006_sparse_mark_cache_benchmark_test.go
	;;
check)
	git diff --check
	;;
benchmark)
	go test "$package_path" -run '^$' -bench '^BenchmarkCH006' -benchmem -benchtime=20x -count=5
	;;
*)
	printf 'usage: %s [red|baseline|package|race|vet|format|check|benchmark]\n' "$0" >&2
	exit 2
	;;
esac
