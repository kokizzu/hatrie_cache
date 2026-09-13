#!/usr/bin/env bash
set -euo pipefail

mode="${1:-package}"
package_path="./hat/hatSql"

case "$mode" in
red)
	go test "$package_path" -run 'Test(TypedTableDecompressedBlockCache|ColumnarDecompressedBlockCache)' -count=1
	;;
baseline)
	tmp_dir="$(mktemp -d)"
	trap 'rm -rf "$tmp_dir"' EXIT
	git archive "${CH007_BASELINE_REVISION:-4932bae2}" | tar -x -C "$tmp_dir"
	mkdir -p "$tmp_dir/hat/hatSql"
	cp hat/hatSql/ch007_decompressed_block_cache_baseline_benchmark_test.go "$tmp_dir/hat/hatSql/"
	go test -C "$tmp_dir" "$package_path" -run '^$' -bench '^BenchmarkCH007BaselineDecompressedColumnBlockQuery$' -benchmem -benchtime=20x -count=5
	;;
package)
	go test "$package_path" -run 'Test(TypedTableDecompressedBlockCache|ColumnarDecompressedBlockCache)' -count=1
	;;
race)
	go test -race "$package_path" -run 'Test(TypedTableDecompressedBlockCache|ColumnarDecompressedBlockCache)' -count=1
	;;
vet)
	go vet "$package_path"
	;;
format)
	gofmt -w hat/hatSql/contracts.go hat/hatSql/typed_table.go hat/hatSql/columnar_decompressed_block_cache.go hat/hatSql/ch007_decompressed_block_cache_test.go hat/hatSql/ch007_decompressed_block_cache_baseline_benchmark_test.go hat/hatSql/ch007_decompressed_block_cache_benchmark_test.go
	;;
check)
	git diff --check
	;;
benchmark)
	go test "$package_path" -run '^$' -bench '^BenchmarkCH007' -benchmem -benchtime=20x -count=5
	;;
*)
	printf 'unknown CH-007 test mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
