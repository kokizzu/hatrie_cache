#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chu21-after-XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT
git archive HEAD | tar -x -C "$tmp_dir"
cp hat/hatSql/external.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/ch_u21_streaming_import_baseline_benchmark_test.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/ch_u21_streaming_import_benchmark_test.go "$tmp_dir/hat/hatSql/"
(
	cd "$tmp_dir"
	GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^Benchmark(ExternalTablesImport(CSVWholeBuffer|NDJSONWholeBuffer|CSVReader|JSONEachRowReader)|Stream(CSV|JSONEachRow))$' -benchmem -benchtime=200ms -count=5
)
