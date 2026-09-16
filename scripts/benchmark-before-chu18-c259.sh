#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chu18-before-XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT
git archive HEAD | tar -x -C "$tmp_dir"
cp hat/hatSql/ch_u18_sparse_primary_baseline_benchmark_test.go "$tmp_dir/hat/hatSql/"
(
	cd "$tmp_dir"
	GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLColumnarSparsePrimarySingleFieldRange$' -benchmem -benchtime=1s -count=7
)
