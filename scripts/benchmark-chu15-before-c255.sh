#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chu15-before-XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

git archive HEAD | tar -x -C "$tmp_dir"
cp hat/hatSql/ch_u15_decimal_baseline_benchmark_test.go "$tmp_dir/hat/hatSql/"
(
	cd "$tmp_dir"
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLDecimalKernelBaseline$' -benchmem -benchtime=200ms -count=5
)
