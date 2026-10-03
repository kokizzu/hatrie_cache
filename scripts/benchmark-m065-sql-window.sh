#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m065-window.XXXXXX")"
cleanup() {
	rm -rf "$tmp_dir"
}
trap cleanup EXIT
result_file="$tmp_dir/benchmark.txt"
GOCACHE="$tmp_dir/gocache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkM065SQLWindowFrame$' -benchmem -count=5 -benchtime=100ms >"$result_file"
rg -n '^(BenchmarkM065SQLWindowFrame|PASS|ok[[:space:]])' "$result_file"
