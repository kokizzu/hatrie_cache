#!/usr/bin/env sh
set -eu

rows=${SQL_BENCH_ROWS:-1000}
iterations=${SQL_BENCH_ITERATIONS:-5}
artifact_dir=${BENCHMARK_ARTIFACT_DIR:-}
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-sql-bench.XXXXXX")
report="$tmp_dir/sql-benchmark.json"

cleanup() { rm -rf "$tmp_dir"; }
trap cleanup EXIT HUP INT TERM

go run ./cmd/hatrie-sqlbench -rows "$rows" -iterations "$iterations" -out "$report"
cat "$report"

if [ -n "$artifact_dir" ]; then
	mkdir -p "$artifact_dir"
	cp "$report" "$artifact_dir/sql-benchmark.json"
	printf 'SQL benchmark artifact written to %s/sql-benchmark.json\n' "$artifact_dir"
fi
