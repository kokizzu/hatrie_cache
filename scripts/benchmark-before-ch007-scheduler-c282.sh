#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-ch007-scheduler-before-XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT
git archive HEAD | tar -x -C "$tmp_dir"
mkdir -p "$tmp_dir/hat/hatSql"
cp hat/hatSql/ch007_ttl_scheduler_baseline_benchmark_test.go "$tmp_dir/hat/hatSql/"
cd "$tmp_dir"
GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH007TTLDirectPurgeBaseline$' -benchmem -benchtime=200ms -count=5
