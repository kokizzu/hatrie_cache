#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chu09-benchmark.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

git archive --format=tar HEAD | tar -xf - -C "$tmp_dir"
cp hat/hatSql/result_cache_persistence.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/result_cache_persistence_benchmark_test.go "$tmp_dir/hat/hatSql/"
cd "$tmp_dir"
go test ./hat/hatSql -run '^$' -bench 'BenchmarkSQLResultCache(MemoryHit1KRows|Persist1KRows|Restore1KRows|JSONEncoding1KRows)$' -benchmem -count=5 -benchtime=200ms
