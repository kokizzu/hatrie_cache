#!/usr/bin/env bash
set -euo pipefail

repo_root=$(pwd)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

git archive HEAD | tar -x -C "$tmp_dir"
cp "$repo_root/hat/hatSql/row_binary_stream_benchmark_data_test.go" "$tmp_dir/hat/hatSql/row_binary_stream_benchmark_data_test.go"
cp "$repo_root/hat/hatSql/row_binary_stream_ndjson_benchmark_test.go" "$tmp_dir/hat/hatSql/row_binary_stream_ndjson_benchmark_test.go"
cd "$tmp_dir"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH050NDJSONBaseline$' -benchmem -count=5
