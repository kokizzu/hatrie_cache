#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d /tmp/hatrie-cache-chu10-benchmark.XXXXXX)
trap 'rm -rf "$tmp_dir"' EXIT

git archive HEAD | tar -x -C "$tmp_dir"
cp hat/hatSql/projection_advisor.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/projection_advisor_benchmark_test.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/ch_u10_projection_feedback_test.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/ch_u10_projection_feedback_benchmark_test.go "$tmp_dir/hat/hatSql/"
(cd "$tmp_dir" && go test -bench='BenchmarkSQLProjectionAdvisor' -benchmem -benchtime=200ms -count=5 ./hat/hatSql)
