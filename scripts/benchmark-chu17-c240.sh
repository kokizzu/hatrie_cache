#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d /tmp/hatrie-cache-chu17-bench.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT

git archive HEAD | tar -x -C "$tmp_dir"
git show HEAD:hat/hatSql/asof_join.go > "$tmp_dir/hat/hatSql/asof_join.go"
cp hat/hatSql/in_program.go "$tmp_dir/hat/hatSql/in_program.go"
cp hat/hatSql/chu17_in_program_benchmark_test.go "$tmp_dir/hat/hatSql/chu17_in_program_benchmark_test.go"
(cd "$tmp_dir" && go test ./hat/hatSql -run '^$' -bench 'BenchmarkCHU17' -benchtime=200ms -count=5)
