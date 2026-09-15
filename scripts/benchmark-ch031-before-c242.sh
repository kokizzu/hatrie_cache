#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d /tmp/hatrie-cache-ch031-before.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT
git archive HEAD | tar -x -C "$tmp_dir"
git show HEAD:hat/hatSql/asof_join.go > "$tmp_dir/hat/hatSql/asof_join.go"
cp hat/hatSql/ch031_typed_json_subcolumn_baseline_benchmark_test.go "$tmp_dir/hat/hatSql/ch031_typed_json_subcolumn_baseline_benchmark_test.go"
(cd "$tmp_dir" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH031JSONValueBaseline$' -benchmem -benchtime=200ms -count=5)
