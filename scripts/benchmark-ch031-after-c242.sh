#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d /tmp/hatrie-cache-ch031-after.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT
git archive HEAD | tar -x -C "$tmp_dir"
git show HEAD:hat/hatSql/asof_join.go > "$tmp_dir/hat/hatSql/asof_join.go"
for file in \
  ch031_typed_json_subcolumn_test.go \
  ch031_typed_json_subcolumn_baseline_benchmark_test.go \
  ch031_typed_json_subcolumn_benchmark_test.go \
  columnar_json_subcolumn.go \
  columnar_json_subcolumn_scan.go \
  contracts.go \
  json_path.go \
  query.go; do
  cp "hat/hatSql/$file" "$tmp_dir/hat/hatSql/$file"
done
(cd "$tmp_dir" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH031(JSONValueBaseline|JSONValueTypedSubcolumn|JSONSubcolumnMaterialize)$' -benchmem -benchtime=200ms -count=5)
