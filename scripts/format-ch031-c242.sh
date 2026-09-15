#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
files=(
  "hat/hatSql/ch031_typed_json_subcolumn_benchmark_test.go"
  "hat/hatSql/ch031_typed_json_subcolumn_baseline_benchmark_test.go"
  "hat/hatSql/ch031_typed_json_subcolumn_test.go"
  "hat/hatSql/columnar_json_subcolumn.go"
  "hat/hatSql/columnar_json_subcolumn_scan.go"
  "hat/hatSql/contracts.go"
  "hat/hatSql/json_path.go"
  "hat/hatSql/query.go"
)

gofmt -w "${files[@]/#/$repo_dir/}"
gofmt -d "${files[@]/#/$repo_dir/}"
