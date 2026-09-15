#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-ch031-race.XXXXXX")"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

git -C "$repo_dir" archive --format=tar HEAD | tar -x -C "$tmp_dir"
files=(
  "hat/hatSql/ch031_typed_json_subcolumn_test.go"
  "hat/hatSql/columnar_json_subcolumn.go"
  "hat/hatSql/columnar_json_subcolumn_scan.go"
  "hat/hatSql/contracts.go"
  "hat/hatSql/json_path.go"
  "hat/hatSql/query.go"
)
for file in "${files[@]}"; do
  cp "$repo_dir/$file" "$tmp_dir/$file"
done

cd "$tmp_dir"
go test -race ./hat/hatSql -run '^TestCH031'
