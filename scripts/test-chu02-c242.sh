#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-chu02-test.XXXXXX")"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

git -C "$repo_dir" archive --format=tar HEAD | tar -x -C "$tmp_dir"
files=(
  "hat/hatSql/contracts.go"
  "hat/hatSql/external.go"
  "hat/hatSql/query.go"
  "hat/hatSql/chu02_external_order_spill_test.go"
)
for file in "${files[@]}"; do
  cp "$repo_dir/$file" "$tmp_dir/$file"
done

cd "$tmp_dir"
go test ./hat/hatSql -run '^TestCHU02'
