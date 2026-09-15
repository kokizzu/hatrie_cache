#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-chu01-after.XXXXXX")"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

git -C "$repo_dir" archive --format=tar HEAD | tar -x -C "$tmp_dir"
files=(
  "hat/hatCache/ch009_async_insert_buffer.go"
  "hat/hatCache/chu01_durable_async_insert_benchmark_test.go"
)
for file in "${files[@]}"; do
  cp "$repo_dir/$file" "$tmp_dir/$file"
done

cd "$tmp_dir/hat/hatCache"
go test -run '^$' -bench '^BenchmarkCHU01AsyncInsert(Unkeyed|KeyedDuplicate)$' -benchmem -benchtime=200ms -count=5 .
