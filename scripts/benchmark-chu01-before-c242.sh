#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-chu01-before.XXXXXX")"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

git -C "$repo_dir" archive --format=tar HEAD | tar -x -C "$tmp_dir"
cp "$repo_dir/hat/hatCache/chu01_durable_async_insert_benchmark_test.go" "$tmp_dir/hat/hatCache/chu01_durable_async_insert_benchmark_test.go"

cd "$tmp_dir/hat/hatCache"
go test -run '^$' -bench '^BenchmarkCHU01AsyncInsertUnkeyed$' -benchmem -benchtime=200ms -count=5 .
