#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
files=(
  "hat/hatCache/ch009_async_insert_buffer.go"
  "hat/hatCache/ch009_async_insert_buffer_test.go"
  "hat/hatCache/chu01_durable_async_insert_benchmark_test.go"
  "hat/hatCache/chu01_durable_async_insert_test.go"
)
gofmt -w "${files[@]/#/$repo_dir/}"
gofmt -d "${files[@]/#/$repo_dir/}"
