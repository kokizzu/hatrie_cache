#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root_dir"
gofmt -w \
  hat/hatSql/ch004_final_schema_store.go \
  hat/hatSql/ch004_final_schema_store_test.go \
  hat/hatSql/ch004_final_schema_store_benchmark_test.go
