#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root_dir"
gofmt -w \
  hat/hatSql/columnar_array_join.go \
  hat/hatSql/ch037_columnar_array_join_test.go
