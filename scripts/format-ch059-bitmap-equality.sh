#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatCache/sql_query.go \
  hat/hatCache/ch059_bitmap_equality_test.go \
  hat/hatCache/ch059_bitmap_equality_benchmark_test.go \
