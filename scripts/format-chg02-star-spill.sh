#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/query.go \
  hat/hatSql/chg02_unified_external_sort_test.go \
  hat/hatSql/chg02_unified_external_sort_benchmark_test.go
