#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m052o_native_composite_group_test.go \
  hat/hatSql/m052o_native_composite_group_benchmark_test.go
