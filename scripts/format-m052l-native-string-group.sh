#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m052e_native_group_test.go \
  hat/hatSql/m052l_native_string_group_test.go \
  hat/hatSql/m052l_native_string_group_benchmark_test.go
