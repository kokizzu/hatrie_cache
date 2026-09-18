#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/function.go \
  hat/hatSql/registry.go \
  hat/hatCache/sql_function.go \
  hat/hatSql/mu032_function_capabilities_test.go \
  hat/hatSql/mu032_function_capabilities_benchmark_test.go
