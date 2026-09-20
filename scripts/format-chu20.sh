#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/columnar_json_subcolumn.go \
  hat/hatSql/columnar_json_subcolumn_scan.go \
  hat/hatSql/json_path.go \
  hat/hatSql/ch031_automatic_json_subcolumn.go \
  hat/hatSql/chu20_array_json_late_materialization_test.go \
  hat/hatSql/chu20_array_json_late_materialization_benchmark_test.go \
  hat/hatSql/chu20_compile_shim.go
