#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/columnar_arithmetic_projection.go hat/hatSql/chu62_arithmetic_projection_test.go hat/hatSql/chu62_arithmetic_projection_benchmark_test.go
