#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/differential_difference.go \
  hat/hatSql/differential_difference_test.go \
  hat/hatSql/differential_difference_benchmark_test.go
