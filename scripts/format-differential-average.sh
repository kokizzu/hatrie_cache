#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/differential_average.go \
  hat/hatSql/differential_average_test.go \
  hat/hatSql/differential_average_benchmark_test.go
