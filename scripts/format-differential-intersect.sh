#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/differential_intersect.go \
  hat/hatSql/differential_intersect_test.go \
  hat/hatSql/differential_intersect_benchmark_test.go
