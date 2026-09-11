#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/differential_min_max_benchmark_test.go \
  hat/hatSql/differential_min_max_test.go
