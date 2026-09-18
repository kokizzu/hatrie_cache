#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/ch012_projection_advisor_cost_test.go \
  hat/hatSql/ch012_projection_advisor_cost_benchmark_test.go \
  hat/hatSql/projection_advisor.go
