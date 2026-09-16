#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/projection_advisor.go \
  hat/hatSql/ch_u10_projection_feedback_test.go \
  hat/hatSql/ch_u10_projection_feedback_integration_test.go \
  hat/hatSql/ch_u10_projection_feedback_benchmark_test.go
