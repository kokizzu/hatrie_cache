#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/ch012_projection_advisor_persistence.go \
  hat/hatSql/ch012_projection_advisor_persistence_test.go \
  hat/hatSql/ch012_projection_advisor_persistence_benchmark_test.go
