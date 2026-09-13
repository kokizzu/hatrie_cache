#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/sql.go \
  hat/hatCache/tr033_returning_old_row_test.go \
  hat/hatCache/tr033_returning_old_row_benchmark_test.go
