#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/query.go \
  hat/hatSql/chu04_external_distinct_spill_test.go \
  hat/hatSql/chu04_external_distinct_spill_benchmark_test.go
