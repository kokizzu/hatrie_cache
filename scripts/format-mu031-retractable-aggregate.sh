#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/aggregate_combinator.go \
  hat/hatSql/mu031_retractable_aggregate_test.go \
  hat/hatSql/mu031_retractable_aggregate_benchmark_test.go
