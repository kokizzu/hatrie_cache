#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/aggregate_state_if.go \
  hat/hatSql/chu44_aggregate_combinator_test.go \
  hat/hatSql/chu44_aggregate_combinator_benchmark_test.go
