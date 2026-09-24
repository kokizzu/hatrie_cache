#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/mu031_retractable_aggregate_transaction.go \
  hat/hatSql/mu031_retractable_aggregate_transaction_test.go \
  hat/hatSql/mu031_retractable_aggregate_baseline_benchmark_test.go \
  hat/hatSql/mu031_retractable_aggregate_transaction_benchmark_test.go
