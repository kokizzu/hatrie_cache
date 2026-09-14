#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m031_skew_aware_join_exchange.go \
  hat/hatSql/m031_skew_aware_join_exchange_test.go \
  hat/hatSql/m031_skew_aware_join_exchange_benchmark_test.go
