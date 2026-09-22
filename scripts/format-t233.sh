#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatCache/sql_transaction.go hat/hatCache/t233_transaction_yield.go hat/hatCache/t233_transaction_yield_test.go hat/hatCache/t233_transaction_yield_benchmark_test.go
