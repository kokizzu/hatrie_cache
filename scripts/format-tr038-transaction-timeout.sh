#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/sql_transaction.go \
  hat/hatCache/sql_transaction_options.go \
  hat/hatCache/tr038_transaction_timeout_test.go \
  hat/hatCache/tr038_transaction_timeout_benchmark_test.go
