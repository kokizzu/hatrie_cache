#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatCache/sql_transaction.go hat/hatCache/sql_transaction_options.go hat/hatCache/t234_early_conflict_test.go hat/hatCache/t234_early_conflict_benchmark_test.go
