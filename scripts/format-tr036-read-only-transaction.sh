#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatCache/sql_transaction.go hat/hatCache/sql_transaction_options.go hat/hatCache/tr036_read_only_transaction_test.go
