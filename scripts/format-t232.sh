#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatCache/t232_transaction_scope_test.go hat/hatCache/sql_transaction.go hat/hatCache/sql_transaction_scope.go
