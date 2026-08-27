#!/usr/bin/env sh
set -eu

gofmt -w hat/hatCache/sql.go hat/hatCache/sql_transaction.go hat/hatCache/sql_test.go hat/hatCache/sql_function_test.go
