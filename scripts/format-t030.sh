#!/bin/sh
set -eu

gofmt -w hat/hatCache/sql_transaction.go hat/hatCache/sql_transaction_options.go hat/hatCache/sql_transaction_isolation_test.go api.go
