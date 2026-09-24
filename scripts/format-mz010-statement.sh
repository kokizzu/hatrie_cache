#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/mz010_sql_subscription_statement.go hat/hatSql/mz010_sql_subscription_statement_test.go
