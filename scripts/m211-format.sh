#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/m211_as_of_bounds.go hat/hatSql/m211_as_of_bounds_test.go hat/hatSql/m211_as_of_bounds_benchmark_test.go hat/hatSql/query.go hat/hatSql/sql_snapshot_token.go
