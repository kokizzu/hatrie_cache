#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/auto_distinct.go hat/hatSql/auto_distinct_test.go hat/hatSql/approx_aggregate.go hat/hatSql/query.go
