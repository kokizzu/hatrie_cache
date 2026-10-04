#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/approx_aggregate.go hat/hatSql/approx_percentile_info_test.go hat/hatSql/query.go
