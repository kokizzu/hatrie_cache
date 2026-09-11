#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/arg_extreme.go hat/hatSql/arg_extreme_test.go hat/hatSql/query.go
