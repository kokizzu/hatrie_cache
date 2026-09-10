#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/query.go hat/hatSql/limit_with_ties_test.go hat/hatSql/limit_with_ties_benchmark_test.go
