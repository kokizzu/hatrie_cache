#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/aggregate_if.go hat/hatSql/aggregate_if_test.go hat/hatSql/aggregate_if_benchmark_test.go hat/hatSql/query.go
