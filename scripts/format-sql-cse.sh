#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/rewrite.go hat/hatSql/common_subexpression_test.go hat/hatSql/common_subexpression_benchmark_test.go
