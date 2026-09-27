#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/columnar_numeric_predicate.go hat/hatSql/query.go hat/hatSql/ch048_not_between_benchmark_test.go
