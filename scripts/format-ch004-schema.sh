#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/ch004_schema.go hat/hatSql/ch004_final.go hat/hatSql/query.go hat/hatSql/ch004_schema_test.go hat/hatSql/ch004_schema_benchmark_test.go
