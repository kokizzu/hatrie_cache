#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/model.go hat/hatSql/materialized.go hat/hatSql/query.go hat/hatSql/c237_explain_projection_test.go hat/hatSql/c237_explain_projection_benchmark_test.go
