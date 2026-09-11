#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/query.go hat/hatSql/keyset.go hat/hatSql/sql_as_of.go hat/hatSql/mz008_asof_test.go hat/hatSql/mz008_asof_benchmark_test.go
