#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/typed_table_stats.go hat/hatSql/c209_typed_table_stats_test.go hat/hatSql/c209_typed_table_stats_benchmark_test.go
