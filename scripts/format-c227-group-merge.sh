#!/bin/sh
set -eu
gofmt -w hat/hatSql/query.go hat/hatSql/sql_result_cache.go hat/hatSql/c227_group_merge_budget_test.go hat/hatSql/c227_group_merge_budget_benchmark_test.go
