#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/query_log.go hat/hatSql/query_log_test.go hat/hatSql/query_log_benchmark_test.go hat/hatSql/query_manager.go hat/hatSql/query_manager_execute_benchmark_test.go hat/hatCache/sql_query.go sql_query_manager_api.go
