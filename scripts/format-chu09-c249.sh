#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/result_cache_persistence.go hat/hatSql/result_cache_persistence_test.go hat/hatSql/result_cache_persistence_benchmark_test.go hat/hatCache/sql_result_cache_auto.go hat/hatCache/sql_result_cache_persistence_test.go
