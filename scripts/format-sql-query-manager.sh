#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/query_manager.go hat/hatSql/query_manager_test.go hat/hatSql/query_manager_benchmark_test.go
