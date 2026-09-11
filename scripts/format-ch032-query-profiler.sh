#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/query_profiler.go hat/hatSql/query_profiler_test.go hat/hatSql/query_profiler_benchmark_test.go
