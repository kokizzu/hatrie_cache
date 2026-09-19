#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/query_profiler.go \
  hat/hatSql/chu26_query_memory_profile_test.go \
  hat/hatSql/chu26_query_memory_profile_benchmark_test.go
