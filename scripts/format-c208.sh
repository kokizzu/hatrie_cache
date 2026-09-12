#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/result_cache.go \
  hat/hatSql/query.go \
  hat/hatSql/c208_query_cache_metrics_test.go
