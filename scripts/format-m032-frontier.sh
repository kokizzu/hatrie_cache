#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/sql_source_frontier.go \
  hat/hatSql/sql_source_frontier_test.go \
  hat/hatSql/sql_common_frontier_benchmark_test.go
