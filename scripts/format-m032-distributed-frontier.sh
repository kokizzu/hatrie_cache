#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/sql_distributed_frontier.go \
  hat/hatSql/sql_distributed_frontier_test.go \
  hat/hatSql/sql_distributed_frontier_benchmark_test.go
