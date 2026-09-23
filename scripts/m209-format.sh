#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/logical_frontier.go \
  hat/hatSql/m209_logical_frontier_benchmark_test.go \
  hat/hatSql/m209_logical_frontier_test.go \
  hat/hatSql/query.go \
  hat/hatSql/sql_snapshot_token.go \
  hat/hatSql/subscription.go
