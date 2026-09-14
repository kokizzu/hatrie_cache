#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/sql_snapshot_token.go \
  hat/hatSql/mz044_snapshot_token_test.go \
  hat/hatSql/mz044_snapshot_token_benchmark_test.go \
  hat/hatSql/query.go \
  hat/hatSql/keyset.go \
  hat/hatCache/sql_query.go
