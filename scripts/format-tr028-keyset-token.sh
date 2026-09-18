#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/sql_keyset_token.go \
  hat/hatSql/tr028_keyset_token_test.go \
  hat/hatSql/tr028_keyset_token_integration_test.go \
  hat/hatSql/tr028_keyset_token_benchmark_test.go
