#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/mz021_sink_idempotency_token.go \
  hat/hatSql/mz021_sink_idempotency_token_test.go \
  hat/hatSql/mz021_sink_idempotency_token_benchmark_test.go
