#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/sql.go hat/hatCache/sql_idempotency_test.go hat/hatCache/sql_idempotency_benchmark_test.go
