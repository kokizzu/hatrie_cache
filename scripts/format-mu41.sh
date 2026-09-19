#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/m_u41_webhook_idempotency.go \
	hat/hatSql/m_u41_webhook_idempotency_test.go \
	hat/hatSql/m_u41_webhook_idempotency_benchmark_test.go \
	hat/hatSql/m_u41_webhook_idempotency_baseline_benchmark_test.go
