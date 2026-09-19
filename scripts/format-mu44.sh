#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/m_u44_transaction_visibility.go \
	hat/hatSql/m_u44_transaction_visibility_test.go \
	hat/hatSql/m_u44_transaction_visibility_benchmark_test.go \
	hat/hatSql/m_u44_transaction_visibility_baseline_benchmark_test.go
