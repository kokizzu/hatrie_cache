#!/usr/bin/env bash
set -euo pipefail

exec gofmt -w \
	hat/hatSql/mu022_connector_transaction_retry_journal.go \
	hat/hatSql/mu022_retry_journal_test.go \
	hat/hatSql/mu022_retry_journal_baseline_benchmark_test.go \
	hat/hatSql/mu022_retry_journal_benchmark_test.go
