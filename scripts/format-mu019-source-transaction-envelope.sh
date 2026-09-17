#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/mu019_source_transaction_envelope.go \
  hat/hatSql/mu019_source_transaction_envelope_test.go \
  hat/hatSql/mu019_source_transaction_envelope_baseline_benchmark_test.go \
  hat/hatSql/sql_source_ingestion.go
