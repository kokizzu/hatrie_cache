#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/upsert_envelope.go hat/hatSql/m206_upsert_envelope_test.go \
  hat/hatSql/m206_upsert_envelope_baseline_benchmark_test.go \
  hat/hatSql/m206_upsert_envelope_benchmark_test.go
