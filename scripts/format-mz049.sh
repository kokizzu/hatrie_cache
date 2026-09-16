#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/mz049_schema_drift_quarantine.go \
  hat/hatSql/mz049_schema_drift_quarantine_test.go \
  hat/hatSql/mz049_schema_drift_baseline_benchmark_test.go \
  hat/hatSql/mz049_schema_drift_quarantine_benchmark_test.go
