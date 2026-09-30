#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/differential_row_number_lag.go hat/hatSql/m_u65_differential_row_number_lag_test.go hat/hatSql/m_u65_differential_row_number_lag_benchmark_test.go
