#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/sql_telemetry.go hat/hatSql/m245_timestamp_metrics_test.go hat/hatSql/m245_timestamp_metrics_benchmark_test.go
