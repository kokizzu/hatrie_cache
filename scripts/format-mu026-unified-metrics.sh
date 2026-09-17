#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/mu026_unified_metrics.go hat/hatSql/mu026_unified_metrics_test.go hat/hatSql/mu026_unified_metrics_benchmark_test.go
