#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/m040_incremental_percentile.go hat/hatSql/m040_incremental_percentile_test.go hat/hatSql/m040_incremental_percentile_benchmark_test.go
